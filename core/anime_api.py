from __future__ import annotations

import os
import re
import time
import logging
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import quote, urljoin

import requests
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# Hiyori / Miruro Native API.
# This replaces the old AnimePahe/Cloudflare path while keeping the
# function names and return shapes expected by handlers.py/scheduler.py.
HIYORI_BASE_URL = os.environ.get(
    "HIYORI_BASE_URL",
    "https://api.hiyori.tv"
).rstrip("/")

HLS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.0.0 Safari/537.36"
)
HLS_REFERER = os.environ.get("HIYORI_HLS_REFERER", "https://megaplay.buzz/")

_session = requests.Session()
_session.headers.update({
    "User-Agent": HLS_USER_AGENT,
    "Accept": "application/json",
})


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    reraise=True,
)
def _hiyori_get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET JSON from Hiyori with a small retry policy for transient 5xx errors."""
    url = path if path.startswith("http") else f"{HIYORI_BASE_URL}/{path.lstrip('/')}"
    response = _session.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected Hiyori response type: {type(data).__name__}")
    return data


def _title_text(item: Dict[str, Any]) -> str:
    title = item.get("title") or item.get("name") or "Unknown Anime"
    if isinstance(title, dict):
        return (
            title.get("english")
            or title.get("romaji")
            or title.get("native")
            or "Unknown Anime"
        )
    return str(title)


def _year_value(item: Dict[str, Any]) -> int:
    value = item.get("year")
    if value is None:
        value = item.get("seasonYear")
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _episode_value(item: Dict[str, Any]) -> int:
    """Extract a released/current episode number from several Hiyori shapes."""
    for key in ("episode", "latestEpisode", "latest_episode", "currentEpisode", "current_episode"):
        value = item.get(key)
        if isinstance(value, dict):
            value = value.get("episode") or value.get("number")
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass

    # /schedule exposes next_episode + timeUntilAiring. If it is in the
    # future, the most recently released episode is normally next - 1.
    try:
        nxt = int(item.get("next_episode") or item.get("nextEpisode") or 0)
        remaining = float(item.get("timeUntilAiring") or item.get("time_until_airing") or 0)
        if nxt > 0:
            return max(nxt - 1, 0) if remaining > 0 else nxt
    except (TypeError, ValueError):
        pass

    # Last fallback: some collection responses expose an episode count.
    try:
        return int(item.get("episodes") or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_anime(item: Dict[str, Any]) -> Dict[str, Any]:
    """Return the old AnimePahe-compatible anime shape used by the bot."""
    anilist_id = item.get("id") or item.get("anilistId") or item.get("anilist_id")
    title = _title_text(item)
    year = _year_value(item)
    episodes = item.get("episodes")

    if isinstance(episodes, dict):
        episodes = episodes.get("released") or episodes.get("total")

    try:
        episodes = int(episodes or 0)
    except (TypeError, ValueError):
        episodes = 0

    return {
        "id": anilist_id,
        "session": str(anilist_id) if anilist_id is not None else "",
        "title": title,
        "year": year,
        "episodes": episodes,
        "poster": item.get("poster") or item.get("coverImage"),
        "format": item.get("format"),
        "status": item.get("status"),
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    reraise=True,
)
async def search_anime(query: str) -> Optional[List[Dict[str, Any]]]:
    """Search Hiyori and return the old bot-compatible result shape."""
    data = await asyncio.to_thread(
        _hiyori_get_json,
        "/suggestions",
        {"query": query},
    )

    results = data.get("suggestions") or data.get("results") or data.get("data") or []
    if not isinstance(results, list):
        return None

    normalized = []
    for item in results:
        if isinstance(item, dict) and item.get("id") is not None:
            normalized.append(_normalize_anime(item))

    return normalized or None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=8),
    reraise=True,
)
async def get_episode_list(session_id: str, page: int = 1) -> Dict[str, Any]:
    """
    Hiyori episode list -> old AnimePahe-compatible shape.

    session_id is the AniList ID returned by search_anime().
    One row is created per episode number. get_stream_links() later
    resolves the actual SUB/DUB provider IDs for that episode.
    """
    anilist_id = str(session_id).strip()
    data = await asyncio.to_thread(_hiyori_get_json, f"/episodes/{anilist_id}")

    providers = data.get("providers") or {}
    by_number: Dict[int, Dict[str, Any]] = {}

    if isinstance(providers, dict):
        for provider_name, provider_data in providers.items():
            if not isinstance(provider_data, dict):
                continue

            groups = provider_data.get("episodes") or {}
            if not isinstance(groups, dict):
                continue

            for language in ("sub", "dub"):
                episode_list = groups.get(language) or []
                if not isinstance(episode_list, list):
                    continue

                for ep in episode_list:
                    if not isinstance(ep, dict):
                        continue
                    try:
                        number = int(ep.get("number"))
                    except (TypeError, ValueError):
                        continue

                    if number not in by_number:
                        by_number[number] = {
                            "episode": number,
                            # Synthetic ID; no provider is hard-coded here.
                            "session": f"hiyori:{anilist_id}:{number}",
                            "title": ep.get("title") or f"Episode {number}",
                            "image": ep.get("image"),
                            "airDate": ep.get("airDate"),
                        }

    episodes = [by_number[n] for n in sorted(by_number)]
    return {
        "data": episodes,
        "last_page": 1,
        "total": len(episodes),
    }


async def get_all_episodes(anime_session: str):
    data = await get_episode_list(anime_session, 1)
    return data.get("data", []) if data else []


def find_closest_episode(episodes, target_episode):
    try:
        target = int(target_episode)
    except (ValueError, TypeError):
        return None

    valid = []
    for ep in episodes or []:
        try:
            valid.append((int(ep.get("episode")), ep))
        except (TypeError, ValueError):
            continue

    if not valid:
        return None

    valid.sort(key=lambda x: x[0])

    exact = next((ep for n, ep in valid if n == target), None)
    if exact:
        return exact

    # Preserve the old behavior: use the latest episode <= target.
    previous = [ep for n, ep in valid if n <= target]
    return previous[-1] if previous else valid[0][1]


def _episode_from_session(anime_session: str, episode_session: str) -> tuple[str, int]:
    """Read the synthetic hiyori:<anilist_id>:<episode> session."""
    if isinstance(episode_session, str) and episode_session.startswith("hiyori:"):
        parts = episode_session.split(":", 2)
        if len(parts) == 3:
            return parts[1], int(parts[2])

    return str(anime_session), int(episode_session)


def _find_provider_episode_ids(
    providers: Dict[str, Any],
    episode_number: int,
) -> Dict[str, List[str]]:
    """
    Return provider episode IDs grouped by sub/dub.
    Each ID is directly usable as Hiyori's /watch/... path.
    """
    found = {"sub": [], "dub": []}

    for provider_name, provider_data in (providers or {}).items():
        if not isinstance(provider_data, dict):
            continue

        groups = provider_data.get("episodes") or {}
        if not isinstance(groups, dict):
            continue

        for language in ("sub", "dub"):
            for ep in groups.get(language) or []:
                if not isinstance(ep, dict):
                    continue
                try:
                    number = int(ep.get("number"))
                except (TypeError, ValueError):
                    continue

                if number != episode_number:
                    continue

                ep_id = ep.get("id")
                if isinstance(ep_id, str) and ep_id and ep_id not in found[language]:
                    found[language].append(ep_id)

    return found


def _quality_from_resolution(width: int, height: int) -> str:
    if height >= 2160:
        return "2160p"
    if height >= 1080:
        return "1080p"
    if height >= 720:
        return "720p"
    if height >= 480:
        return "480p"
    if height >= 360:
        return "360p"
    return f"{height}p"


def _parse_hls_variants(master_url: str) -> List[Dict[str, Any]]:
    """
    Expand an HLS master playlist into real quality-specific media
    playlists. If the playlist has only one variant, that one is returned.
    """
    try:
        response = _session.get(
            master_url,
            headers={
                "User-Agent": HLS_USER_AGENT,
                "Referer": HLS_REFERER,
                "Accept": "*/*",
            },
            timeout=30,
        )
        response.raise_for_status()
        text = response.text

        if "#EXTM3U" not in text:
            return []

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        variants: List[Dict[str, Any]] = []

        for i, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF:"):
                continue

            resolution = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
            bandwidth = re.search(r"BANDWIDTH=(\d+)", line)

            # URI is the first non-tag line after EXT-X-STREAM-INF.
            variant_uri = None
            for following in lines[i + 1:]:
                if not following.startswith("#"):
                    variant_uri = urljoin(master_url, following)
                    break

            if not variant_uri:
                continue

            if resolution:
                width = int(resolution.group(1))
                height = int(resolution.group(2))
            else:
                width, height = 0, 0

            variants.append({
                "url": variant_uri,
                "master_url": master_url,
                "resolution": height or 0,
                "width": width,
                "height": height,
                "quality": _quality_from_resolution(width, height) if height else None,
                "bandwidth": int(bandwidth.group(1)) if bandwidth else 0,
            })

        # Some providers return a media playlist directly instead of a
        # master. Treat it as a single stream rather than failing.
        if not variants and "#EXTINF:" in text:
            return [{
                "url": master_url,
                "master_url": master_url,
                "resolution": 0,
                "width": 0,
                "height": 0,
                "quality": None,
                "bandwidth": 0,
            }]

        return variants

    except Exception as e:
        logger.warning("Failed to parse HLS master %s: %s", master_url, e)
        return []


def _stream_quality_number(stream: Dict[str, Any]) -> int:
    value = stream.get("quality") or stream.get("resolution") or 0
    if isinstance(value, str):
        match = re.search(r"(\d{3,4})", value)
        if match:
            return int(match.group(1))
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def get_stream_links(anime_session: str, episode_session: str) -> Optional[List[Dict[str, Any]]]:
    """
    Resolve a synthetic episode session through Hiyori.

    Output fields intentionally match the old AnimePahe stream objects:
      url, fansub, resolution, audio, av1, text
    """
    try:
        anilist_id, episode_number = _episode_from_session(
            anime_session, episode_session
        )

        episodes_data = _hiyori_get_json(f"/episodes/{anilist_id}")
        provider_ids = _find_provider_episode_ids(
            episodes_data.get("providers") or {},
            episode_number,
        )

        stream_links: List[Dict[str, Any]] = []
        seen = set()

        # Prefer SUB and DUB separately. We keep the first successful
        # provider(s), but allow multiple providers when useful.
        for language, ids in (("sub", provider_ids["sub"]), ("dub", provider_ids["dub"])):
            for episode_id in ids:
                try:
                    stream_data = _hiyori_get_json(f"/{episode_id}")
                except Exception as e:
                    logger.warning(
                        "Hiyori stream failed for %s/%s: %s",
                        language, episode_id, e
                    )
                    continue

                streams = stream_data.get("streams") or []
                if isinstance(streams, dict):
                    streams = streams.get("data") or streams.get("streams") or []
                if not isinstance(streams, list):
                    continue

                for source in streams:
                    if not isinstance(source, dict):
                        continue

                    master_url = source.get("url")
                    if not isinstance(master_url, str) or ".m3u8" not in master_url:
                        continue

                    variants = _parse_hls_variants(master_url)
                    if not variants:
                        variants = [{
                            "url": master_url,
                            "resolution": _stream_quality_number(source),
                            "quality": source.get("quality"),
                            "width": 0,
                            "height": _stream_quality_number(source),
                            "bandwidth": 0,
                        }]

                    for variant in variants:
                        resolution = int(variant.get("resolution") or 0)
                        quality = variant.get("quality") or (
                            f"{resolution}p" if resolution else "HLS"
                        )

                        # Hiyori's category is the reliable audio marker.
                        audio = "eng" if language == "dub" else "jpn"
                        key = (audio, variant["url"])
                        if key in seen:
                            continue
                        seen.add(key)

                        stream_links.append({
                            "url": variant["url"],
                            "master_url": master_url,
                            "fansub": episode_id.split("/")[1] if "/" in episode_id else "hiyori",
                            "resolution": resolution,
                            "audio": audio,
                            "av1": "0",
                            "text": f"{quality} ({audio.upper()})",
                            "quality": quality,
                            "headers": {
                                "User-Agent": HLS_USER_AGENT,
                                "Referer": HLS_REFERER,
                            },
                        })

                # Once a provider gave streams, don't hammer every provider.
                if any(s.get("audio") == ("eng" if language == "dub" else "jpn")
                       for s in stream_links):
                    break

        if not stream_links:
            logger.error(
                "No Hiyori streams found for AniList %s episode %s",
                anilist_id, episode_number
            )
            return None

        stream_links.sort(
            key=lambda s: (0 if s["audio"] == "jpn" else 1, s["resolution"])
        )

        logger.info(
            "Hiyori streams for AniList %s Ep%s: %s",
            anilist_id,
            episode_number,
            [(s["resolution"], s["audio"], s["fansub"]) for s in stream_links],
        )
        return stream_links

    except Exception as e:
        logger.exception("Error getting Hiyori stream links: %s", e)
        return None


def extract_m3u8_from_kwik(link: str) -> Optional[Dict[str, Any]]:
    """
    Compatibility wrapper.

    The old function resolved AnimePahe/Kwik links. Hiyori already returns
    an HLS URL, so we simply pass it through with the required headers.
    """
    if not isinstance(link, str) or ".m3u8" not in link:
        logger.error("Expected Hiyori HLS URL, got: %s", link)
        return None

    return {
        "m3u8_url": link,
        "headers": {
            "User-Agent": HLS_USER_AGENT,
            "Referer": HLS_REFERER,
        },
    }


async def download_m3u8(
    m3u8_url: str,
    headers: Dict[str, str],
    output_path: str,
    progress_callback=None,
) -> bool:
    """
    Compatibility wrapper around the bot's real HLS downloader.

    The previous implementation downloaded direct MP4 files. Hiyori
    returns HLS, so use core.downloader.download_m3u8 here.
    """
    from core.downloader import download_m3u8 as _download_hls

    return await _download_hls(
        m3u8_url=m3u8_url,
        output_path=output_path,
        headers=headers or {},
        progress_callback=progress_callback,
    )


def map_resolution_to_quality_tier(resolution: int) -> str:
    if resolution <= 360:
        return "360p"
    elif resolution <= 720:
        return "720p"
    elif resolution <= 1080:
        return "1080p"
    else:
        return "2160p"


def get_quality_streams(
    stream_links: List[Dict[str, Any]],
    enabled_qualities: List[str],
    preferred_audio: str = "jpn",
) -> Dict[str, Dict[str, Any]]:
    """
    Select one real Hiyori HLS variant for every enabled quality.

    Important: a quality is only returned when that quality actually exists.
    No fake 480p/720p entries are created.
    """
    filtered = [s for s in stream_links if s.get("audio") == preferred_audio]

    if not filtered:
        filtered = stream_links
        logger.warning(
            "No streams found for audio '%s', using all available",
            preferred_audio,
        )

    result: Dict[str, Dict[str, Any]] = {}

    for quality in enabled_qualities:
        try:
            target = int(str(quality).rstrip("p"))
        except ValueError:
            continue

        exact = [s for s in filtered if int(s.get("resolution") or 0) == target]
        if exact:
            result[quality] = exact[-1]
            continue

        candidates = [
            s for s in filtered
            if map_resolution_to_quality_tier(int(s.get("resolution") or 0)) == quality
        ]

        if candidates:
            candidates.sort(key=lambda s: int(s.get("resolution") or 0))
            result[quality] = candidates[0] if target <= 360 else candidates[-1]

    return result


def detect_audio_type(stream_links: List[Dict[str, Any]]) -> str:
    has_eng = any(s.get("audio") == "eng" for s in stream_links)
    has_jpn = any(s.get("audio") == "jpn" for s in stream_links)

    if has_eng and not has_jpn:
        return "Dub"
    return "Sub"


def get_sub_dub_streams(stream_links: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "sub": [s for s in stream_links if s.get("audio") == "jpn"],
        "dub": [s for s in stream_links if s.get("audio") == "eng"],
    }


def get_latest_releases(page=1):
    """
    Keep the old {'data': [...]} shape used by handlers/scheduler.

    Hiyori's /schedule gives the next airing episode. When it is still in
    the future, we expose next_episode - 1 as the currently released episode.
    """
    try:
        data = _hiyori_get_json(
            "/schedule",
            {"page": page, "per_page": 20},
        )
        results = data.get("results") or data.get("data") or []

        normalized = []
        for item in results:
            if not isinstance(item, dict):
                continue

            # Full anime info may be nested.
            anime = item.get("anime") if isinstance(item.get("anime"), dict) else item
            title = _title_text(anime)
            episode = _episode_value(item)

            if episode <= 0:
                episode = _episode_value(anime)

            normalized.append({
                "anime_title": title,
                "episode": episode,
                "session": str(anime.get("id") or item.get("id") or ""),
                "anilist_id": anime.get("id") or item.get("id"),
                "airingAt": item.get("airingAt"),
                "timeUntilAiring": item.get("timeUntilAiring"),
            })

        return {
            "data": normalized,
            "page": data.get("page", page),
            "last_page": 1,
            "total": len(normalized),
        }

    except Exception as e:
        logger.error("Error fetching Hiyori schedule: %s", e)
        return {"data": [], "page": page, "last_page": 1, "total": 0}


async def get_anime_info(title: str) -> Dict[str, Any]:
    """
    Keep the existing AniList-backed metadata function. This is used for
    posters/info and does not depend on AnimePahe.
    """
    query = """
    query ($search: String) {
      Media(search: $search, type: ANIME) {
        id
        idMal
        title { romaji english native }
        type
        format
        status(version: 2)
        description(asHtml: false)
        startDate { year month day }
        endDate { year month day }
        season
        seasonYear
        episodes
        duration
        chapters
        volumes
        countryOfOrigin
        source
        hashtag
        trailer { id site thumbnail }
        updatedAt
        coverImage { extraLarge large medium }
        bannerImage
        genres
        synonyms
        averageScore
        meanScore
        popularity
        trending
        favourites
        studios { nodes { name siteUrl } }
        isAdult
        nextAiringEpisode { airingAt timeUntilAiring episode }
        airingSchedule {
          edges { node { airingAt timeUntilAiring episode } }
        }
        externalLinks { url site }
        relations {
          edges {
            relationType
            node { id bannerImage }
          }
        }
        siteUrl
      }
    }
    """

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://graphql.anilist.co",
                json={"query": query, "variables": {"search": title}},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.error("AniList API returned %s", resp.status)
                    return {}
                data = await resp.json()
                return data.get("data", {}).get("Media", {}) or {}
    except Exception as e:
        logger.error("Error fetching anime info from AniList: %s", e)
        return {}


async def download_anime_poster(title: str, save_dir: str = None) -> Optional[str]:
    try:
        info = await get_anime_info(title)
        if not info:
            return None

        image_url = info.get("bannerImage")

        if not image_url:
            relations = info.get("relations", {}).get("edges", [])
            for rel in relations:
                if rel.get("relationType") in ("PREQUEL", "PARENT", "SOURCE"):
                    node_banner = rel.get("node", {}).get("bannerImage")
                    if node_banner:
                        image_url = node_banner
                        break
            if not image_url:
                for rel in relations:
                    node_banner = rel.get("node", {}).get("bannerImage")
                    if node_banner:
                        image_url = node_banner
                        break

        if not image_url:
            cover_image = info.get("coverImage", {})
            if cover_image:
                image_url = (
                    cover_image.get("extraLarge")
                    or cover_image.get("large")
                    or cover_image.get("medium")
                )

        if not image_url:
            return None

        if save_dir is None:
            save_dir = str(Path(__file__).parent.parent / "thumbnails")

        os.makedirs(save_dir, exist_ok=True)
        safe_title = re.sub(r"[^\w\s-]", "", title).strip().replace(" ", "_")[:50]
        save_path = os.path.join(save_dir, f"{safe_title}_poster.jpg")

        if os.path.exists(save_path) and os.path.getsize(save_path) > 1000:
            return save_path

        async with aiohttp.ClientSession() as session:
            async with session.get(
                image_url,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    with open(save_path, "wb") as f:
                        f.write(data)
                    if os.path.getsize(save_path) > 1000:
                        return save_path

        return None

    except Exception as e:
        logger.error("Error downloading anime poster: %s", e)
        return None
