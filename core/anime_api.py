from __future__ import annotations
import os
import re
import ast
import time
import random
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import quote, urlparse

import requests
import aiohttp
import cloudscraper
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from core.config import HEADERS, ANILIST_API, ANIMEPAHE_BASE_URL

logger = logging.getLogger(__name__)

KWIK_USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Mobile Safari/537.36"

# =====================================================================
# FIX (Render deploy bug, part 1): animepahe.pw sits behind Cloudflare.
# Plain aiohttp/requests calls (as this file used to do for the JSON
# API endpoints below) get blocked/challenged by Cloudflare when they
# come from a datacenter IP like Render's -- you get an HTML
# "checking your browser" page back instead of JSON, so response.json()
# throws and the bot shows "search error" / "failed request".
#
# The rest of this file (get_stream_links, extract_m3u8_from_kwik)
# already solved this by using `cloudscraper` instead of plain
# requests/aiohttp. This helper applies that same fix to the search,
# episode-list, and latest-releases endpoints, which were missed.
#
# cloudscraper is synchronous, so from async functions we run it in a
# worker thread via asyncio.to_thread so it doesn't block the event loop.
# =====================================================================
#
# FIX (Render deploy bug, part 2): on Render, animepahe.pw returned a
# hard 403 Forbidden -- not a JS challenge page. That means Cloudflare
# is IP-blocking Render's whole datacenter range outright. cloudscraper
# only solves JS/browser challenges, it cannot get past an IP-level
# block, because Cloudflare refuses the connection before any challenge
# is served. The only fix for that is routing requests through an IP
# Cloudflare hasn't blocked, i.e. a proxy (ideally residential/rotating,
# from a service like Webshare, Smartproxy, Bright Data, etc.).
#
# Set the environment variable ANIMEPAHE_PROXY on Render to a full
# proxy URL, e.g.:
#   ANIMEPAHE_PROXY=http://username:password@proxy-host:port
# If it's not set, everything behaves exactly as before (no proxy).
# =====================================================================
ANIMEPAHE_PROXY = os.environ.get("ANIMEPAHE_PROXY", "").strip()
PROXIES = {"http": ANIMEPAHE_PROXY, "https": ANIMEPAHE_PROXY} if ANIMEPAHE_PROXY else None

def _new_scraper():
    """Create a cloudscraper session, routed through ANIMEPAHE_PROXY if set."""
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'linux', 'mobile': False}
    )
    if PROXIES:
        scraper.proxies.update(PROXIES)
    return scraper

def _cf_get_json(url: str) -> Any:
    """Synchronous helper: GET a URL through cloudscraper (bypasses
    Cloudflare's JS challenge, and the proxy, if set, bypasses IP
    blocks) and return the parsed JSON body."""
    scraper = _new_scraper()
    scraper.headers.update(HEADERS)
    response = scraper.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def search_anime(query: str) -> Optional[List[Dict[str, Any]]]:
    # CHANGED: uses ANIMEPAHE_BASE_URL instead of a hardcoded domain, so
    # this can be pointed at a Cloudflare Worker proxy (see config.py)
    # to route around Render's IP being blocked.
    search_url = f"{ANIMEPAHE_BASE_URL}/api?m=search&q={quote(query)}"

    # CHANGED: was a raw aiohttp.ClientSession() request, which Cloudflare
    # blocked on Render. Now routed through cloudscraper (in a thread).
    data = await asyncio.to_thread(_cf_get_json, search_url)

    if data.get('total', 0) == 0:
        return None

    return data.get('data', [])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
async def get_episode_list(session_id: str, page: int = 1) -> Dict[str, Any]:
    # CHANGED: uses ANIMEPAHE_BASE_URL instead of a hardcoded domain.
    episodes_url = f"{ANIMEPAHE_BASE_URL}/api?m=release&id={session_id}&sort=episode_asc&page={page}"

    # CHANGED: was a raw aiohttp.ClientSession() request, same Cloudflare
    # issue as search_anime above. Now routed through cloudscraper.
    return await asyncio.to_thread(_cf_get_json, episodes_url)

def get_latest_releases(page=1):
    # CHANGED: uses ANIMEPAHE_BASE_URL instead of a hardcoded domain.
    releases_url = f"{ANIMEPAHE_BASE_URL}/api?m=airing&page={page}"

    # CHANGED: was `requests.get(...)`, blocked by Cloudflare on Render
    # (this is why /latest and /airing showed no results either).
    # Now uses cloudscraper directly (this function is already sync,
    # so no asyncio.to_thread wrapper is needed here).
    return _cf_get_json(releases_url)


async def get_all_episodes(anime_session):
    all_episodes = []
    page = 1
    while True:
        episode_data = await get_episode_list(anime_session, page)
        if not episode_data or 'data' not in episode_data:
            break
        episodes = episode_data['data']
        all_episodes.extend(episodes)
        if page >= episode_data.get('last_page', 1):
            break
        page += 1
    return all_episodes

def find_closest_episode(episodes, target_episode):
    try:
        target = int(target_episode)
    except (ValueError, TypeError):
        return None
    
    valid_episodes = []
    for ep in episodes:
        try:
            ep_num = int(ep['episode'])
            valid_episodes.append((ep_num, ep))
        except (ValueError, TypeError):
            continue
    
    if not valid_episodes:
        return None
    
    valid_episodes.sort(key=lambda x: x[0])
    
    closest = None
    for ep_num, ep in valid_episodes:
        if ep_num <= target:
            closest = ep
        else:
            break
    
    if closest is None and valid_episodes:
        closest = valid_episodes[0][1]
    
    return closest

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=10),
    reraise=True
)
def get_stream_links(anime_session: str, episode_session: str) -> Optional[List[Dict[str, Any]]]:
    # CHANGED: uses ANIMEPAHE_BASE_URL instead of a hardcoded domain.
    if '-' in episode_session:
        episode_url = f"{ANIMEPAHE_BASE_URL}/play/{episode_session}"
    else:
        episode_url = f"{ANIMEPAHE_BASE_URL}/play/{anime_session}/{episode_session}"
    
    try:
        # CHANGED: now goes through _new_scraper() so it also picks up
        # ANIMEPAHE_PROXY if you set one -- same 403/IP-block issue can
        # hit this endpoint too, not just search.
        session = _new_scraper()
        session.headers.update(HEADERS)
        time.sleep(random.uniform(1, 3))
        
        local_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        session.headers.update(local_headers)
        # CHANGED: warm-up request now also goes through the configurable base URL.
        session.get(f"{ANIMEPAHE_BASE_URL}/")
        
        logger.info(f"Fetching episode page: {episode_url}")
        response = session.get(episode_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        buttons = soup.select('#resolutionMenu button[data-src]')
        
        if not buttons:
            buttons = soup.select('button.dropdown-item[data-src]')
        
        if not buttons:
            buttons = soup.select('button[data-src*="kwik"]')
        
        if not buttons:
            logger.error(f"No stream buttons found for episode: {episode_url}")
            logger.debug(f"Page sample: {response.text[:2000]}")
            return None
        
        stream_links = []
        for btn in buttons:
            src = btn.get('data-src', '')
            fansub = btn.get('data-fansub', 'Unknown')
            resolution = btn.get('data-resolution', '0')
            audio = btn.get('data-audio', 'jpn')
            av1 = btn.get('data-av1', '0')
            text = btn.get_text(strip=True)
            
            if src and 'kwik' in src:
                stream_links.append({
                    'url': src,
                    'fansub': fansub,
                    'resolution': int(resolution) if resolution.isdigit() else 0,
                    'audio': audio,
                    'av1': av1,
                    'text': text
                })
        
        # Direct-download (MP4) links: pahe.win -> kwik /f/ -> mp4. No HLS/m3u8.
        direct = []
        for a in (soup.select('#pickDownload a.dropdown-item[href]')
                  or soup.select('a.dropdown-item[href*="pahe.win"]')):
            href = a.get('href', '')
            txt = a.get_text(' ', strip=True)
            m = re.search(r'(\d{3,4})p', txt)
            if not href or not m:
                continue
            direct.append({
                'href': href,
                'res': int(m.group(1)),
                'audio': 'eng' if re.search(r'\beng\b', txt, re.I) else 'jpn',
                'fansub': txt.split('\u00b7')[0].strip().lower(),
            })
        logger.info(f"Found {len(direct)} direct download links")

        for s in stream_links:
            s['embed_url'] = s['url']
            cands = [d for d in direct if d['res'] == s['resolution'] and d['audio'] == s['audio']]
            same = [d for d in cands if d['fansub'] == str(s['fansub']).strip().lower()]
            pick = (same or cands or [None])[0]
            if pick:
                s['url'] = pick['href']

        if stream_links:
            logger.info(f"Found {len(stream_links)} stream links: {[(s['resolution'], s['audio']) for s in stream_links]}")
            return stream_links
        
        logger.error(f"No valid kwik stream links found for episode: {episode_url}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting stream links: {str(e)}")
        logger.error(f"URL attempted: {episode_url}")
        raise

def _unpack_js(p, a, c, k, e=None, d=None):
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    
    def base_encode(n):
        rem = n % a
        digit = chr(rem + 29) if rem > 35 else digits[rem]
        if n < a:
            return digit
        return base_encode(n // a) + digit

    d = {} if d is None else d
    for i in range(c - 1, -1, -1):
        key = base_encode(i)
        d[key] = k[i] if i < len(k) and k[i] else key

    pattern = re.compile(r'\b\w+\b')
    def replace(m):
        w = m.group(0)
        return d.get(w, w)

    return pattern.sub(replace, p)

def _kwik_find(html: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, html)
    if m:
        return m.group(1)
    for p, a_str, c_str, k_str in re.findall(
        r"eval\(function\(p,a,c,k,e,d\)\{.*?\}\('(.*?)',(\d+),(\d+),'(.*?)'\.split\('\|'\)",
        html, re.DOTALL
    ):
        try:
            decoded = _unpack_js(p, int(a_str), int(c_str), k_str.split('|'))
            m = re.search(pattern, decoded)
            if m:
                return m.group(1)
        except Exception:
            continue
    return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=3, max=8),
    reraise=True
)
def extract_m3u8_from_kwik(link: str) -> Optional[Dict[str, Any]]:
    """Resolve a pahe.win download link to a direct MP4 URL (no m3u8).

    Name kept so existing callers in handlers.py / scheduler.py work
    unchanged. Returns {'m3u8_url': <mp4 url>, 'headers': {...}}.
    """
    if 'pahe.win' not in link and '/f/' not in link:
        logger.error(f"No direct download link (got embed link): {link}")
        return None

    session = _new_scraper()
    session.headers.update({"User-Agent": KWIK_USER_AGENT})

    kwik_f = link
    if 'pahe.win' in link:
        r = session.get(link, headers={"Referer": "https://animepahe.pw/"},
                        timeout=30, allow_redirects=True)
        r.raise_for_status()
        kwik_f = _kwik_find(r.text, r"(https?://kwik\.[a-z]+/f/[A-Za-z0-9]+)")
        if not kwik_f and '/f/' in r.url:
            kwik_f = r.url
        if not kwik_f:
            logger.error(f"No kwik /f/ link found on pahe.win page (status {r.status_code})")
            return None

    kp = urlparse(kwik_f)
    kwik_base = f"{kp.scheme}://{kp.netloc}"
    page = session.get(kwik_f, headers={"Referer": link}, timeout=30, allow_redirects=True)
    page.raise_for_status()

    token = _kwik_find(page.text, r'name="_token"\s+value="([^"]+)"')
    action = _kwik_find(page.text, r'<form[^>]+action="([^"]+)"') or _kwik_find(
        page.text, r"(https?://kwik\.[a-z]+/d/[A-Za-z0-9]+)")
    if not token or not action:
        logger.error("kwik /f/ page: token or form action not found")
        return None
    if action.startswith('/'):
        action = kwik_base + action

    resp = session.post(
        action, data={"_token": token},
        headers={"Referer": kwik_f, "Origin": kwik_base,
                 "Content-Type": "application/x-www-form-urlencoded"},
        allow_redirects=False, timeout=30,
    )
    mp4 = resp.headers.get("location")
    if not mp4:
        logger.error(f"kwik POST returned {resp.status_code} with no redirect")
        return None

    logger.info(f"Resolved direct MP4: {mp4[:80]}...")
    return {
        'm3u8_url': mp4,   # key name kept for compatibility; this is an MP4 URL
        'headers': {"Referer": f"{kwik_base}/", "User-Agent": KWIK_USER_AGENT},
    }


def _direct_download_sync(url, headers, output_path, state):
    session = _new_scraper()
    with session.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        state['total'] = int(r.headers.get('content-length', 0) or 0)
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 512):
                if chunk:
                    f.write(chunk)
                    state['done'] += len(chunk)
    state['finished'] = True


async def download_m3u8(m3u8_url: str, headers: Dict[str, str], output_path: str,
                        progress_callback=None) -> bool:
    """Direct MP4 download (name kept for compatibility; no m3u8/ffmpeg)."""
    from core.downloader import DownloadProgress

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    state = {'done': 0, 'total': 0, 'finished': False}
    start = time.time()
    task = asyncio.create_task(asyncio.to_thread(
        _direct_download_sync, m3u8_url, headers, output_path, state))

    prog = DownloadProgress(status="downloading")
    last_bytes, last_ts = 0, start
    while not task.done():
        await asyncio.sleep(3)
        now = time.time()
        prog.downloaded_bytes = state['done']
        prog.total_bytes = state['total']
        prog.speed_bps = (state['done'] - last_bytes) / max(now - last_ts, 0.001)
        prog.elapsed = now - start
        if state['total'] and prog.speed_bps > 0:
            prog.eta = (state['total'] - state['done']) / prog.speed_bps
        last_bytes, last_ts = state['done'], now
        if progress_callback:
            try:
                await progress_callback(prog)
            except Exception:
                pass

    try:
        await task
    except Exception as e:
        logger.error(f"Direct download failed: {e}")
        prog.status = "failed"
        if progress_callback:
            try:
                await progress_callback(prog)
            except Exception:
                pass
        return False

    ok = os.path.exists(output_path) and os.path.getsize(output_path) > 1000
    prog.status = "done" if ok else "failed"
    prog.downloaded_bytes = prog.total_bytes = state['done']
    prog.elapsed = time.time() - start
    if progress_callback:
        try:
            await progress_callback(prog)
        except Exception:
            pass
    return ok

def map_resolution_to_quality_tier(resolution: int) -> str:
    if resolution <= 360:
        return "360p"
    elif resolution <= 720:
        return "720p"
    else:
        return "1080p"

def get_quality_streams(stream_links: List[Dict[str, Any]], enabled_qualities: List[str], 
                        preferred_audio: str = "jpn") -> Dict[str, Dict[str, Any]]:
    filtered = [s for s in stream_links if s['audio'] == preferred_audio]
    
    if not filtered:
        filtered = stream_links
        logger.warning(f"No streams found for audio '{preferred_audio}', using all available")
    
    result = {}
    for quality in enabled_qualities:
        target_value = int(quality[:-1])
        
        exact = [s for s in filtered if s['resolution'] == target_value]
        if exact:
            result[quality] = exact[0]
            continue
        
        candidates = [(s['resolution'], s) for s in filtered 
                     if map_resolution_to_quality_tier(s['resolution']) == quality]
        
        if candidates:
            candidates.sort(key=lambda x: x[0])
            if quality == "360p":
                result[quality] = candidates[0][1]
            else:
                result[quality] = candidates[-1][1]
    
    return result

def detect_audio_type(stream_links: List[Dict[str, Any]]) -> str:
    has_eng = any(s['audio'] == 'eng' for s in stream_links)
    has_jpn = any(s['audio'] == 'jpn' for s in stream_links)
    
    if has_eng and not has_jpn:
        return "Dub"
    return "Sub"

def get_sub_dub_streams(stream_links: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    sub_streams = [s for s in stream_links if s['audio'] == 'jpn']
    dub_streams = [s for s in stream_links if s['audio'] == 'eng']
    
    return {
        'sub': sub_streams,
        'dub': dub_streams
    }

async def get_anime_info(title: str) -> Dict[str, Any]:
    query = """
query ($id: Int, $search: String, $seasonYear: Int) {
  Media(id: $id, type: ANIME, search: $search, seasonYear: $seasonYear) {
    id
    idMal
    title {
      romaji
      english
      native
    }
    type
    format
    status(version: 2)
    description(asHtml: false)
    startDate {
      year
      month
      day
    }
    endDate {
      year
      month
      day
    }
    season
    seasonYear
    episodes
    duration
    chapters
    volumes
    countryOfOrigin
    source
    hashtag
    trailer {
      id
      site
      thumbnail
    }
    updatedAt
    coverImage {
      extraLarge
      large
    }
    bannerImage
    genres
    synonyms
    averageScore
    meanScore
    popularity
    trending
    favourites
    studios {
      nodes {
         name
         siteUrl
      }
    }
    isAdult
    nextAiringEpisode {
      airingAt
      timeUntilAiring
      episode
    }
    airingSchedule {
      edges {
        node {
          airingAt
          timeUntilAiring
          episode
        }
      }
    }
    externalLinks {
      url
      site
    }
    relations {
      edges {
        relationType
        node {
          id
          bannerImage
        }
      }
    }
    siteUrl
  }
}

"""

    variables = {'search': title}
    url = 'https://graphql.anilist.co'

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={'query': query, 'variables': variables}, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.error(f"AniList API returned {resp.status}")
                    return {}
                data = await resp.json()
                media = data.get('data', {}).get('Media', {})
                return media if media else {}
    except Exception as e:
        logger.error(f"Error fetching anime info from AniList: {e}")
        return {}


def find_closest_episode(episodes: List[Dict], target_episode: int) -> Optional[Dict]:
    if not episodes:
        return None

    exact = None
    for ep in episodes:
        try:
            ep_num = int(ep.get('episode', 0))
            if ep_num == target_episode:
                exact = ep
                break
        except (ValueError, TypeError):
            continue

    if exact:
        return exact

    closest = None
    min_diff = float('inf')
    for ep in episodes:
        try:
            ep_num = int(ep.get('episode', 0))
            diff = abs(ep_num - target_episode)
            if diff < min_diff:
                min_diff = diff
                closest = ep
        except (ValueError, TypeError):
            continue

    return closest


async def download_anime_poster(title: str, save_dir: str = None) -> Optional[str]:
    try:
        info = await get_anime_info(title)
        if not info:
            return None

        image_url = info.get('bannerImage')

        if not image_url:
            relations = info.get('relations', {}).get('edges', [])
            for rel in relations:
                if rel.get('relationType') in ('PREQUEL', 'PARENT', 'SOURCE'):
                    node_banner = rel.get('node', {}).get('bannerImage')
                    if node_banner:
                        image_url = node_banner
                        break
            if not image_url:
                for rel in relations:
                    node_banner = rel.get('node', {}).get('bannerImage')
                    if node_banner:
                        image_url = node_banner
                        break

        if not image_url:
            cover_image = info.get('coverImage', {})
            if cover_image:
                image_url = cover_image.get('extraLarge') or cover_image.get('large') or cover_image.get('medium')

        if not image_url:
            return None

        if save_dir is None:
            save_dir = str(Path(__file__).parent.parent / "thumbnails")

        os.makedirs(save_dir, exist_ok=True)
        safe_title = re.sub(r'[^\w\s-]', '', title).strip().replace(' ', '_')[:50]
        save_path = os.path.join(save_dir, f"{safe_title}_poster.jpg")

        if os.path.exists(save_path) and os.path.getsize(save_path) > 1000:
            return save_path

        async with aiohttp.ClientSession() as session:
            async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    with open(save_path, 'wb') as f:
                        f.write(data)
                    if os.path.getsize(save_path) > 1000:
                        return save_path

        return None
    except Exception as e:
        logger.error(f"Error downloading anime poster: {e}")
        return None
