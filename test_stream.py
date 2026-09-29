import requests
import re
from urllib.parse import quote

API = "https://api.hiyori.tv"
ANIME_NAME = "One Piece"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
})


def get_json(url):
    r = session.get(url, timeout=30)
    print(f"\nGET {url}")
    print(f"Status: {r.status_code}")

    r.raise_for_status()
    return r.json()


def find_hls(streams):
    for stream in streams:
        url = stream.get("url", "")

        if ".m3u8" in url:
            return url

    return None


def parse_master_playlist(url):
    print(f"\nTesting HLS:")
    print(url)

    try:
        r = session.get(
            url,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://megaplay.buzz/"
            }
        )

        print(f"Master playlist status: {r.status_code}")

        if r.status_code != 200:
            print("❌ Cannot access master playlist")
            return []

        text = r.text

        print(f"Playlist size: {len(text)} bytes")

        if "#EXTM3U" not in text:
            print("❌ Response is not a valid HLS playlist")
            print(text[:500])
            return []

        qualities = []

        lines = text.splitlines()

        for i, line in enumerate(lines):

            if line.startswith("#EXT-X-STREAM-INF:"):

                resolution = re.search(
                    r"RESOLUTION=(\d+)x(\d+)",
                    line
                )

                bandwidth = re.search(
                    r"BANDWIDTH=(\d+)",
                    line
                )

                if resolution:
                    width = int(resolution.group(1))
                    height = int(resolution.group(2))

                    if height >= 1080:
                        quality = "1080p"
                    elif height >= 720:
                        quality = "720p"
                    elif height >= 480:
                        quality = "480p"
                    elif height >= 360:
                        quality = "360p"
                    else:
                        quality = f"{height}p"

                    qualities.append({
                        "quality": quality,
                        "resolution": f"{width}x{height}",
                        "bandwidth": bandwidth.group(1) if bandwidth else "unknown"
                    })

        return qualities

    except Exception as e:
        print(f"❌ HLS error: {e}")
        return []


def main():

    print("=" * 60)
    print("HIYORI STREAM TEST")
    print("=" * 60)

    # -------------------------------------------------
    # SEARCH
    # -------------------------------------------------

    search_url = f"{API}/suggestions?query={quote(ANIME_NAME)}"

    search = get_json(search_url)

    if not search:
        print("❌ No anime found")
        return

    anime = None

    for item in search:
        title = str(item.get("title", ""))

        if "one piece" in title.lower():
            anime = item
            break

    if not anime:
        anime = search[0]

    anime_id = anime.get("id")

    print("\nAnime:")
    print(anime.get("title"))
    print("AniList ID:", anime_id)

    # -------------------------------------------------
    # EPISODES
    # -------------------------------------------------

    episodes = get_json(f"{API}/episodes/{anime_id}")

    print("\nProviders:")

    if isinstance(episodes, dict):
        for provider, data in episodes.items():
            print("-", provider)

    # -------------------------------------------------
    # FIND SUB/DUB EPISODE
    # -------------------------------------------------

    sub_episode = None
    dub_episode = None

    def search_episode(obj):

        nonlocal sub_episode, dub_episode

        if isinstance(obj, dict):

            ep_id = obj.get("id")

            if isinstance(ep_id, str):

                if "/sub/" in ep_id and not sub_episode:
                    sub_episode = ep_id

                if "/dub/" in ep_id and not dub_episode:
                    dub_episode = ep_id

            for value in obj.values():
                search_episode(value)

        elif isinstance(obj, list):

            for value in obj:
                search_episode(value)

    search_episode(episodes)

    print("\nSUB episode:", sub_episode)
    print("DUB episode:", dub_episode)

    results = {}

    # -------------------------------------------------
    # TEST SUB + DUB
    # -------------------------------------------------

    for language, episode_id in [
        ("SUB", sub_episode),
        ("DUB", dub_episode)
    ]:

        print("\n" + "=" * 60)
        print(language)
        print("=" * 60)

        if not episode_id:
            print(f"❌ {language} episode not found")
            continue

        stream_url = f"{API}/{episode_id}"

        streams = get_json(stream_url)

        print(f"\nStreams found: {len(streams)}")

        hls_url = find_hls(streams)

        if not hls_url:
            print("❌
