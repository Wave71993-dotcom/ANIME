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
    print()
    print("GET:", url)

    r = session.get(url, timeout=30)

    print("Status:", r.status_code)

    r.raise_for_status()
    return r.json()


def find_hls(streams):
    if not isinstance(streams, list):
        return None

    for stream in streams:
        if not isinstance(stream, dict):
            continue

        url = stream.get("url", "")

        if isinstance(url, str) and ".m3u8" in url:
            return url

    return None


def parse_master_playlist(url):
    print()
    print("Testing HLS:")
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

        print("Master playlist status:", r.status_code)

        if r.status_code != 200:
            print("FAILED: Cannot access master playlist")
            return []

        text = r.text

        print("Playlist size:", len(text), "bytes")

        if "#EXTM3U" not in text:
            print("FAILED: Response is not a valid HLS playlist")
            print(text[:500])
            return []

        qualities = []

        lines = text.splitlines()

        for line in lines:

            if not line.startswith("#EXT-X-STREAM-INF:"):
                continue

            resolution_match = re.search(
                r"RESOLUTION=(\d+)x(\d+)",
                line
            )

            bandwidth_match = re.search(
                r"BANDWIDTH=(\d+)",
                line
            )

            if not resolution_match:
                continue

            width = int(resolution_match.group(1))
            height = int(resolution_match.group(2))

            if height >= 1080:
                quality = "1080p"
            elif height >= 720:
                quality = "720p"
            elif height >= 480:
                quality = "480p"
            elif height >= 360:
                quality = "360p"
            else:
                quality = str(height) + "p"

            bandwidth = (
                bandwidth_match.group(1)
                if bandwidth_match
                else "unknown"
            )

            qualities.append({
                "quality": quality,
                "resolution": f"{width}x{height}",
                "bandwidth": bandwidth
            })

        return qualities

    except Exception as e:
        print("FAILED: HLS error:", e)
        return []


def find_episode_ids(obj, result=None):
    if result is None:
        result = {
            "sub": None,
            "dub": None
        }

    if isinstance(obj, dict):

        episode_id = obj.get("id")

        if isinstance(episode_id, str):

            if "/sub/" in episode_id and result["sub"] is None:
                result["sub"] = episode_id

            if "/dub/" in episode_id and result["dub"] is None:
                result["dub"] = episode_id

        for value in obj.values():
            find_episode_ids(value, result)

    elif isinstance(obj, list):

        for value in obj:
            find_episode_ids(value, result)

    return result


def main():

    print("=" * 60)
    print("HIYORI STREAM TEST")
    print("=" * 60)

    # --------------------------------------------------
    # SEARCH
    # --------------------------------------------------

    search_url = (
        f"{API}/suggestions?query={quote(ANIME_NAME)}"
    )

    search = get_json(search_url)

    if not search:
        print("FAILED: No anime found")
        return

    anime = None

    for item in search:

        if not isinstance(item, dict):
            continue

        title = str(item.get("title", ""))

        if "one piece" in title.lower():
            anime = item
            break

    if anime is None:
        anime = search[0]

    anime_id = anime.get("id")

    print()
    print("Anime:", anime.get("title"))
    print("AniList ID:", anime_id)

    if not anime_id:
        print("FAILED: Anime ID not found")
        return

    # --------------------------------------------------
    # EPISODES
    # --------------------------------------------------

    episodes = get_json(
        f"{API}/episodes/{anime_id}"
    )

    episode_ids = find_episode_ids(episodes)

    sub_episode = episode_ids["sub"]
    dub_episode = episode_ids["dub"]

    print()
    print("SUB episode:", sub_episode)
    print("DUB episode:", dub_episode)

    results = {}

    # --------------------------------------------------
    # SUB + DUB STREAM TEST
    # --------------------------------------------------

    for language, episode_id in [
        ("SUB", sub_episode),
        ("DUB", dub_episode)
    ]:

        print()
        print("=" * 60)
        print(language)
        print("=" * 60)

        if not episode_id:
            print("FAILED:", language, "episode not found")
            continue

        stream_url = f"{API}/{episode_id}"

        streams = get_json(stream_url)

        print()
        print("Streams found:", len(streams))

        hls_url = find_hls(streams)

        if not hls_url:
            print("FAILED: No HLS stream found")
            continue

        qualities = parse_master_playlist(hls_url)

        results[language] = qualities

        if not qualities:
            print("FAILED: No quality variants detected")
            continue

        print()
        print("Available qualities:")

        for item in qualities:

            print(
                "  OK",
                item["quality"],
                "|",
                item["resolution"],
                "| bandwidth:",
                item["bandwidth"]
            )

    # --------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------

    print()
    print("=" * 60)
    print("FINAL RESULT")
    print("=" * 60)

    for language in ["SUB", "DUB"]:

        qualities = results.get(language, [])

        found = {
            item["quality"]
            for item in qualities
        }

        print()
        print(language + ":")

        for quality in ["480p", "720p", "1080p"]:

            if quality in found:
                print("  OK", quality)
            else:
                print("  NO", quality)

    print()
    print("Test completed.")


if __name__ == "__main__":
    main()
