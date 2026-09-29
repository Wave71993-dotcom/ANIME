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

    data = r.json()

    print("Response type:", type(data).__name__)

    return data


def get_search_results(data):
    """
    Hiyori may return either:

    [
        {...},
        {...}
    ]

    or:

    {
        "results": [...]
    }

    or another dictionary containing the results.
    """

    if isinstance(data, list):
        return data

    if isinstance(data, dict):

        # Common possibilities
        for key in ["results", "data", "suggestions", "items", "animes"]:

            value = data.get(key)

            if isinstance(value, list):
                return value

        # Sometimes the API can return a single anime object
        if "id" in data:
            return [data]

    return []


def find_anime(results):

    if not results:
        return None

    # First try exact/close One Piece match
    for item in results:

        if not isinstance(item, dict):
            continue

        title = str(
            item.get("title")
            or item.get("name")
            or ""
        )

        if "one piece" in title.lower():
            return item

    # Otherwise use first result
    return results[0]


def find_hls(streams):

    if isinstance(streams, dict):

        for key in ["streams", "data", "results"]:

            if isinstance(streams.get(key), list):
                streams = streams[key]
                break

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

            print("FAILED: Not a valid HLS playlist")

            print(text[:500])

            return []

        qualities = []

        for line in text.splitlines():

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

            if height >= 2160:
                quality = "2160p"

            elif height >= 1080:
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

            if "/sub/" in episode_id:

                if result["sub"] is None:
                    result["sub"] = episode_id

            if "/dub/" in episode_id:

                if result["dub"] is None:
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

    search_data = get_json(search_url)

    results = get_search_results(search_data)

    print()
    print("Search results found:", len(results))

    if not results:

        print("FAILED: No search results")

        print()
        print("Raw response:")

        print(search_data)

        return

    anime = find_anime(results)

    if not anime:

        print("FAILED: Could not select anime")

        return

    anime_id = anime.get("id")

    title = (
        anime.get("title")
        or anime.get("name")
        or "Unknown"
    )

    print()
    print("Anime:", title)
    print("AniList ID:", anime_id)

    if not anime_id:

        print("FAILED: Anime ID not found")

        print("Selected object:")
        print(anime)

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

    results_by_language = {}

    # --------------------------------------------------
    # TEST SUB + DUB
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

            print(
                "FAILED:",
                language,
                "episode not found"
            )

            continue

        stream_url = f"{API}/{episode_id}"

        streams = get_json(stream_url)

        if isinstance(streams, list):

            print(
                "Streams found:",
                len(streams)
            )

        elif isinstance(streams, dict):

            print(
                "Stream response is dictionary"
            )

        hls_url = find_hls(streams)

        if not hls_url:

            print("FAILED: No HLS stream found")

            continue

        qualities = parse_master_playlist(hls_url)

        results_by_language[language] = qualities

        if not qualities:

            print(
                "FAILED: No quality variants detected"
            )

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

        qualities = results_by_language.get(
            language,
            []
        )

        found = {
            item["quality"]
            for item in qualities
        }

        print()
        print(language + ":")

        for quality in [
            "480p",
            "720p",
            "1080p",
            "2160p"
        ]:

            if quality in found:

                print(
                    "  OK",
                    quality
                )

            else:

                print(
                    "  NO",
                    quality
                )

    print()
    print("Test completed.")


if __name__ == "__main__":
    main()
