import requests
import sys
import json

BASE_URL = "https://api.hiyori.tv"
ANIME_NAME = "One Piece"
EPISODE_NUMBER = 1

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})


def get_json(url):
    print(f"\n→ GET {url}")

    try:
        response = session.get(url, timeout=30)

        print(f"  HTTP {response.status_code}")

        if response.status_code != 200:
            print(f"  ERROR: {response.text[:500]}")
            return None

        return response.json()

    except requests.exceptions.Timeout:
        print("  ERROR: Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: {e}")
    except ValueError:
        print("  ERROR: Response was not JSON")
        print(response.text[:500])

    return None


# ============================================================
# 1. SEARCH
# ============================================================

print("=" * 60)
print("1. SEARCH")
print("=" * 60)

search_url = f"{BASE_URL}/suggestions"

data = get_json(search_url + "?query=" + requests.utils.quote(ANIME_NAME))

if not data:
    print("\n❌ SEARCH FAILED")
    sys.exit(1)

print("✅ SEARCH WORKS")

# API may return a list or an object containing suggestions
if isinstance(data, dict):
    results = data.get("suggestions", [])
else:
    results = data

if not results:
    print("❌ No anime found")
    sys.exit(1)

print(f"Found {len(results)} result(s)")

for i, anime in enumerate(results[:5]):
    print(
        f"{i + 1}. "
        f"{anime.get('title', 'Unknown')} "
        f"(ID: {anime.get('id')})"
    )

# Select first result
anime = results[0]

anilist_id = anime.get("id")

if not anilist_id:
    print("❌ Could not find AniList ID")
    sys.exit(1)

print(f"\nSelected:")
print(f"Title: {anime.get('title')}")
print(f"AniList ID: {anilist_id}")


# ============================================================
# 2. GET EPISODES
# ============================================================

print("\n" + "=" * 60)
print("2. EPISODES")
print("=" * 60)

episodes_url = f"{BASE_URL}/episodes/{anilist_id}"

episodes_data = get_json(episodes_url)

if not episodes_data:
    print("\n❌ EPISODE REQUEST FAILED")
    sys.exit(1)

print("✅ EPISODE API WORKS")

providers = episodes_data.get("providers", {})

if not providers:
    print("❌ No providers returned")
    print(json.dumps(episodes_data, indent=2)[:3000])
    sys.exit(1)

print(f"Providers found: {', '.join(providers.keys())}")


# ============================================================
# 3. FIND SUB + DUB EPISODES
# ============================================================

print("\n" + "=" * 60)
print("3. SUB / DUB")
print("=" * 60)

selected_sub = None
selected_dub = None

for provider_name, provider_data in providers.items():

    episode_groups = provider_data.get("episodes", {})

    sub_episodes = episode_groups.get("sub", [])
    dub_episodes = episode_groups.get("dub", [])

    print(
        f"\nProvider: {provider_name}"
        f"\n  SUB episodes: {len(sub_episodes)}"
        f"\n  DUB episodes: {len(dub_episodes)}"
    )

    # Find requested episode
    if not selected_sub:
        for ep in sub_episodes:
            if ep.get("number") == EPISODE_NUMBER:
                selected_sub = {
                    "provider": provider_name,
                    "episode": ep
                }
                break

    if not selected_dub:
        for ep in dub_episodes:
            if ep.get("number") == EPISODE_NUMBER:
                selected_dub = {
                    "provider": provider_name,
                    "episode": ep
                }
                break


print("\n--- SUB ---")

if selected_sub:
    print("✅ SUB episode found")
    print("Provider:", selected_sub["provider"])
    print("Episode ID:", selected_sub["episode"].get("id"))
else:
    print("❌ SUB episode not found")


print("\n--- DUB ---")

if selected_dub:
    print("✅ DUB episode found")
    print("Provider:", selected_dub["provider"])
    print("Episode ID:", selected_dub["episode"].get("id"))
else:
    print("⚠️ DUB episode not found")


# ============================================================
# 4. STREAM EXTRACTION
# ============================================================

def test_stream(language, selected):

    if not selected:
        return

    provider = selected["provider"]
    episode = selected["episode"]

    episode_id = episode.get("id")

    print("\n" + "=" * 60)
    print(f"4. {language} STREAM EXTRACTION")
    print("=" * 60)

    if not episode_id:
        print("❌ Episode has no stream ID")
        return

    # The API documentation says the episode ID itself can be
    # used as the /watch/... path.
    watch_url = f"{BASE_URL}/{episode_id}"

    stream_data = get_json(watch_url)

    if not stream_data:
        print(f"❌ {language} STREAM EXTRACTION FAILED")
        return

    print(f"✅ {language} STREAM API WORKS")

    streams = stream_data.get("streams", [])

    if not streams:
        print("❌ No streams returned")
        print(json.dumps(stream_data, indent=2)[:5000])
        return

    print(f"\nStreams found: {len(streams)}")

    for i, stream in enumerate(streams, 1):

        quality = stream.get("quality", "Unknown")
        stream_type = stream.get("type", "Unknown")
        stream_url = stream.get("url")

        print(f"\n{i}. Quality: {quality}")
        print(f"   Type: {stream_type}")

        if stream_url:
            print(f"   URL: {stream_url[:200]}...")
        else:
            print("   URL: MISSING")

    subtitles = stream_data.get("subtitles", [])

    print(f"\nSubtitles found: {len(subtitles)}")

    for subtitle in subtitles:
        print(
            f"  - {subtitle.get('label')}: "
            f"{subtitle.get('file', '')[:100]}"
        )


# Test SUB
test_stream("SUB", selected_sub)

# Test DUB
test_stream("DUB", selected_dub)


# ============================================================
# FINISHED
# ============================================================

print("\n" + "=" * 60)
print("TEST FINISHED")
print("=" * 60)

print("""
If you see:

✅ SEARCH WORKS
✅ EPISODE API WORKS
✅ SUB episode found
✅ DUB episode found
✅ SUB STREAM API WORKS
✅ DUB STREAM API WORKS

then Railway is able to communicate with the API and obtain
stream information.

Quality availability depends on the particular episode/source.
""")
