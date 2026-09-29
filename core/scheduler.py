from __future__ import annotations
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import schedule
from zoneinfo import ZoneInfo

from core.config import (
    DOWNLOAD_DIR, ADMIN_CHAT_ID,
    CHANNEL_USERNAME, BOT_USERNAME, CHANNEL_NAME,
    DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME
)
from core.client import client, FFMPEG_AVAILABLE, currently_processing
from core.state import (
    auto_download_state, quality_settings, anime_queue, bot_settings,
    episode_tracker, EpisodeState, deferred_episodes
)
from core.utils import (
    sanitize_filename, format_filename, format_size, format_speed,
    get_fixed_thumbnail, is_episode_processed, update_processed_qualities, mark_episode_processed,
    ProgressMessage, UploadProgressBar, safe_edit,
    generate_batch_link, generate_single_link
)
from core.anime_api import (
    search_anime, get_all_episodes, get_latest_releases,
    get_stream_links, extract_m3u8_from_kwik, download_m3u8,
    get_quality_streams, detect_audio_type, get_anime_info,
    find_closest_episode, map_resolution_to_quality_tier, download_anime_poster
)
from core.download import (
    rename_video_with_ffmpeg, robust_upload_file
)

logger = logging.getLogger(__name__)

from telethon.errors import FloodWaitError
from telethon.tl.custom import Button

_currently_processing = False
_scheduler_lock = asyncio.Lock() if asyncio else None

_request_time_job_tag = "daily_request_processing"

def get_currently_processing():
    return _currently_processing

def set_currently_processing(value: bool):
    global _currently_processing
    _currently_processing = value

def _get_scheduler_lock():
    global _scheduler_lock
    if _scheduler_lock is None:
        _scheduler_lock = asyncio.Lock()
    return _scheduler_lock


async def _get_best_image(anime_info):
    if not anime_info:
        return None

    banner = anime_info.get('coverImage')
    if banner:
        return banner

    relations = anime_info.get('relations', {}).get('edges', [])
    for rel in relations:
        node_banner = rel.get('node', {}).get('coverImage')
        if node_banner:
            return node_banner

    try:
        import aiohttp
        anilist_id = anime_info.get('id')
        if not anilist_id:
            cover_data = anime_info.get('coverImage', {})
            return cover_data.get('extraLarge') or cover_data.get('large')

        query = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    relations {
      edges {
        relationType
        node {
          id
          bannerImage
        }
      }
    }
  }
}
"""
        visited = {anilist_id}
        queue = []
        for rel in relations:
            if rel.get('relationType') in ('PREQUEL', 'PARENT'):
                nid = rel.get('node', {}).get('id')
                if nid and nid not in visited:
                    queue.append(nid)
                    visited.add(nid)

        url = 'https://graphql.anilist.co'
        async with aiohttp.ClientSession() as session:
            for _ in range(5):
                if not queue:
                    break
                current_id = queue.pop(0)
                async with session.post(url, json={'query': query, 'variables': {'id': current_id}}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    media = data.get('data', {}).get('Media', {})
                    if not media:
                        continue
                    edges = media.get('relations', {}).get('edges', [])
                    for rel in edges:
                        node = rel.get('node', {})
                        nb = node.get('bannerImage')
                        if nb:
                            return nb
                        if rel.get('relationType') in ('PREQUEL', 'PARENT'):
                            nid = node.get('id')
                            if nid and nid not in visited:
                                queue.append(nid)
                                visited.add(nid)
    except Exception as e:
        logger.warning(f"Error walking prequel chain for banner: {e}")

    cover_data = anime_info.get('coverImage', {})
    return cover_data.get('extraLarge') or cover_data.get('large')

async def _resolve_channel_target(client, channel_id, channel_username):
    """Resolve a Telethon channel to an entity with a valid access hash.

    A raw numeric channel ID is not enough for Telethon when the entity is
    absent from the session cache. Prefer username, otherwise search dialogs.
    """
    if channel_username:
        username = str(channel_username).lstrip("@")
        try:
            return await client.get_entity(username)
        except Exception as exc:
            logger.warning("Could not resolve channel username @%s: %s", username, exc)

    if channel_id:
        try:
            wanted = int(channel_id)
        except (TypeError, ValueError):
            wanted = None

        if wanted is not None:
            # Telethon IDs may appear signed/unsigned depending on how they
            # were stored, so compare the absolute channel ID as well.
            wanted_abs = abs(wanted)
            try:
                async for dialog in client.iter_dialogs():
                    entity = dialog.entity
                    entity_id = getattr(entity, "id", None)
                    if entity_id is not None and (int(entity_id) == wanted_abs or abs(int(entity_id)) == wanted_abs):
                        return entity
            except Exception as exc:
                logger.warning("Could not search Telegram dialogs for channel %s: %s", wanted, exc)

            try:
                return await client.get_entity(wanted)
            except Exception as exc:
                logger.error(
                    "Could not resolve channel ID %s. Set CHANNEL_USERNAME to the channel username "
                    "or make sure the channel is present in the Telethon account's dialogs: %s",
                    wanted, exc
                )

    return None


async def _get_saved_media_message(client, anime_title, kind):
    try:
        media_map = bot_settings.get(f"anime_{kind}s", {}) or {}
        ref = media_map.get(anime_title)
        if not ref or not ref.get("chat_id") or not ref.get("message_id"):
            return None
        return await client.get_messages(ref["chat_id"], ids=ref["message_id"])
    except Exception as exc:
        logger.warning("Could not load saved %s for %s: %s", kind, anime_title, exc)
        return None

async def _download_saved_media_url(url, path):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    with open(path, "wb") as fh:
                        fh.write(await resp.read())
                    return path
    except Exception as exc:
        logger.warning("Could not download saved media URL: %s", exc)
    return None

async def _get_anime_thumb_path(client, anime_title):
    media_map = bot_settings.get("anime_thumbs", {}) or {}
    ref = media_map.get(anime_title)
    if not ref:
        return await get_fixed_thumbnail()
    path = os.path.join(DOWNLOAD_DIR, f"thumb_{sanitize_filename(anime_title)}.jpg")
    try:
        if ref.get("url"):
            return await _download_saved_media_url(ref["url"], path) or await get_fixed_thumbnail()
        msg = await _get_saved_media_message(client, anime_title, "thumb")
        if msg and msg.media:
            result = await client.download_media(msg, file=path)
            return result or await get_fixed_thumbnail()
    except Exception as exc:
        logger.warning("Could not prepare anime thumb: %s", exc)
    return await get_fixed_thumbnail()



async def _get_default_poster_path(anime_title, anime_info, suffix=""):
    """Get the bot's original/default anime poster when no custom poster is set.

    Preserve the old img.anili.st poster first, then use AniList cover/banner
    fallbacks so a missing custom poster never silently removes the poster.
    """
    safe = sanitize_filename(anime_title)
    path = os.path.join(DOWNLOAD_DIR, f"poster_{safe}{suffix}.jpg")

    candidates = []
    ani_id = anime_info.get("id") if anime_info else None
    if ani_id:
        # This was the original poster source used by the bot.
        candidates.append(f"https://img.anili.st/media/{ani_id}")

    if anime_info:
        cover = anime_info.get("coverImage") or {}
        if isinstance(cover, dict):
            candidates.extend([cover.get("extraLarge"), cover.get("large"), cover.get("medium")])
        elif isinstance(cover, str):
            candidates.append(cover)
        candidates.append(anime_info.get("bannerImage"))

    for url in candidates:
        if not url:
            continue
        try:
            result = await _download_saved_media_url(url, path)
            if result and os.path.exists(result) and os.path.getsize(result) > 1000:
                logger.info("Using default anime poster for %s: %s", anime_title, url)
                return result
        except Exception as exc:
            logger.warning("Default poster source failed for %s: %s", anime_title, exc)

    # Final fallback uses the existing AniList poster downloader/cache.
    try:
        result = await download_anime_poster(anime_title, save_dir=DOWNLOAD_DIR)
        if result and os.path.exists(result) and os.path.getsize(result) > 1000:
            logger.info("Using AniList fallback poster for %s", anime_title)
            return result
    except Exception as exc:
        logger.warning("AniList fallback poster failed for %s: %s", anime_title, exc)

    return None

async def _get_anime_poster_media(client, anime_title):
    media_map = bot_settings.get("anime_posters", {}) or {}
    ref = media_map.get(anime_title)
    if ref:
        if ref.get("url"):
            path = os.path.join(DOWNLOAD_DIR, f"poster_{sanitize_filename(anime_title)}.jpg")
            return await _download_saved_media_url(ref["url"], path)
        msg = await _get_saved_media_message(client, anime_title, "poster")
        if msg and msg.media:
            return msg.media
    return None

async def _send_anime_sticker(client, target, anime_title):
    try:
        msg = await _get_saved_media_message(client, anime_title, "sticker")
        if msg and msg.media:
            await client.send_file(target, msg.media)
    except Exception as exc:
        logger.warning("Could not send anime sticker for %s: %s", anime_title, exc)

def _post_format_caption(anime_title, anime_info, episode_number, audio_type, channel_format):
    titles = (anime_info or {}).get('title', {})
    english = titles.get('english') or ''
    romaji = titles.get('romaji') or anime_title
    genres = ', '.join((anime_info or {}).get('genres', [])[:4])
    score = (anime_info or {}).get('averageScore', '')
    studios = ', '.join([x.get('name','') for x in ((anime_info or {}).get('studios', {}) or {}).get('nodes', [])[:2]])
    audio = 'Japanese' if audio_type == 'Sub' else 'English'
    fmt = bot_settings.get('post_formats', {}) or {}
    template = fmt.get('caption_template', '') if isinstance(fmt, dict) else ''
    if template:
        try:
            return template.format(title=romaji, english=english, episode=episode_number, audio=audio,
                                   genres=genres, score=score, studio=studios, channel=channel_format)
        except Exception as exc:
            logger.warning("Invalid custom caption format: %s", exc)
    caption = (f"<b><blockquote>✦ {english or romaji} ✦</blockquote>\n"
               f"──────────────────\n<blockquote>"
               f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n・ Aᴜᴅɪᴏ: {audio}")
    if genres:
        caption += f"\n・ Gᴇɴʀᴇs: {genres}"
    caption += (f"</blockquote>\n──────────────────\n"
                 f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>")
    return caption

def _custom_buttons(anime_title):
    data = bot_settings.get('anime_custom_buttons', {}) or {}
    return [Button.url(str(item.get('text','Button'))[:64], str(item.get('url'))) for item in data.get(anime_title, [])
            if isinstance(item, dict) and item.get('url')]


async def post_anime_with_buttons(client, anime_title, anime_info, episode_number, audio_type, quality_files):
    from core.config import CHANNEL_ID, CHANNEL_USERNAME, FIXED_THUMBNAIL_URL
    from core.database import get_anime_channel

    anime_channel = await get_anime_channel(anime_title)
    target_id = anime_channel.get("channel_id") if anime_channel else CHANNEL_ID
    target_username = anime_channel.get("channel_username") if anime_channel else CHANNEL_USERNAME
    channel_target = await _resolve_channel_target(client, target_id, target_username)
    if not channel_target:
        logger.warning("No resolvable main channel configured for posting")
        return

    channel_format = (target_username or CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')

    try:
        title_romaji = anime_title
        title_english = ""
        genres = ""
        score = ""
        studios = ""

        if anime_info:
            titles = anime_info.get('title', {})
            title_romaji = titles.get('romaji', anime_title)
            title_english = titles.get('english', '')
            genres = ', '.join(anime_info.get('genres', [])[:4])
            score = anime_info.get('averageScore', '')
            studio_nodes = anime_info.get('studios', {}).get('nodes', [])
            studios = ', '.join([s['name'] for s in studio_nodes[:2]]) if studio_nodes else ''

        if audio_type == "Sub":
            audio_alpha = "Japanese"
        else:
            audio_alpha = "English"

        caption = _post_format_caption(anime_title, anime_info, episode_number, audio_type, channel_format)

        button_list = []
        sorted_qualities = sorted(quality_files.keys(), key=lambda x: int(x[:-1]))

        for quality in sorted_qualities:
            msg_ids = quality_files[quality]
            if not msg_ids:
                continue

            if len(msg_ids) == 1:
                link = await generate_single_link(msg_ids[0])
            else:
                link = await generate_batch_link(msg_ids)

            quality_map = {
                "360p": "𝟯𝟲𝟬𝗣",
                "720p": "𝟳𝟮𝟬𝗣",
                "1080p": "𝟭𝟬𝟴𝟬𝗣"
            }

            quality_btn = quality_map.get(quality, quality)

            if link:
                button_list.append(Button.url(f"{quality_btn}", link))

        if not button_list:
            logger.error("No valid download links generated for buttons")
            return

        button_list.extend(_custom_buttons(anime_title))
        fmt = bot_settings.get('post_formats', {}) or {}
        try: layout = int(fmt.get('button_layout', 2))
        except Exception: layout = 2
        buttons = _arrange_buttons(button_list, layout)

        poster_media = await _get_anime_poster_media(client, anime_title)
        if poster_media:
            await client.send_file(channel_target, poster_media, caption=caption, parse_mode='html', buttons=buttons, link_preview=False)
        else:
            poster_path = await _get_default_poster_path(anime_title, anime_info)
            if poster_path and os.path.exists(poster_path):
                await client.send_file(channel_target, poster_path, caption=caption, parse_mode='html', buttons=buttons, link_preview=False)
                try: os.remove(poster_path)
                except: pass
            else:
                await client.send_message(channel_target, caption, parse_mode='html', buttons=buttons, link_preview=False)
        await _send_anime_sticker(client, channel_target, anime_title)
        logger.info(f"Posted {anime_title} Episode {episode_number} to channel with {len(button_list)} buttons")

    except FloodWaitError as e:
        logger.warning(f"Flood wait during post: {e.seconds}s")
        await asyncio.sleep(e.seconds + 5)
        raise
    except Exception as e:
        logger.error(f"Error posting anime with buttons: {e}")
        raise


def _arrange_buttons(button_list, per_row=2):
    try: per_row = max(1, min(3, int(per_row)))
    except Exception: per_row = 2
    return [button_list[i:i + per_row] for i in range(0, len(button_list), per_row)]


async def post_anime_batch_with_buttons(client, anime_title, anime_info, quality_files, total_episodes, audio_type):
    from core.config import CHANNEL_ID, CHANNEL_USERNAME
    from core.database import get_anime_channel

    anime_channel = await get_anime_channel(anime_title)
    target_id = anime_channel.get("channel_id") if anime_channel else CHANNEL_ID
    target_username = anime_channel.get("channel_username") if anime_channel else CHANNEL_USERNAME
    channel_target = await _resolve_channel_target(client, target_id, target_username)
    if not channel_target:
        logger.warning("No resolvable main channel configured for posting")
        return

    channel_format = (target_username or CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')

    try:
        title_romaji = anime_title
        title_english = ""
        genres = ""

        if anime_info:
            titles = anime_info.get('title', {})
            title_romaji = titles.get('romaji', anime_title)
            title_english = titles.get('english', '')
            genres = ', '.join(anime_info.get('genres', [])[:4])

        if audio_type == "Sub":
            audio_alpha = "Japanese"
        else:
            audio_alpha = "English"

        caption = _post_format_caption(anime_title, anime_info, f"1-{total_episodes}", audio_type, channel_format)

        button_list = []
        sorted_qualities = sorted(quality_files.keys(), key=lambda x: int(x[:-1]))

        for quality in sorted_qualities:
            msg_ids = quality_files[quality]
            if not msg_ids:
                continue

            if len(msg_ids) == 1:
                link = await generate_single_link(msg_ids[0])
            else:
                link = await generate_batch_link(msg_ids)

            quality_map = {
                "360p": "𝟯𝟲𝟬𝗣",
                "720p": "𝟳𝟮𝟬𝗣",
                "1080p": "𝟭𝟬𝟴𝟬𝗣"
            }

            quality_btn = quality_map.get(quality, quality)

            if link:
                button_list.append(Button.url(f"{quality} - {total_episodes} Episodes", link))

        if not button_list:
            logger.error("No valid download links for batch buttons")
            return

        button_list.extend(_custom_buttons(anime_title))
        fmt = bot_settings.get('post_formats', {}) or {}
        try: layout = int(fmt.get('button_layout', 2))
        except Exception: layout = 2
        buttons = _arrange_buttons(button_list, layout)
        poster_media = await _get_anime_poster_media(client, anime_title)
        if poster_media:
            await client.send_file(channel_target, poster_media, caption=caption, parse_mode='html', buttons=buttons, link_preview=False)
        else:
            poster_path = await _get_default_poster_path(anime_title, anime_info, suffix="_batch")
            if poster_path and os.path.exists(poster_path):
                await client.send_file(channel_target, poster_path, caption=caption, parse_mode='html', buttons=buttons, link_preview=False)
                try: os.remove(poster_path)
                except: pass
            else:
                await client.send_message(channel_target, caption, parse_mode='html', buttons=buttons, link_preview=False)
        await _send_anime_sticker(client, channel_target, anime_title)
        logger.info(f"Posted batch: {anime_title} ({total_episodes} episodes) to channel")

    except Exception as e:
        logger.error(f"Error posting anime batch with buttons: {e}")

async def _download_and_upload_single_quality(
    anime_title, episode_number, quality, stream_info, audio_type, progress=None, channel_format=""
):
    download_path = None
    try:
        kwik_url = stream_info['url']
        resolution = stream_info['resolution']
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
                f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ URL...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
        if not m3u8_data:
            logger.error(f"Failed to extract m3u8 from {kwik_url}")
            return None
        
        m3u8_url = m3u8_data['m3u8_url']
        m3u8_headers = m3u8_data['headers']
        
        base_name = format_filename(anime_title, episode_number, quality, audio_type)
        main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
        full_caption = f"**{base_name} {main_channel_username}.mkv**"
        filename = sanitize_filename(full_caption)
        download_path = os.path.join(DOWNLOAD_DIR, filename)
        
        from core.dl_progress import FFmpegProgressReporter, make_upload_status_text
        dl_reporter = FFmpegProgressReporter(
            progress_message=progress,
            anime_title=anime_title,
            episode_number=episode_number,
            quality=quality,
            audio_type=audio_type,
            channel_format=channel_format,
        )
        
        download_start = time.time()
        success = await download_m3u8(m3u8_url, m3u8_headers, download_path,
                                       progress_callback=dl_reporter.callback)
        
        if not success:
            logger.error(f"M3U8 download failed for {quality}")
            return None
        
        if not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
            logger.error(f"Downloaded file is too small or doesn't exist for {quality}")
            return None
        
        download_time = time.time() - download_start
        file_size = os.path.getsize(download_path)
        avg_speed = file_size / download_time if download_time > 0 else 0
        
        logger.info(f"Download complete: {quality} - {format_size(file_size)} in {download_time:.1f}s ({format_speed(avg_speed)})")
        
        async def _upload_progress(current, total):
            if not progress:
                return
            pct = int(current * 100 / total) if total else 0
            bar_fill = pct // 5
            bar = "█" * bar_fill + "░" * (20 - bar_fill)
            status = f"[{bar}] {pct}% — {format_size(current)}/{format_size(total)}"
            text = make_upload_status_text(
                anime_title, episode_number, quality, audio_type,
                total, channel_format, extra_status=status,
            )
            await progress.update(text, parse_mode='html')
        
        thumb = await _get_anime_thumb_path(client, anime_title)
        
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=full_caption,
            thumb_path=thumb,
            max_retries=3,
            progress_callback=_upload_progress,
        )
        
        try:
            os.remove(download_path)
        except:
            pass
        
        if dump_msg_id:
            logger.info(f"Successfully uploaded {quality} version: msg_id={dump_msg_id}")
            return dump_msg_id
        else:
            logger.error(f"Upload failed for {quality}")
            return None
            
    except Exception as e:
        logger.error(f"Error in _download_and_upload_single_quality for {quality}: {e}")
        try:
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
        except:
            pass
        return None

async def auto_download_latest_episode():
    global _currently_processing
    
    logger.info("Starting auto download process...")
    
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return False
    
    _currently_processing = True
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    if ADMIN_CHAT_ID:
        progress = ProgressMessage(client, ADMIN_CHAT_ID, "<b>Auto processing started...</b>")
        await progress.send()
    
    try:
        if auto_download_state.last_checked:
            last_check = datetime.fromisoformat(auto_download_state.last_checked)
            time_since_last_check = (datetime.now() - last_check).total_seconds()
            
            cooldown_period = auto_download_state.interval / 2
            if time_since_last_check < cooldown_period:
                logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                return False
        
        if progress:
            await progress.update("<b><blockquote>ᴄʜᴇᴄᴋɪɴɢ ғᴏʀ ɴᴇᴡ ᴇᴘɪsᴏᴅᴇs...</blockquote></b>", parse_mode='html')
        
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            if progress:
                await progress.update("<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇsᴛ ʀᴇʟᴇᴀsᴇ</blockquote></b>", parse_mode='html')
            return False
        
        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)
        
        logger.info(f"Latest airing anime: {anime_title} Episode {episode_number}")
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗖𝗛𝗘𝗖𝗞𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title} \n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Cʜᴇᴄᴋɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title} \n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssᴇᴅ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return True
        
        success = await process_specific_anime(latest_anime, progress)
        
        auto_download_state.last_checked = datetime.now().isoformat()
        return success
        
    except Exception as e:
        logger.error(f"Error in auto_download_latest_episode: {e}")
        if progress:
            await progress.update(f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</blockquote></b>", parse_mode='html')
        return False
    finally:
        _currently_processing = False

async def check_and_process_next_episode(progress=None):
    if anime_queue.pending_queue:
        next_item = anime_queue.pending_queue[0]
        anime_queue.pending_queue.pop(0)
        
        anime_data = {
            'anime_title': next_item.get('title'),
            'episode': next_item.get('episode')
        }
        return await process_specific_anime(anime_data, progress)
    return False

async def process_pending_queue(progress=None):
    while anime_queue.pending_queue:
        await check_and_process_next_episode(progress)
        await asyncio.sleep(5)

async def process_single_episode(anime_title, episode_number, progress=None, from_queue=False):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    anime_data = {
        'anime_title': anime_title,
        'episode': episode_number
    }
    return await process_specific_anime(anime_data, progress)

async def check_for_new_episodes(client):
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    progress = None
    
    if not auto_download_state.enabled:
        return
    
    scheduler_lock = _get_scheduler_lock()
    if scheduler_lock:
        if scheduler_lock.locked():
            logger.info("Scheduler lock held by another task. Skipping this check.")
            return
    
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    async with scheduler_lock if scheduler_lock else asyncio.Lock():
        _currently_processing = True
        logger.info("Checking for new episodes, deferred episodes, and pending queue...")
        
        if anime_queue.pending_queue:
            logger.info(f"Processing {len(anime_queue.pending_queue)} pending episodes first...")
            await process_pending_queue()
        
        # Process deferred episodes first (these are waiting for all qualities to appear)
        deferred_list = deferred_episodes.get_all_deferred()
        if deferred_list:
            logger.info(f"Checking {len(deferred_list)} deferred episodes for quality availability...")
            deferred_episodes.cleanup_expired()
            
            for deferred_item in deferred_list:
                d_title = deferred_item.get('anime_title')
                d_episode = deferred_item.get('episode_number')
                d_anime_data = deferred_item.get('anime_data', {})
                
                if is_episode_processed(d_title, d_episode):
                    deferred_episodes.remove_episode(d_title, d_episode)
                    continue
                
                if episode_tracker.is_posted(d_title, d_episode):
                    deferred_episodes.remove_episode(d_title, d_episode)
                    continue
                
                logger.info(f"Re-checking deferred: {d_title} Ep{d_episode} (check #{deferred_item.get('check_count', 0)})")
                
                if not episode_tracker.try_start_processing(d_title, d_episode):
                    continue
                
                try:
                    success = await process_specific_anime(d_anime_data, progress, _caller_holds_lock=True)
                    if success:
                        logger.info(f"Deferred episode now processed successfully: {d_title} Ep{d_episode}")
                    else:
                        episode_tracker.release_processing(d_title, d_episode, success=False)
                        logger.info(f"Deferred episode still not ready: {d_title} Ep{d_episode}")
                except Exception as e:
                    logger.error(f"Error processing deferred {d_title} Ep{d_episode}: {e}")
                    episode_tracker.release_processing(d_title, d_episode, success=False)
        
        try:
            if auto_download_state.last_checked:
                last_check = datetime.fromisoformat(auto_download_state.last_checked)
                time_since_last_check = (datetime.now() - last_check).total_seconds()
                
                cooldown_period = auto_download_state.interval / 2
                if time_since_last_check < cooldown_period:
                    logger.info(f"Skipping auto check, last check was {time_since_last_check:.1f} seconds ago")
                    return
            
            latest_data = get_latest_releases(page=1)
            if not latest_data or 'data' not in latest_data:
                logger.error("Failed to get latest releases")
                return
            
            unprocessed_anime = []
            for anime_data in latest_data['data']:
                anime_title = anime_data.get('anime_title', 'Unknown Anime')
                episode_number = anime_data.get('episode', 0)
                
                if is_episode_processed(anime_title, episode_number):
                    continue
                
                if episode_tracker.is_posted(anime_title, episode_number):
                    continue
                
                if episode_tracker.is_processing(anime_title, episode_number):
                    continue
                
                # Skip if already deferred (will be handled in deferred check above)
                if deferred_episodes.is_deferred(anime_title, episode_number):
                    continue
                
                unprocessed_anime.append(anime_data)
                logger.info(f"Found unprocessed: {anime_title} Episode {episode_number}")
            
            if not unprocessed_anime:
                logger.info("No new unprocessed anime found.")
                auto_download_state.last_checked = datetime.now().isoformat()
                return
            
            logger.info(f"Found {len(unprocessed_anime)} unprocessed anime to process sequentially")
            
            if ADMIN_CHAT_ID:
                progress = ProgressMessage(client, ADMIN_CHAT_ID, f"<b><blockquote>ғᴏᴜɴᴅ {len(unprocessed_anime)} ɴᴇᴡ ᴀɴɪᴍᴇ ᴛᴏ ᴘʀᴏᴄᴇss...</blockquote></b>", parse_mode='html')
                await progress.send()
            
            processed_count = 0
            failed_count = 0
            skipped_count = 0
            
            for idx, anime_data in enumerate(unprocessed_anime):
                anime_title = anime_data.get('anime_title', 'Unknown Anime')
                episode_number = anime_data.get('episode', 0)
                
                if not episode_tracker.try_start_processing(anime_title, episode_number):
                    logger.info(f"Skipping {anime_title} Ep{episode_number}: could not acquire processing lock")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing anime {idx + 1}/{len(unprocessed_anime)}: {anime_title} Episode {episode_number}")
                
                if progress:
                    await progress.update(
                        f"<b><blockquote>✦ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                        f"・ Pʀᴏɢʀᴇss: {idx + 1}/{len(unprocessed_anime)}\n"
                        f"・ Sᴛᴀᴛᴜs: Pʀᴏᴄᴇssɪɴɢ</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                        parse_mode='html'
                    )
                
                try:
                    success = await process_specific_anime(anime_data, progress, _caller_holds_lock=True)
                    
                    if success:
                        processed_count += 1
                        logger.info(f"Successfully processed: {anime_title} Episode {episode_number}")
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to process: {anime_title} Episode {episode_number}")
                        episode_tracker.release_processing(anime_title, episode_number, success=False)
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing {anime_title} Episode {episode_number}: {e}")
                    episode_tracker.release_processing(anime_title, episode_number, success=False)
                    continue
            
            auto_download_state.last_checked = datetime.now().isoformat()
            
            deferred_count = len(deferred_episodes.get_all_deferred())
            
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Pʀᴏᴄᴇssᴇᴅ: {processed_count}\n"
                    f"・ Dᴇғᴇʀʀᴇᴅ: {deferred_count}\n"
                    f"・ Fᴀɪʟᴇᴅ: {failed_count}\n"
                    f"・ Sᴋɪᴘᴘᴇᴅ: {skipped_count}\n"
                    f"・ Tᴏᴛᴀʟ: {len(unprocessed_anime)}</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            
            logger.info(f"Batch processing complete: {processed_count} processed, {failed_count} failed, {skipped_count} skipped, {deferred_count} deferred")
            
        except Exception as e:
            logger.error(f"Error checking for new episodes: {str(e)}")
            if progress:
                await progress.update(
                    f"<b><blockquote>ᴇʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ ᴀɴɪᴍᴇ:</b> {str(e)}</blockquote>",
                    parse_mode='html'
                )
        finally:
            _currently_processing = False

async def process_specific_anime(anime_data: dict, progress=None, _caller_holds_lock: bool = False) -> bool:
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    anime_title = anime_data.get('anime_title', 'Unknown Anime')
    episode_number = anime_data.get('episode', 0)
    
    # Check DB first - if already processed with all qualities, skip immediately
    if is_episode_processed(anime_title, episode_number):
        logger.info(f"Episode {episode_number} of {anime_title} already fully processed in DB. Skipping.")
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗘𝗗 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Aʟʀᴇᴀᴅʏ ɪɴ DB</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        return True
    
    # Only acquire the processing lock if caller hasn't already done so
    if not _caller_holds_lock:
        if _currently_processing:
            logger.info(f"Already processing another anime, skipping {anime_title} Ep{episode_number}")
            return False
        _currently_processing = True
    
    try:
        logger.info(f"Starting processing: {anime_title} Episode {episode_number}")
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            return False
        
        target_episode = None
        for ep in episodes:
            try:
                if int(ep['episode']) == episode_number:
                    target_episode = ep
                    break
            except (ValueError, TypeError):
                continue
        
        if not target_episode:
            target_episode = find_closest_episode(episodes, episode_number)
            if target_episode:
                episode_number = int(target_episode['episode'])
            else:
                logger.error(f"No episodes found for {anime_title}")
                return False
        
        episode_session = target_episode['session']
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗦𝗧𝗥𝗘𝗔𝗠𝗦 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ URLs...</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
        if not stream_links:
            logger.error(f"No stream links found for {anime_title} Episode {episode_number}")
            # Defer this episode - stream links not available yet
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=[],
                missing_qualities=list(quality_settings.enabled_qualities),
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Rᴇᴀsᴏɴ: Nᴏ sᴛʀᴇᴀᴍ ʟɪɴᴋs ʏᴇᴛ\n"
                    f"・ Sᴛᴀᴛᴜs: Wɪʟʟ ʀᴇᴛʀʏ ʟᴀᴛᴇʀ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        enabled_qualities = quality_settings.enabled_qualities
        preferred_audio = "jpn"
        
        quality_mapping = get_quality_streams(stream_links, enabled_qualities, preferred_audio)
        available_qualities = [q for q, s in quality_mapping.items() if s is not None]
        missing_qualities = [q for q in enabled_qualities if q not in available_qualities]
        
        audio_type = detect_audio_type(stream_links)
        
        logger.info(f"Quality mapping result - Available: {available_qualities}, Missing: {missing_qualities}")
        
        if not available_qualities:
            logger.error(f"No suitable qualities found for {anime_title} Episode {episode_number}")
            # Defer - no qualities available at all
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=[],
                missing_qualities=missing_qualities,
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Rᴇᴀsᴏɴ: Nᴏ ǫᴜᴀʟɪᴛɪᴇs ᴀᴠᴀɪʟᴀʙʟᴇ\n"
                    f"・ Sᴛᴀᴛᴜs: Wɪʟʟ ʀᴇᴛʀʏ ʟᴀᴛᴇʀ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        # KEY LOGIC: If ANY selected quality is missing, DO NOT process the episode.
        # Defer it and wait until ALL selected qualities have embed URLs available.
        if missing_qualities:
            logger.info(f"NOT processing {anime_title} Ep{episode_number}: missing qualities {missing_qualities}. "
                       f"Available: {available_qualities}. Will retry later when all qualities are available.")
            deferred_episodes.defer_episode(
                anime_title, episode_number,
                available_qualities=available_qualities,
                missing_qualities=missing_qualities,
                anime_data=anime_data
            )
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗗𝗘𝗙𝗘𝗥𝗥𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Aᴠᴀɪʟᴀʙʟᴇ: {', '.join(available_qualities)}\n"
                    f"・ Mɪssɪɴɢ: {', '.join(missing_qualities)}\n"
                    f"・ Sᴛᴀᴛᴜs: Wᴀɪᴛɪɴɢ ғᴏʀ ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
        
        # All qualities are available! Remove from deferred list if it was there
        if deferred_episodes.is_deferred(anime_title, episode_number):
            deferred_episodes.remove_episode(anime_title, episode_number)
            logger.info(f"All qualities now available for {anime_title} Ep{episode_number}, removed from deferred list")
        
        if progress:
            await progress.update(
                f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Aᴠᴀɪʟᴀʙʟᴇ: {', '.join(available_qualities)}\n"
                f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
        
        sorted_qualities = sorted(available_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality_idx, quality in enumerate(sorted_qualities):
            try:
                logger.info(f"Downloading {anime_title} Episode {episode_number} {quality} ({quality_idx+1}/{len(sorted_qualities)})")
                
                stream_info = quality_mapping[quality]
                if not stream_info:
                    continue
                
                if progress:
                    await progress.update(
                        f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                        f"・ Qᴜᴀʟɪᴛʏ: {quality} ({quality_idx+1}/{len(sorted_qualities)})\n"
                        f"・ Aᴜᴅɪᴏ: {audio_type}\n"
                        f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ M3U8...</blockquote>\n"
                        f"──────────────────\n"
                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                        parse_mode='html'
                    )
                
                dump_msg_id = await _download_and_upload_single_quality(
                    anime_title, episode_number, quality, stream_info,
                    audio_type, progress, channel_format
                )
                
                if dump_msg_id:
                    if quality not in quality_files:
                        quality_files[quality] = []
                    quality_files[quality].append(dump_msg_id)
                    update_processed_qualities(anime_title, episode_number, quality)
                    downloaded_qualities.append(quality)
                    episode_tracker.mark_quality_uploaded(anime_title, episode_number, quality, dump_msg_id)
                    logger.info(f"Successfully processed {quality}")
                else:
                    logger.error(f"Failed to process {quality}, will retry once")
                
            except Exception as e:
                logger.error(f"Error processing quality {quality}: {e}")
                continue
        
        # Retry failed qualities once
        failed_qualities = [q for q in sorted_qualities if q not in downloaded_qualities and quality_mapping.get(q)]
        if failed_qualities:
            logger.info(f"Retrying {len(failed_qualities)} failed qualities: {failed_qualities}")
            await asyncio.sleep(5)
            
            for quality in failed_qualities:
                try:
                    stream_info = quality_mapping[quality]
                    if not stream_info:
                        continue
                    
                    logger.info(f"Retrying {anime_title} Episode {episode_number} {quality}")
                    
                    dump_msg_id = await _download_and_upload_single_quality(
                        anime_title, episode_number, quality, stream_info,
                        audio_type, progress, channel_format
                    )
                    
                    if dump_msg_id:
                        if quality not in quality_files:
                            quality_files[quality] = []
                        quality_files[quality].append(dump_msg_id)
                        update_processed_qualities(anime_title, episode_number, quality)
                        downloaded_qualities.append(quality)
                        episode_tracker.mark_quality_uploaded(anime_title, episode_number, quality, dump_msg_id)
                        logger.info(f"Retry successful for {quality}")
                    else:
                        logger.error(f"Retry also failed for {quality}")
                except Exception as e:
                    logger.error(f"Error retrying quality {quality}: {e}")
                    continue
        
        if quality_files:
            episode_tracker.mark_completed(anime_title, episode_number)
            
            max_post_retries = 3
            post_created = False
            for retry in range(max_post_retries):
                try:
                    anilist_info = await get_anime_info(anime_title)
                    await post_anime_with_buttons(
                        client, anime_title, anilist_info,
                        episode_number, audio_type, quality_files
                    )
                    post_created = True
                    logger.info(f"Successfully posted banner for {anime_title} Episode {episode_number}")
                    break
                except FloodWaitError as e:
                    logger.warning(f"Flood wait during post (attempt {retry+1}/{max_post_retries}): {e.seconds}s")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logger.error(f"Error posting banner (attempt {retry+1}/{max_post_retries}): {e}")
                    if retry < max_post_retries - 1:
                        await asyncio.sleep(5)
            
            # Mark as processed in DB with all downloaded qualities IMMEDIATELY
            # This ensures even if post fails, the episode won't be reprocessed
            mark_episode_processed(anime_title, episode_number, downloaded_qualities)
            logger.info(f"Marked {anime_title} Ep{episode_number} as processed in DB with qualities: {downloaded_qualities}")
            
            if post_created:
                episode_tracker.mark_posted(anime_title, episode_number)
            
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Qᴜᴀʟɪᴛɪᴇs: {', '.join(downloaded_qualities)}\n"
                    f"・ Sᴛᴀᴛᴜs: {'Pᴏsᴛᴇᴅ ✓' if post_created else 'Uᴘʟᴏᴀᴅᴇᴅ (ᴘᴏsᴛ ғᴀɪʟᴇᴅ)'}</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            
            return True
        else:
            logger.error(f"No qualities uploaded successfully for {anime_title} Ep{episode_number}")
            if progress:
                await progress.update(
                    f"<b><blockquote>✦ 𝗙𝗔𝗜𝗟𝗘𝗗 ✦</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                    f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                    f"・ Sᴛᴀᴛᴜs: Aʟʟ ǫᴜᴀʟɪᴛɪᴇs ғᴀɪʟᴇᴅ</blockquote>\n"
                    f"──────────────────\n"
                    f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                    parse_mode='html'
                )
            return False
            
    except Exception as e:
        logger.error(f"Error in process_specific_anime: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if not _caller_holds_lock:
            _currently_processing = False

async def process_all_qualities(client):
    global _currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    if _currently_processing:
        logger.info("Already processing an episode. Skipping auto check.")
        return
    
    logger.info("Processing latest airing anime with all qualities...")
    
    try:
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            return

        latest_anime = latest_data['data'][0]
        anime_title = latest_anime.get('anime_title', 'Unknown Anime')
        episode_number = latest_anime.get('episode', 0)

        if is_episode_processed(anime_title, episode_number):
            logger.info(f"Episode {episode_number} of {anime_title} already processed. Skipping.")
            return
        
        progress = None
        if ADMIN_CHAT_ID:
            progress = ProgressMessage(client, ADMIN_CHAT_ID, f"<b><blockquote>ᴘʀᴏᴄᴇssɪɴɢ ʟᴀᴛᴇsᴛ ᴀɪʀɪɴɢ ᴀɴɪᴍᴇ ᴡɪᴛʜ ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs...</blockquote></b>", parse_mode='html')
            await progress.send()
        
        success = await process_specific_anime(latest_anime, progress)
        
        if success:
            logger.info("Successfully processed latest episode with all qualities")
        else:
            logger.error("Failed to process latest episode with all qualities")
    except Exception as e:
        logger.error(f"Error processing latest airing anime: {str(e)}")

async def process_daily_requests(client):
    global _currently_processing
    
    from core.database import (
        get_all_pending_requests, mark_request_processed, 
        get_processed_request_results, add_processed_request_result
    )
    
    logger.info("Processing daily requests...")
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    _currently_processing = True
    logger.info("Request processing started - auto-processing PAUSED")
    
    try:
        pending_requests = await get_all_pending_requests()
        
        if not pending_requests:
            logger.info("No pending requests to process")
            return
        
        logger.info(f"Found {len(pending_requests)} pending requests to process")
        
        for idx, request in enumerate(pending_requests, 1):
            try:
                request_text = request.get('text')
                request_id = request.get('_id')
                user_id = request.get('user_id')
                
                logger.info(f"Processing request {idx}/{len(pending_requests)}: {request_text}")
                
                progress = None
                if ADMIN_CHAT_ID:
                    progress = ProgressMessage(client, ADMIN_CHAT_ID, 
                        f"<b><blockquote>ᴘʀᴏᴄᴇssɪɴɢ ʀᴇǫᴜᴇsᴛ ({idx}/{len(pending_requests)})...</blockquote></b>",
                        parse_mode='html'
                    )
                    await progress.send()
                
                search_results = await search_anime(request_text)
                
                if not search_results:
                    logger.warning(f"No results found for request: {request_text}")
                    if progress:
                        await progress.update(
                            f"<b><blockquote>ɴᴏ ʀᴇsᴜʟᴛs ғᴏᴜɴᴅ ғᴏʀ: {request_text}</blockquote></b>",
                            parse_mode='html'
                        )
                    mark_request_processed(request_id)
                    continue
                
                processed_results = await get_processed_request_results(request_text)
                
                remaining_results = []
                for result in search_results:
                    anime_title = result.get('title', result.get('anime_title'))
                    if anime_title not in processed_results:
                        remaining_results.append(result)
                
                if not remaining_results:
                    logger.info(f"All search results for '{request_text}' have been processed")
                    mark_request_processed(request_id)
                    continue
                
                processed_any = False
                for result_idx, anime_result in enumerate(remaining_results[:1], 1):
                    try:
                        anime_title = anime_result.get('title', anime_result.get('anime_title'))
                        anime_session = anime_result.get('session')
                        
                        logger.info(f"Processing result: {anime_title}")
                        
                        episodes = await get_all_episodes(anime_session)
                        if not episodes:
                            logger.warning(f"No episodes found for {anime_title}")
                            continue
                        
                        total_episodes = len(episodes)
                        logger.info(f"Found {total_episodes} episodes for {anime_title}")
                        
                        anime_info_anilist = await get_anime_info(anime_title)
                        
                        enabled_qualities = quality_settings.enabled_qualities
                        sorted_qualities = sorted(enabled_qualities, key=lambda x: int(x[:-1]))
                        
                        all_quality_files = {q: [] for q in sorted_qualities}
                        
                        first_ep_streams = get_stream_links(anime_session, episodes[0].get('session'))
                        audio_type = detect_audio_type(first_ep_streams) if first_ep_streams else "Sub"
                        
                        thumb = await _get_anime_thumb_path(client, anime_title)
                        
                        for ep_idx, episode in enumerate(episodes):
                            episode_number = int(episode.get('episode', 0))
                            episode_session = episode.get('session')
                            
                            try:
                                if progress:
                                    await progress.update(
                                        f"<b><blockquote>✦ 𝗥𝗘𝗤𝗨𝗘𝗦𝗧 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚 ✦</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                                        f"・ Eᴘɪsᴏᴅᴇ: {episode_number} ({ep_idx+1}/{total_episodes})\n"
                                        f"・ Sᴛᴀᴛᴜs: Fᴇᴛᴄʜɪɴɢ sᴛʀᴇᴀᴍs...</blockquote>\n"
                                        f"──────────────────\n"
                                        f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                                        parse_mode='html'
                                    )
                                
                                ep_stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
                                if not ep_stream_links:
                                    logger.warning(f"No streams for Episode {episode_number}, skipping")
                                    continue
                                
                                ep_quality_mapping = get_quality_streams(ep_stream_links, sorted_qualities, "jpn")
                                
                                for quality in sorted_qualities:
                                    stream_info = ep_quality_mapping.get(quality)
                                    if not stream_info:
                                        continue
                                    
                                    kwik_url = stream_info['url']
                                    m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
                                    if not m3u8_data:
                                        continue
                                    
                                    base_name = format_filename(anime_title, episode_number, quality, audio_type)
                                    main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                                    full_caption = f"**{base_name} {main_channel_username}.mkv**"
                                    filename = sanitize_filename(f"{base_name}.mkv")
                                    download_path = os.path.join(DOWNLOAD_DIR, filename)
                                    
                                    dl_success = await download_m3u8(m3u8_data['m3u8_url'], m3u8_data['headers'], download_path)
                                    
                                    if dl_success and os.path.exists(download_path) and os.path.getsize(download_path) > 1000:
                                        dump_msg_id = await robust_upload_file(
                                            file_path=download_path,
                                            caption=full_caption,
                                            thumb_path=thumb,
                                            max_retries=3
                                        )
                                        
                                        if dump_msg_id:
                                            all_quality_files[quality].append(dump_msg_id)
                                            logger.info(f"Uploaded Episode {episode_number} [{quality}] - msg_id: {dump_msg_id}")
                                        
                                        try:
                                            os.remove(download_path)
                                        except:
                                            pass
                                    else:
                                        try:
                                            if os.path.exists(download_path):
                                                os.remove(download_path)
                                        except:
                                            pass
                                
                            except Exception as e:
                                logger.error(f"Error processing Episode {episode_number}: {e}")
                            
                            await asyncio.sleep(2)
                        
                        final_quality_files = {q: ids for q, ids in all_quality_files.items() if ids}
                        
                        if final_quality_files:
                            logger.info(f"Creating final channel post for {anime_title}")
                            
                            await post_anime_batch_with_buttons(
                                client, anime_title, anime_info_anilist, final_quality_files, total_episodes, audio_type
                            )
                            
                            await add_processed_request_result(request_text, anime_title)
                            processed_any = True
                            logger.info(f"Successfully processed ALL {total_episodes} episodes of '{anime_title}'")
                        else:
                            logger.warning(f"No files uploaded for {anime_title}")
                        
                    except Exception as e:
                        logger.error(f"Error processing result: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                
                if processed_any:
                    mark_request_processed(request_id)
                    
            except Exception as e:
                logger.error(f"Error processing request {idx}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("Daily request processing completed")
        
    except Exception as e:
        logger.error(f"Error in process_daily_requests: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        _currently_processing = False
        logger.info("Request processing finished - auto-processing RESUMED")

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

def convert_ist_to_utc(ist_time_str: str) -> str:
    try:
        ist_time = datetime.strptime(ist_time_str, "%H:%M")
        ist_datetime = datetime.now(IST).replace(
            hour=ist_time.hour, minute=ist_time.minute, second=0, microsecond=0
        )
        utc_datetime = ist_datetime.astimezone(UTC)
        return utc_datetime.strftime("%H:%M")
    except Exception as e:
        logger.error(f"Error converting IST to UTC: {e}")
        return "00:00"

def get_current_ist_time() -> str:
    return datetime.now(IST).strftime("%H:%M:%S")

def get_current_utc_time() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")

def setup_scheduler(client):
    def schedule_check():
        asyncio.create_task(check_for_new_episodes(client))
    
    def schedule_queue_check():
        asyncio.create_task(process_pending_queue())
    
    def schedule_daily_requests():
        asyncio.create_task(process_daily_requests(client))
        logger.info(f"Triggered daily request processing at {get_current_utc_time()} UTC / {get_current_ist_time()} IST")
    
    async def setup_daily_request_scheduler():
        from core.database import get_request_process_time
        
        try:
            ist_time_str = await get_request_process_time()
            
            if ist_time_str and ist_time_str != "00:00":
                utc_time_str = convert_ist_to_utc(ist_time_str)
                
                schedule.clear(_request_time_job_tag)
                schedule.every().day.at(utc_time_str).do(schedule_daily_requests).tag(_request_time_job_tag)
                
                logger.info(f"Daily request processing scheduled at {ist_time_str} IST ({utc_time_str} UTC)")
            else:
                logger.info("No daily request processing time configured")
        except Exception as e:
            logger.error(f"Error setting up daily request scheduler: {e}")
    
    def reschedule():
        for job in schedule.get_jobs():
            if _request_time_job_tag not in job.tags:
                schedule.cancel_job(job)
        
        interval = auto_download_state.interval
        schedule.every(interval).seconds.do(schedule_check)
        logger.info(f"Scheduler started with interval: {interval}s")
    
    reschedule()
    
    asyncio.create_task(setup_daily_request_scheduler())
    
    orig_setter = auto_download_state.__class__.interval.fset
    def interval_setter(self, seconds):
        orig_setter(self, seconds)
        reschedule()
    
    auto_download_state.__class__.interval = auto_download_state.__class__.interval.setter(interval_setter)
    
    async def scheduler_loop():
        while True:
            schedule.run_pending()
            await asyncio.sleep(1)
    
    asyncio.create_task(scheduler_loop())

async def reschedule_daily_requests(ist_time_str: str):
    try:
        utc_time_str = convert_ist_to_utc(ist_time_str)
        
        schedule.clear(_request_time_job_tag)
        
        def schedule_daily_requests_job():
            from core.client import client
            asyncio.create_task(process_daily_requests(client))
            logger.info(f"Triggered daily request processing at {get_current_utc_time()} UTC / {get_current_ist_time()} IST")
        
        schedule.every().day.at(utc_time_str).do(schedule_daily_requests_job).tag(_request_time_job_tag)
        
        logger.info(f"Rescheduled daily request processing to {ist_time_str} IST ({utc_time_str} UTC)")
        return True
    except Exception as e:
        logger.error(f"Error rescheduling daily requests: {e}")
        return False

