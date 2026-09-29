from __future__ import annotations
import os
import re
import time
import base64
import asyncio
import logging
from html import escape
from datetime import datetime, timedelta

import aiohttp
from bs4 import BeautifulSoup
from telethon import events, types
from telethon.tl import functions
from telethon.tl.custom import Button
from telethon.tl.types import PeerUser
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import WebpageMediaEmptyError

from core.config import *
from core.client import *
from core.state import *
from core.utils import *
from core.anime_api import (
    search_anime, get_episode_list, get_all_episodes, get_latest_releases,
    get_stream_links, extract_m3u8_from_kwik, download_m3u8,
    get_quality_streams, detect_audio_type, get_anime_info,
    find_closest_episode, map_resolution_to_quality_tier
)
from core.download import fast_upload_file, robust_upload_file, rename_video_with_ffmpeg
from core.scheduler import *
from core.database import (
    add_anime_channel, remove_anime_channel, get_anime_channel, get_all_anime_channels, add_request,
    get_all_pending_requests, get_pending_request_count, get_user_pending_requests, delete_request,
    get_max_requests_setting, set_max_requests_setting,
    get_request_process_time, set_request_process_time,
    get_request_group_chat, set_request_group_chat,
)

logger = logging.getLogger(__name__)

DOWNLOAD_DIR = BASE_DIR / "anime_downloads"

currently_processing = False

async def delete_message_after(message, seconds):
    await asyncio.sleep(seconds)
    try:
        await client.delete_messages(message.chat_id, [message.id])
        logger.info(f"Deleted message {message.id} from chat {message.chat_id}")
    except Exception as e:
        logger.error(f"Failed to delete message: {e}")

async def download_and_upload_quality(anime_title, episode_number, quality, stream_info, 
                                       audio_type, event, progress, channel_format):
    try:
        kwik_url = stream_info['url']
        resolution = stream_info['resolution']
        
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
        
        from core.dl_progress import FFmpegProgressReporter
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
        
        from core.dl_progress import make_upload_status_text
        
        async def _upload_progress(current, total):
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
        caption = full_caption
        
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=caption,
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
        logger.error(f"Error in download_and_upload_quality for {quality}: {e}")
        try:
            if 'download_path' in locals() and os.path.exists(download_path):
                os.remove(download_path)
        except:
            pass
        return None

async def download_anime_by_index(event, index: int, force_redownload: bool = False):
    global currently_processing
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    logger.info(f"Downloading anime at index {index} from latest airing list...")
    
    if currently_processing:
        await safe_respond(event, "<b><blockquote>ᴀʟʀᴇᴀᴅʏ ᴘʀᴏᴄᴇssɪɴɢ ᴀɴᴏᴛʜᴇʀ ᴀɴɪᴍᴇ. ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ.</b></blockquote>", parse_mode='html')
        return False
    
    currently_processing = True
    try:
        progress = ProgressMessage(client, event.chat_id, f"<b><blockquote>ᴀᴅᴅɪɴɢ ᴛᴀsᴋ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴀɴɪᴍᴇ ᴀᴛ ɪɴᴅᴇx {index}...</b></blockquote>", parse_mode='html')
        if not await progress.send():
            await safe_respond(event, "<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɪɴɪᴛɪᴀʟɪᴢᴇ ᴘʀᴏɢʀᴇss ᴛʀᴀᴄᴋɪɴɢ</b></blockquote>", parse_mode='html')
            return False
        
        await progress.update("<b><blockquote>ғᴇᴛᴄʜɪɴɢ ʟᴀᴛᴇsᴛ ᴀɴɪᴍᴇ ʟɪsᴛ...</b></blockquote>", parse_mode='html')
        latest_data = get_latest_releases(page=1)
        if not latest_data or 'data' not in latest_data:
            logger.error("Failed to get latest releases")
            await progress.update("<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇsᴛ ʀᴇʟᴇᴀsᴇs</b></blockquote>", parse_mode='html')
            return False
        
        if index < 1 or index > len(latest_data['data']):
            logger.error(f"Invalid index: {index}")
            await progress.update(f"<b><blockquote>ɪɴᴠᴀʟɪᴅ ɪɴᴅᴇx: {index}. ᴍᴜsᴛ ʙᴇ 1-{len(latest_data['data'])}</b></blockquote>", parse_mode='html')
            return False
        
        anime_data = latest_data['data'][index - 1]
        anime_title = anime_data.get('anime_title', 'Unknown Anime')
        episode_number = anime_data.get('episode', 0)
        
        logger.info(f"Selected anime: {anime_title} Episode {episode_number}")
        await progress.update(
            f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗗𝗘𝗧𝗔𝗜𝗟𝗦 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Sᴛᴀᴛᴜs: Fᴇᴛᴄʜɪɴɢ ᴇᴘɪsᴏᴅᴇ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        search_results = await search_anime(anime_title)
        if not search_results:
            logger.error(f"Anime not found: {anime_title}")
            await progress.update(f"<b><blockquote>ᴀɴɪᴍᴇ ɴᴏᴛ ғᴏᴜɴᴅ: {anime_title}</b></blockquote>", parse_mode='html')
            return False
        
        anime_info = search_results[0]
        anime_session = anime_info['session']
        
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            logger.error(f"Failed to get episode list for {anime_title}")
            await progress.update(f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ɢᴇᴛ ᴇᴘɪsᴏᴅᴇ ʟɪsᴛ ғᴏʀ {anime_title}</b></blockquote>", parse_mode='html')
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
                await progress.update(f"<b><blockquote>ɴᴏ ᴇᴘɪsᴏᴅᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title}</b></blockquote>", parse_mode='html')
                return False
        
        episode_session = target_episode['session']
        
        await progress.update(
            f"<b><blockquote>✦ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗦𝗧𝗥𝗘𝗔𝗠𝗦 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ ᴜʀʟs...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
        if not stream_links:
            logger.error(f"No stream links found for {anime_title} Episode {episode_number}")
            await progress.update(f"<b><blockquote>ɴᴏ sᴛʀᴇᴀᴍ ʟɪɴᴋs ғᴏᴜɴᴅ ғᴏʀ {anime_title} Eᴘ {episode_number}</b></blockquote>", parse_mode='html')
            return False
        
        audio_type = detect_audio_type(stream_links)
        preferred_audio = "jpn"
        
        enabled_qualities = quality_settings.enabled_qualities
        quality_mapping = get_quality_streams(stream_links, enabled_qualities, preferred_audio)
        available_qualities = [q for q, s in quality_mapping.items() if s is not None]
        
        if not available_qualities:
            logger.error(f"No suitable qualities found for {anime_title} Episode {episode_number}")
            await progress.update(
                f"<b><blockquote>ɴᴏ sᴜɪᴛᴀʙʟᴇ ǫᴜᴀʟɪᴛɪᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title} Eᴘ {episode_number}</b></blockquote>",
                parse_mode='html'
            )
            return False
        
        logger.info(f"Available qualities: {available_qualities}")
        sorted_qualities = sorted(available_qualities, key=lambda x: int(x[:-1]))
        
        downloaded_qualities = []
        quality_files = {}
        
        for quality in sorted_qualities:
            stream_info = quality_mapping[quality]
            
            dump_msg_id = await download_and_upload_quality(
                anime_title, episode_number, quality, stream_info,
                audio_type, event, progress, channel_format
            )
            
            if dump_msg_id:
                if quality not in quality_files:
                    quality_files[quality] = []
                quality_files[quality].append(dump_msg_id)
                update_processed_qualities(anime_title, episode_number, quality)
                downloaded_qualities.append(quality)
                logger.info(f"Successfully processed {quality}")
            else:
                logger.error(f"Failed to process {quality}")
        
        if quality_files:
            anilist_info = await get_anime_info(anime_title)
            if anilist_info:
                await post_anime_with_buttons(client, anime_title, anilist_info, episode_number, audio_type, quality_files)
        
        if downloaded_qualities:
            await progress.update(
                f"<b><blockquote>sᴜᴄᴄᴇssғᴜʟʟʏ ᴘʀᴏᴄᴇssᴇᴅ:</blockquote>\n"
                f"<blockquote>ᴀɴɪᴍᴇ: {anime_title}\n"
                f"ᴇᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ: {', '.join(downloaded_qualities)}</b></blockquote>\n",
                parse_mode='html'
            )
            return True
        else:
            await progress.update(
                f"<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ:</blockquote>\n"
                f"<blockquote>ᴀɴɪᴍᴇ: {anime_title}\n"
                f"ᴇᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs ғᴀɪʟᴇᴅ</b></blockquote>",
                parse_mode='html'
            )
            return False
    
    except Exception as e:
        logger.error(f"Error in download_anime_by_index: {e}")
        await safe_respond(event, f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</b></blockquote>", parse_mode='html')
        return False
    finally:
        currently_processing = False

async def download_episode(event, anime_title, anime_session, episode_number, 
                           episode_session, selected_quality_info):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    try:
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {selected_quality_info.get('text', 'Unknown')}\n"
            f"・ Sᴛᴀᴛᴜs: Exᴛʀᴀᴄᴛɪɴɢ sᴛʀᴇᴀᴍ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        kwik_url = selected_quality_info['url']
        resolution = selected_quality_info['resolution']
        audio = selected_quality_info.get('audio', 'jpn')
        quality = f"{resolution}p"
        audio_type = "Dub" if audio == "eng" else "Sub"
        
        m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
        if not m3u8_data:
            await safe_edit(event, "<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ ᴇxᴛʀᴀᴄᴛ sᴛʀᴇᴀᴍ URL.</blockquote></b>", parse_mode='html')
            return
        
        m3u8_url = m3u8_data['m3u8_url']
        m3u8_headers = m3u8_data['headers']
        
        base_name = format_filename(anime_title, episode_number, quality, audio_type)
        main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
        full_caption = f"**{base_name} {main_channel_username}.mkv**"
        filename = sanitize_filename(full_caption)
        download_path = os.path.join(DOWNLOAD_DIR, filename)
        
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
            f"・ Sᴛᴀᴛᴜs: Dᴏᴡɴʟᴏᴀᴅɪɴɢ ᴠɪᴀ M3U8...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        download_start = time.time()
        success = await download_m3u8(m3u8_url, m3u8_headers, download_path)
        
        if not success or not os.path.exists(download_path) or os.path.getsize(download_path) < 1000:
            await safe_edit(event, "<b><blockquote>ᴅᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ.</blockquote></b>", parse_mode='html')
            return
        
        download_time = time.time() - download_start
        file_size = os.path.getsize(download_path)
        
        await safe_edit(event,
            f"<b><blockquote>✦ 𝗨𝗣𝗟𝗢𝗔𝗗𝗜𝗡𝗚 ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
            f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
            f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
            f"・ Sɪᴢᴇ: {format_size(file_size)}\n"
            f"・ Sᴛᴀᴛᴜs: Uᴘʟᴏᴀᴅɪɴɢ...</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
            parse_mode='html'
        )
        
        thumb = await _get_anime_thumb_path(client, anime_title)
        dump_msg_id = await robust_upload_file(
            file_path=download_path,
            caption=full_caption,
            thumb_path=thumb,
            max_retries=3
        )
        
        try:
            os.remove(download_path)
        except:
            pass
        
        if dump_msg_id:
            await safe_edit(event,
                f"<b><blockquote>✦ 𝗖𝗢𝗠𝗣𝗟𝗘𝗧𝗘 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Eᴘɪsᴏᴅᴇ: {episode_number}\n"
                f"・ Qᴜᴀʟɪᴛʏ: {quality} ({audio_type})\n"
                f"・ Sɪᴢᴇ: {format_size(file_size)}\n"
                f"・ Tɪᴍᴇ: {download_time:.1f}s</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
            update_processed_qualities(anime_title, episode_number, quality)
        else:
            await safe_edit(event, "<b><blockquote>ᴜᴘʟᴏᴀᴅ ғᴀɪʟᴇᴅ.</blockquote></b>", parse_mode='html')
    
    except Exception as e:
        logger.error(f"Error in download_episode: {e}")
        await safe_edit(event, f"<b><blockquote>ᴇʀʀᴏʀ: {str(e)}</blockquote></b>", parse_mode='html')

async def download_anime_batch(event, anime_session, anime_title):
    channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
    
    try:
        episodes = await get_all_episodes(anime_session)
        if not episodes:
            await safe_edit(event, f"<b><blockquote>ɴᴏ ᴇᴘɪsᴏᴅᴇs ғᴏᴜɴᴅ ғᴏʀ {anime_title}</blockquote></b>", parse_mode='html')
            return False
        
        total = len(episodes)
        success_count = 0
        
        for idx, ep in enumerate(episodes, 1):
            episode_number = ep['episode']
            episode_session = ep['session']
            
            await safe_edit(event,
                f"<b><blockquote>✦ 𝗕𝗔𝗧𝗖𝗛 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 ✦</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>・ Aɴɪᴍᴇ: {anime_title}\n"
                f"・ Pʀᴏɢʀᴇss: {idx}/{total}\n"
                f"・ Cᴜʀʀᴇɴᴛ: Eᴘɪsᴏᴅᴇ {episode_number}</blockquote>\n"
                f"──────────────────\n"
                f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>",
                parse_mode='html'
            )
            
            stream_links = await asyncio.to_thread(get_stream_links, anime_session, episode_session)
            if not stream_links:
                logger.warning(f"No streams for Episode {episode_number}, skipping")
                continue
            
            audio_type = detect_audio_type(stream_links)
            enabled_qualities = quality_settings.enabled_qualities
            quality_mapping = get_quality_streams(stream_links, enabled_qualities, "jpn")
            
            for quality, stream_info in quality_mapping.items():
                if stream_info is None:
                    continue
                
                kwik_url = stream_info['url']
                m3u8_data = await asyncio.to_thread(extract_m3u8_from_kwik, kwik_url)
                if not m3u8_data:
                    continue
                
                base_name = format_filename(anime_title, episode_number, quality, audio_type)
                main_channel_username = CHANNEL_USERNAME if CHANNEL_USERNAME else BOT_USERNAME
                full_caption = f"**{base_name} {main_channel_username}.mkv**"
                filename = sanitize_filename(full_caption)
                download_path = os.path.join(DOWNLOAD_DIR, filename)
                
                dl_success = await download_m3u8(m3u8_data['m3u8_url'], m3u8_data['headers'], download_path)
                
                if dl_success and os.path.exists(download_path) and os.path.getsize(download_path) > 1000:
                    thumb = await _get_anime_thumb_path(client, anime_title)
                    dump_msg_id = await robust_upload_file(download_path, full_caption, thumb)
                    try:
                        os.remove(download_path)
                    except:
                        pass
                    if dump_msg_id:
                        success_count += 1
            
            await asyncio.sleep(2)
        
        return success_count > 0
    
    except Exception as e:
        logger.error(f"Error in batch download: {e}")
        return False

def register_handlers():

    @client.on(events.NewMessage(pattern=r'^/start(?:\s+(.*))?$'))
    async def start_handler(event):
        user_id = event.sender_id
        chnl_name = CHANNEL_NAME
        chnl_user = CHANNEL_USERNAME.lstrip("@")
        user = await event.get_sender()
        mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    
        param = event.pattern_match.group(1)

        if param:
            try:
            
                base64_string = param
                string = await decode(base64_string)
                argument = string.split("-")
            
                if len(argument) == 3:
                    try:
                        start = int(int(argument[1]) / abs(DUMP_CHANNEL_ID))
                        end = int(int(argument[2]) / abs(DUMP_CHANNEL_ID))
                    except (ValueError, ZeroDivisionError):
                        await event.respond("Invalid link format.")
                        return
                    
                    if start <= end:
                        ids = list(range(start, end + 1))
                    else:
                        ids = []
                        i = start
                        while i >= end:
                            ids.append(i)
                            i -= 1
                
                elif len(argument) == 2:
                    try:
                        ids = [int(int(argument[1]) / abs(DUMP_CHANNEL_ID))]
                    except (ValueError, ZeroDivisionError):
                        await event.respond("Invalid link format.")
                        return
                else:
                    await event.respond("Invalid link format.")
                    return

                dump_channel = (
                    bot_settings.get("dump_channel_id")
                    or bot_settings.get("dump_channel_username")
                )
                if not dump_channel:
                    await event.respond("Dump channel not configured.")
                    return
    
                try:
                    processing_msg = await event.respond("<b><blockquote>Pʀᴏᴄᴇssɪɴɢ...</b></blockquote>", parse_mode='html')
                    
                    try:
                        messages = await event.client.get_messages(dump_channel, ids=ids)
                    except Exception as e:
                        logger.error(f"Error fetching messages: {e}")
                        await event.respond("Something went wrong while fetching files.")
                        return
                    
                    if not isinstance(messages, list):
                        messages = [messages]
    
                    delete_timer = bot_settings.get("file_delete_timer", 600)
                    minutes = delete_timer // 60

                    track_msgs = []
                    file_count = 0
                    
                    for msg in messages:
                        if msg and msg.media:
                            file_count += 1
                            try:
                                sent_msg = await event.client.send_file(
                                    event.chat_id,
                                    file=msg.media,
                                    caption=msg.message,
                                    force_document=False,
                                    link_preview=False
                                )
                                
                                if delete_timer and delete_timer > 0:
                                    track_msgs.append(sent_msg)
                                
                            except Exception as e:
                                logger.error(f"Error sending file: {e}")
                                continue
    
                    try:
                        await processing_msg.delete()
                    except:
                        pass
    
                    if file_count > 0:
                        final_msg = await event.client.send_message(
                            event.chat_id, 
                            f"<blockquote><b>sᴜᴄᴄᴇssғᴜʟʟʏ sᴇɴᴛ {file_count} ғɪʟᴇ(s)!</b></blockquote>\n"
                            f"<blockquote><b>ғɪʟᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ {minutes} ᴍɪɴs. ᴘʟᴇᴀsᴇ sᴀᴠᴇ ᴏʀ ғᴏʀᴡᴀʀᴅ ᴛʜᴇᴍ ʙᴇғᴏʀᴇ ᴛʜᴇʏ ɢᴇᴛ ᴅᴇʟᴇᴛᴇᴅ.</b></blockquote>",
                            parse_mode='html',
                            link_preview=False
                        )
                        
                        if delete_timer and delete_timer > 0:
                            track_msgs.append(final_msg)
                            for sent_msg in track_msgs:
                                asyncio.create_task(delete_message_after(sent_msg, delete_timer))
                    else:
                        await event.respond("No files found for this request.")
                        
                except Exception as e:
                    logger.error(f"Error sending files: {e}")
                    try:
                        await processing_msg.delete()
                    except:
                        pass
    
            except Exception as e:
                logger.error(f"Error in start_with_param: {e}")
                await event.respond("An error occurred while processing your request.")
    
        else:
            try:
                start_pic_path = bot_settings.get("start_pic", None)
                if start_pic_path and os.path.exists(start_pic_path):
                    start_media = start_pic_path
                else:
                    temp_pic_path = os.path.join(THUMBNAIL_DIR, "start_pic_temp.jpg")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(START_PIC_URL) as response:
                            if response.status == 200:
                                with open(temp_pic_path, 'wb') as f:
                                    f.write(await response.read())
                                start_media = temp_pic_path
                            else:
                                raise Exception("Failed to download start picture")

                caption_text=(
                    f"<blockquote><b>🍁 Hᴇʏ, {mention}!</b></blockquote>\n"
                    f"<blockquote><b><i>I'ᴍ ᴀ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ. ɪ ᴄᴀɴ ᴅᴏᴡɴʟᴏᴀᴅ ᴏɴɢᴏɪɴɢ ᴀɴᴅ ғɪɴɪsʜᴇᴅ ᴀɴɪᴍᴇ ғʀᴏᴍ ᴀɴɪᴍᴇᴘᴀʜᴇ.ʀᴜ ᴀɴᴅ ᴜᴘʟᴏᴀᴅ ᴛʜᴏsᴇ ғɪʟᴇs ᴏɴ ʏᴏᴜʀ ᴄʜᴀɴᴇʟ ᴅɪʀᴇᴄᴛʟʏ...</i></b>\n</blockquote>"
                    f"<blockquote><b>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/{chnl_user}'>{chnl_name}</a></blockquote></b>"
                )
                
                if is_admin(event.chat_id):
                    buttons = [
                        [Button.inline("𝗠𝗮𝗻𝗮𝗴𝗲", b"manage_menu"), Button.inline("𝗔𝘂𝘁𝗼 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"auto_settings")],
                        [Button.inline("𝗛𝗲𝗹𝗽", b"show_help")],
                    ]
                else:
                    buttons = [
                        [Button.url("𝗗𝗲𝘃𝗲𝗹𝗼𝗽𝗲𝗿", "https://t.me/KamiKaito"),
                         Button.url("𝗠𝗮𝗶𝗻 𝗖𝗵𝗮𝗻𝗻𝗲𝗹", "https://t.me/GenAnimeOngoing")],
                        [Button.url("𝗕𝗮𝗰𝗸𝘂𝗽 𝗖𝗵𝗮𝗻𝗻𝗲𝗹", "https://t.me/OngoingAnimeBackup")]
                    ]
    
                try:
                    await event.client.send_file(
                        event.chat_id,
                        start_media,
                        caption=caption_text,
                        parse_mode='HTML',
                        buttons=buttons,
                        link_preview=False
                    )
                except Exception as photo_error:
                    logger.error(f"Primary send_file failed: {photo_error}")
                    raise
            except Exception as e:
                logger.error(f"Error sending start message: {e}")
                await safe_respond(event, "Welcome! I'm an anime bot. Type /help for more info.")

    @client.on(events.NewMessage(pattern='/request'))
    async def request_command(event):
        # User-facing request command backed by the existing daily request scheduler.
        parts = event.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await safe_respond(event, "<b><blockquote>ᴜsᴀɢᴇ: /request [anime name]</blockquote></b>", parse_mode='html')
            return
        request_text = parts[1].strip()
        try:
            max_requests = await get_max_requests_setting()
            pending_count = await get_user_pending_requests(event.chat_id)
            if pending_count >= max_requests:
                await safe_respond(event, f"<b><blockquote>ʏᴏᴜ ᴀʟʀᴇᴀᴅʏ ʜᴀᴠᴇ {pending_count} pending requests. Maximum is {max_requests}.</blockquote></b>", parse_mode='html')
                return
            sender = await event.get_sender()
            username = getattr(sender, 'username', None)
            ok = await add_request(event.chat_id, request_text, username)
            if not ok:
                await safe_respond(event, "<b><blockquote>ғᴀɪʟᴇᴅ ᴛᴏ sᴀᴠᴇ ʀᴇǫᴜᴇsᴛ.</blockquote></b>", parse_mode='html')
                return
            process_time = await get_request_process_time()
            await safe_respond(event, f"<b><blockquote>✓ ʀᴇǫᴜᴇsᴛ ᴀᴅᴅᴇᴅ</blockquote><blockquote>{request_text}</blockquote><blockquote>ᴅᴀɪʟʏ ᴘʀᴏᴄᴇssɪɴɢ: {process_time}</blockquote></b>", parse_mode='html')
        except Exception as e:
            logger.error(f"Error in request_command: {e}")
            await safe_respond(event, "<b><blockquote>sᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ.</blockquote></b>", parse_mode='html')

    # ------------------------------------------------------------------
    # Full management interface
    # ------------------------------------------------------------------
    def _setting_map(key):
        value = bot_settings.get(key, {})
        return value if isinstance(value, dict) else {}

    def _save_setting_map(key, value):
        bot_settings.set(key, value)

    async def _manage_home(event, edit=True):
        st = user_states.setdefault(event.chat_id, UserState())
        st._manage_media_step = None
        st._manage_media_anime = None
        st._manage_channel_anime = None
        st._manage_button_anime = None
        text = (
            "<b><blockquote>⚙️ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀</blockquote>"
            "<blockquote>Manage all anime-related configurations here.</blockquote></b>"
        )
        buttons = [
            [Button.inline("✦ 𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹", b"mg_set_channel"), Button.inline("✦ 𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗣𝗼𝘀𝘁𝗲𝗿", b"mg_set_poster")],
            [Button.inline("✦ 𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗧𝗵𝘂𝗺𝗯", b"mg_set_thumb"), Button.inline("✦ 𝗣𝗼𝘀𝘁 𝗙𝗼𝗿𝗺𝗮𝘁𝘀", b"mg_formats")],
            [Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹𝘀", b"mg_channels"), Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗣𝗼𝘀𝘁𝗲𝗿𝘀", b"mg_posters")],
            [Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗧𝗵𝘂𝗺𝗯𝘀", b"mg_thumbs"), Button.inline("✦ 𝗦𝘂𝗯𝘀𝗰𝗿𝗶𝗽𝘁𝗶𝗼𝗻𝘀", b"mg_subscriptions")],
            [Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝘁𝗶𝗰𝗸𝗲𝗿", b"mg_sticker"), Button.inline("✦ 𝗕𝗼𝘁 𝗨𝘀𝗲𝗿𝗻𝗮𝗺𝗲", b"mg_username")],
            [Button.inline("✦ 𝗖𝗼𝗺𝗽𝗹𝗲𝘁𝗲𝗱 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀", b"mg_completed"), Button.inline("✦ 𝗦𝗵𝗼𝘄 𝗤𝘂𝗲𝘂𝗲", b"mg_queue")],
            [Button.inline("✦ 𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻", b"mg_custom_button"), Button.inline("✦ 𝗠𝗮𝗶𝗻 𝗖𝗵𝗮𝗻𝗻𝗲𝗹", b"mg_main_channel")],
            [Button.inline("✦ 𝗕𝗮𝗰𝗸", b"back_to_main")],
        ]
        if edit:
            await safe_edit(event, text, buttons=buttons, parse_mode='html')
        else:
            await safe_respond(event, text, buttons=buttons, parse_mode='html')

    @client.on(events.NewMessage(pattern='/manage'))
    async def manage_command(event):
        if not is_admin(event.chat_id):
            return
        await _manage_home(event, edit=False)

    @client.on(events.CallbackQuery(data=b"manage_menu"))
    async def manage_menu_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        await _manage_home(event, edit=True)

    def _set_manage_state(chat_id, step, **extra):
        st = user_states.setdefault(chat_id, UserState())
        st._manage_media_step = step
        for k, v in extra.items():
            setattr(st, k, v)
        return st

    async def _ask_anime(event, step, prompt, **extra):
        _set_manage_state(event.chat_id, step, **extra)
        await safe_edit(event, prompt, buttons=[[Button.inline("✦ 𝗖𝗮𝗻𝗰𝗲𝗹", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_set_channel"))
    async def mg_set_channel(event):
        if not is_admin(event.chat_id): return
        await _ask_anime(event, 'channel_anime', "<b><blockquote>𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹</blockquote><blockquote>Send the exact anime title.</blockquote></b>")

    @client.on(events.CallbackQuery(data=b"mg_set_poster"))
    async def mg_set_poster(event):
        if not is_admin(event.chat_id): return
        await _ask_anime(event, 'poster_anime', "<b><blockquote>𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗣𝗼𝘀𝘁𝗲𝗿</blockquote><blockquote>Send the exact anime title.</blockquote></b>")

    @client.on(events.CallbackQuery(data=b"mg_set_thumb"))
    async def mg_set_thumb(event):
        if not is_admin(event.chat_id): return
        await _ask_anime(event, 'thumb_anime', "<b><blockquote>𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗧𝗵𝘂𝗺𝗯</blockquote><blockquote>Send the exact anime title.</blockquote></b>")

    @client.on(events.CallbackQuery(data=b"mg_sticker"))
    async def mg_sticker(event):
        if not is_admin(event.chat_id): return
        await _ask_anime(event, 'sticker_anime', "<b><blockquote>𝗦𝗲𝘁 𝗔𝗻𝗶𝗺𝗲 𝗦𝘁𝗶𝗰𝗸𝗲𝗿</blockquote><blockquote>Send the exact anime title.</blockquote></b>")

    @client.on(events.CallbackQuery(data=b"mg_custom_button"))
    async def mg_custom_button(event):
        if not is_admin(event.chat_id): return
        data = _setting_map('anime_custom_buttons')
        lines = ["<b><blockquote>✦ 𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻𝘀 ✦</blockquote>"]
        if data:
            for anime, items in list(data.items())[:20]:
                labels = ', '.join(str(x.get('text','Button')) for x in items if isinstance(x,dict))
                lines.append(f"• <b>{escape(str(anime))}</b> → {escape(labels)}")
        else:
            lines.append('No custom buttons saved.')
        lines.append('</blockquote>')
        await safe_edit(event, '\n'.join(lines), buttons=[[Button.inline("✦ 𝗔𝗱𝗱 𝗕𝘂𝘁𝘁𝗼𝗻", b"mg_add_custom_button")],[Button.inline("✦ 𝗥𝗲𝗺𝗼𝘃𝗲 𝗕𝘂𝘁𝘁𝗼𝗻𝘀", b"mg_remove_custom_button")],[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_add_custom_button"))
    async def mg_add_custom_button(event):
        if not is_admin(event.chat_id): return
        await _ask_anime(event, 'button_anime', "<b><blockquote>𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻</blockquote><blockquote>Send the exact anime title.</blockquote></b>")

    @client.on(events.CallbackQuery(data=b"mg_remove_custom_button"))
    async def mg_remove_custom_button(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'remove_button')
        await safe_edit(event, "<b><blockquote>𝗥𝗲𝗺𝗼𝘃𝗲 𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻𝘀</blockquote><blockquote>Send the exact anime title. All extra buttons for that anime will be removed.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_custom_button")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_main_channel"))
    async def mg_main_channel(event):
        if not is_admin(event.chat_id): return
        from core.config import CHANNEL_ID, CHANNEL_USERNAME
        current = bot_settings.get('main_channel', {}) or {}
        shown = current.get('username') or CHANNEL_USERNAME or CHANNEL_ID or 'Not set'
        _set_manage_state(event.chat_id, 'main_channel')
        await safe_edit(event, f"<b><blockquote>𝗠𝗮𝗶𝗻 𝗖𝗵𝗮𝗻𝗻𝗲𝗹</blockquote><blockquote>Current: {escape(str(shown))}\nSend @username or numeric channel ID.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_username"))
    async def mg_username(event):
        if not is_admin(event.chat_id): return
        from core.config import BOT_USERNAME
        _set_manage_state(event.chat_id, 'bot_username')
        await safe_edit(event, f"<b><blockquote>𝗕𝗼𝘁 𝗨𝘀𝗲𝗿𝗻𝗮𝗺𝗲</blockquote><blockquote>Current: @{escape(str(bot_settings.get('bot_username_override', BOT_USERNAME)).lstrip('@'))}\nSend the username to use in generated links/posts.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_formats"))
    async def mg_formats(event):
        if not is_admin(event.chat_id): return
        fmt = bot_settings.get('post_formats', {}) or {}
        cap = fmt.get('caption_template', 'Default')
        fn = fmt.get('filename_template', 'Default')
        layout = fmt.get('button_layout', 2)
        text = ("<b><blockquote>✦ 𝗣𝗼𝘀𝘁 𝗙𝗼𝗿𝗺𝗮𝘁𝘀 ✦</blockquote>"
                f"<blockquote>Caption: {escape(str(cap))[:500]}\nFilename: {escape(str(fn))[:300]}\nButtons per row: {layout}</blockquote>"
                "<blockquote>Caption variables: {title} {english} {episode} {audio} {genres} {score} {studio} {channel}</blockquote></b>")
        buttons = [
            [Button.inline("✦ 𝗦𝗲𝘁 𝗖𝗮𝗽𝘁𝗶𝗼𝗻", b"mg_format_caption"), Button.inline("✦ 𝗦𝗲𝘁 𝗙𝗶𝗹𝗲𝗻𝗮𝗺𝗲", b"mg_format_filename")],
            [Button.inline("✦ 𝗕𝘂𝘁𝘁𝗼𝗻 𝗟𝗮𝘆𝗼𝘂𝘁", b"mg_format_layout"), Button.inline("✦ 𝗥𝗲𝘀𝗲𝘁", b"mg_format_reset")],
            [Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]
        ]
        await safe_edit(event, text, buttons=buttons, parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_format_caption"))
    async def mg_format_caption(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'format_caption')
        await safe_edit(event, "<b><blockquote>𝗖𝗮𝗽𝘁𝗶𝗼𝗻 𝗙𝗼𝗿𝗺𝗮𝘁</blockquote><blockquote>Send your HTML caption template. Variables: {title} {english} {episode} {audio} {genres} {score} {studio} {channel}</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_formats")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_format_filename"))
    async def mg_format_filename(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'format_filename')
        await safe_edit(event, "<b><blockquote>𝗙𝗶𝗹𝗲𝗻𝗮𝗺𝗲 𝗙𝗼𝗿𝗺𝗮𝘁</blockquote><blockquote>Variables: {title} {episode} {quality} {audio} {season}. Send the filename template without .mkv.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_formats")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_format_layout"))
    async def mg_format_layout(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'format_layout')
        await safe_edit(event, "<b><blockquote>𝗕𝘂𝘁𝘁𝗼𝗻 𝗟𝗮𝘆𝗼𝘂𝘁</blockquote><blockquote>Send 1, 2, or 3 buttons per row.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_formats")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_format_reset"))
    async def mg_format_reset(event):
        if not is_admin(event.chat_id): return
        bot_settings.set('post_formats', {'caption_template': '', 'filename_template': '', 'button_layout': 2})
        await event.answer('Post formats reset')
        await mg_formats(event)

    @client.on(events.CallbackQuery(data=b"mg_subscriptions"))
    async def mg_subscriptions(event):
        if not is_admin(event.chat_id): return
        await safe_edit(event, "<b><blockquote>𝗦𝘂𝗯𝘀𝗰𝗿𝗶𝗽𝘁𝗶𝗼𝗻𝘀</blockquote><blockquote>Dummy for now. Subscription features will be added later.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    async def _show_map(event, key, title, empty='No entries saved.'):
        data = _setting_map(key)
        lines = [f"<b><blockquote>✦ {title} ✦</blockquote>"]
        if data:
            for i, (anime, value) in enumerate(list(data.items())[:30], 1):
                if isinstance(value, dict):
                    value = value.get('username') or value.get('url') or f"{value.get('chat_id')}:{value.get('message_id')}"
                lines.append(f"{i}. <b>{escape(str(anime))}</b> → <code>{escape(str(value))}</code>")
        else:
            lines.append(empty)
        lines.append('</blockquote>')
        await safe_edit(event, '\n'.join(lines), buttons=[[Button.inline("✦ 𝗥𝗲𝗳𝗿𝗲𝘀𝗵", f"mg_{key}_list".encode()), Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_channels"))
    async def mg_channels(event):
        if not is_admin(event.chat_id): return
        mappings = await get_all_anime_channels()
        lines = ["<b><blockquote>✦ 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹𝘀 ✦</blockquote>"]
        for i, item in enumerate(mappings[:30], 1):
            lines.append(f"{i}. <b>{escape(str(item.get('anime_title','Unknown')))}</b> → <code>{escape(str(item.get('channel_username') or item.get('channel_id')))}</code>")
        if not mappings: lines.append('No anime-specific channel assignments.')
        lines.append('</blockquote>')
        await safe_edit(event, '\n'.join(lines), buttons=[[Button.inline("✦ 𝗦𝗲𝘁 / 𝗖𝗵𝗮𝗻𝗴𝗲", b"mg_set_channel")],[Button.inline("✦ 𝗥𝗲𝗺𝗼𝘃𝗲", b"mg_remove_channel")],[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_remove_channel"))
    async def mg_remove_channel(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'remove_channel')
        await safe_edit(event, "<b><blockquote>𝗥𝗲𝗺𝗼𝘃𝗲 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹</blockquote><blockquote>Send the exact anime title.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_channels")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_posters"))
    async def mg_posters(event):
        if not is_admin(event.chat_id): return
        data=_setting_map('anime_posters')
        lines=["<b><blockquote>✦ Posters ✦</blockquote>"]
        for i,(anime,ref) in enumerate(list(data.items())[:30],1):
            shown=ref.get('url') if isinstance(ref,dict) and ref.get('url') else 'Telegram media'
            lines.append(f"{i}. <b>{escape(str(anime))}</b> → <code>{escape(str(shown))}</code>")
        if not data: lines.append('No entries saved.')
        lines.append('</blockquote>')
        await safe_edit(event,'\n'.join(lines),buttons=[[Button.inline("✦ 𝗥𝗲𝗺𝗼𝘃𝗲",b"mg_remove_poster")],[Button.inline("✦ 𝗦𝗲𝘁 / 𝗖𝗵𝗮𝗻𝗴𝗲",b"mg_set_poster")],[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_menu")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_anime_posters_list"))
    async def mg_anime_posters_list(event):
        await mg_posters(event)

    @client.on(events.CallbackQuery(data=b"mg_thumbs"))
    async def mg_thumbs(event):
        if not is_admin(event.chat_id): return
        data=_setting_map('anime_thumbs')
        lines=["<b><blockquote>✦ Thumbs ✦</blockquote>"]
        for i,(anime,ref) in enumerate(list(data.items())[:30],1):
            shown=ref.get('url') if isinstance(ref,dict) and ref.get('url') else 'Telegram media'
            lines.append(f"{i}. <b>{escape(str(anime))}</b> → <code>{escape(str(shown))}</code>")
        if not data: lines.append('No entries saved.')
        lines.append('</blockquote>')
        await safe_edit(event,'\n'.join(lines),buttons=[[Button.inline("✦ 𝗥𝗲𝗺𝗼𝘃𝗲",b"mg_remove_thumb")],[Button.inline("✦ 𝗦𝗲𝘁 / 𝗖𝗵𝗮𝗻𝗴𝗲",b"mg_set_thumb")],[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_menu")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_anime_thumbs_list"))
    async def mg_anime_thumbs_list(event):
        await mg_thumbs(event)

    @client.on(events.CallbackQuery(data=b"mg_anime_stickers_list"))
    async def mg_anime_stickers_list(event):
        await mg_stickers_list(event)

    @client.on(events.CallbackQuery(data=b"mg_anime_custom_buttons_list"))
    async def mg_anime_custom_buttons_list(event):
        await mg_custom_button_list(event)

    @client.on(events.CallbackQuery(data=b"mg_stickers_list"))
    async def mg_stickers_list(event):
        if not is_admin(event.chat_id): return
        await _show_map(event, 'anime_stickers', '𝗔𝗻𝗶𝗺𝗲 𝗦𝘁𝗶𝗰𝗸𝗲𝗿𝘀')

    @client.on(events.CallbackQuery(data=b"mg_completed"))
    async def mg_completed(event):
        if not is_admin(event.chat_id): return
        entries = []
        for data in episode_tracker.episodes.values():
            if data.get('state') in ('completed', 'posted'):
                entries.append(data)
        entries = sorted(entries, key=lambda x: x.get('posted_at') or x.get('completed_at') or '', reverse=True)[:40]
        lines = [f"<b><blockquote>✦ 𝗖𝗼𝗺𝗽𝗹𝗲𝘁𝗲𝗱 𝗘𝗽𝗶𝘀𝗼𝗱𝗲𝘀: {len(entries)} ✦</blockquote>"]
        for item in entries:
            lines.append(f"• {escape(str(item.get('anime_title','Unknown')))} — EP {item.get('episode_number','?')} [{item.get('state')}]" )
        if not entries: lines.append('No completed episodes in the tracker.')
        lines.append('</blockquote>')
        await safe_edit(event, '\n'.join(lines), buttons=[[Button.inline("✦ 𝗥𝗲𝗳𝗿𝗲𝘀𝗵", b"mg_completed"), Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_queue"))
    async def mg_queue(event):
        if not is_admin(event.chat_id): return
        pending = list(anime_queue.pending_queue)
        processing = list(anime_queue.processing_queue)
        lines = [f"<b><blockquote>✦ 𝗦𝗵𝗼𝘄 𝗤𝘂𝗲𝘂𝗲 ✦</blockquote>", f"<blockquote>Pending: {len(pending)}\nProcessing: {len(processing)}"]
        for item in pending[:20]: lines.append(f"• {escape(str(item.get('title','Unknown')))} — EP {item.get('episode','?')}")
        lines.append('</blockquote></b>')
        await safe_edit(event, '\n'.join(lines), buttons=[[Button.inline("✦ 𝗥𝗲𝗳𝗿𝗲𝘀𝗵", b"mg_queue"), Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_custom_button_list"))
    async def mg_custom_button_list(event):
        if not is_admin(event.chat_id): return
        await _show_map(event, 'anime_custom_buttons', '𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻𝘀')

    @client.on(events.CallbackQuery(data=b"mg_set_custom_button"))
    async def mg_set_custom_button(event):
        if not is_admin(event.chat_id): return
        await mg_custom_button(event)

    @client.on(events.CallbackQuery(data=b"mg_custom_button_manage"))
    async def mg_custom_button_manage(event):
        await mg_custom_button(event)

    @client.on(events.CallbackQuery(data=b"mg_sticker_list"))
    async def mg_sticker_list(event):
        await mg_stickers_list(event)

    @client.on(events.CallbackQuery(data=b"mg_main_channel_show"))
    async def mg_main_channel_show(event):
        await mg_main_channel(event)

    @client.on(events.CallbackQuery(data=b"mg_remove_poster"))
    async def mg_remove_poster(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'remove_poster')
        await safe_edit(event, "<b><blockquote>Send anime title to remove its poster.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_posters")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_remove_thumb"))
    async def mg_remove_thumb(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'remove_thumb')
        await safe_edit(event, "<b><blockquote>Send anime title to remove its thumb.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"mg_thumbs")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_remove_sticker"))
    async def mg_remove_sticker(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id, 'remove_sticker')
        await safe_edit(event, "<b><blockquote>Send anime title to remove its sticker.</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"mg_username_show"))
    async def mg_username_show(event):
        await mg_username(event)

    @client.on(events.CallbackQuery(data=b"mg_formats_show"))
    async def mg_formats_show(event):
        await mg_formats(event)

    # Backwards-compatible old management callbacks
    @client.on(events.CallbackQuery(data=b"manage_channels"))
    async def manage_channels_callback_compat(event):
        await mg_channels(event)

    @client.on(events.CallbackQuery(data=b"manage_channel_add"))
    async def manage_channel_add_callback_compat(event):
        await mg_set_channel(event)

    @client.on(events.CallbackQuery(data=b"manage_channel_remove"))
    async def manage_channel_remove_callback_compat(event):
        await mg_remove_channel(event)

    @client.on(events.CallbackQuery(data=b"manage_timer"))
    async def manage_timer_callback_compat(event):
        if not is_admin(event.chat_id): return
        current = bot_settings.get("file_delete_timer", 600)
        _set_manage_state(event.chat_id, 'timer')
        await safe_edit(event, f"<b><blockquote>𝗙𝗶𝗹𝗲 𝗗𝗲𝗹𝗲𝘁𝗲 𝗧𝗶𝗺𝗲𝗿</blockquote><blockquote>Current: {current}s\nSend seconds (minimum 60).</blockquote></b>", buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"manage_requests"))
    async def manage_requests_callback(event):
        if not is_admin(event.chat_id): return
        max_req=await get_max_requests_setting(); process_time=await get_request_process_time(); group=await get_request_group_chat()
        text=f"<b><blockquote>✦ 𝗥𝗲𝗾𝘂𝗲𝘀𝘁 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀 ✦</blockquote><blockquote>Max/user: {max_req}\nProcess time: {escape(str(process_time))}\nGroup: {escape(str(group or 'Not set'))}</blockquote></b>"
        await safe_edit(event,text,buttons=[[Button.inline("✦ 𝗦𝗲𝘁 𝗠𝗮𝘅",b"request_set_max"),Button.inline("✦ 𝗦𝗲𝘁 𝗧𝗶𝗺𝗲",b"request_set_time")],[Button.inline("✦ 𝗦𝗲𝘁 𝗚𝗿𝗼𝘂𝗽",b"request_set_group"),Button.inline("✦ 𝗣𝗲𝗻𝗱𝗶𝗻𝗴",b"manage_pending")],[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_menu")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"request_set_max"))
    async def request_set_max_callback(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id,'request_max')
        await safe_edit(event,"<b><blockquote>Send maximum pending requests per user.</blockquote></b>",buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_requests")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"request_set_time"))
    async def request_set_time_callback(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id,'request_time')
        await safe_edit(event,"<b><blockquote>Send daily process time as HH:MM.</blockquote></b>",buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_requests")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"request_set_group"))
    async def request_set_group_callback(event):
        if not is_admin(event.chat_id): return
        _set_manage_state(event.chat_id,'request_group')
        await safe_edit(event,"<b><blockquote>Send @username or numeric group/channel ID.</blockquote></b>",buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_requests")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"manage_pending"))
    async def manage_pending_callback(event):
        if not is_admin(event.chat_id): return
        pending=await get_all_pending_requests()
        lines=[f"<b><blockquote>✦ 𝗣𝗲𝗻𝗱𝗶𝗻𝗴 𝗥𝗲𝗾𝘂𝗲𝘀𝘁𝘀: {len(pending)} ✦</blockquote>"]
        for i,r in enumerate(pending[:25],1): lines.append(f"{i}. {escape(str(r.get('text','Unknown')))} — <code>{r.get('user_id','?')}</code>")
        if not pending: lines.append('No pending requests.')
        lines.append('</blockquote>')
        await safe_edit(event,'\n'.join(lines),buttons=[[Button.inline("✦ 𝗥𝗲𝗳𝗿𝗲𝘀𝗵",b"manage_pending"),Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_requests")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"manage_admins"))
    async def manage_admins_callback(event):
        if not is_admin(event.chat_id): return
        await safe_edit(event,"<b><blockquote>✦ 𝗔𝗱𝗺𝗶𝗻 𝗠𝗮𝗻𝗮𝗴𝗲𝗺𝗲𝗻𝘁 ✦</blockquote><blockquote>Use /add_admin USER_ID or /remove_admin USER_ID. Only the owner can change admins.</blockquote></b>",buttons=[[Button.inline("✦ 𝗕𝗮𝗰𝗸",b"manage_menu")]],parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_settings"))
    async def auto_settings_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        channel_format = (CHANNEL_USERNAME or BOT_USERNAME).lstrip('@')
        enabled = auto_download_state.enabled
        interval = auto_download_state.interval
        last_checked = auto_download_state.last_checked
        status_text = (
            "<blockquote><b>✦ 𝗔𝗨𝗧𝗢 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦: ✦</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>・ Sᴛᴀᴛᴜs: {'Eɴᴀʙʟᴇᴅ' if enabled else 'Dɪsᴀʙʟᴇᴅ'}\n"
            f"・ Iɴᴛᴇʀᴠᴀʟ: {interval}s\n"
            f"・ Lᴀsᴛ Cʜᴇᴄᴋᴇᴅ: {last_checked or 'Nᴇᴠᴇʀ'}</blockquote>\n"
            f"──────────────────\n"
            f"<blockquote>≡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ: <a href='t.me/{channel_format}'>{CHANNEL_NAME}</a></blockquote></b>"
        )
        if enabled:
            btn1 = Button.inline("𝗗𝗶𝘀𝗮𝗯𝗹𝗲", b"auto_disable")
        else:
            btn1 = Button.inline("𝗘𝗻𝗮𝗯𝗹𝗲", b"auto_enable")
        buttons = [
            [btn1, Button.inline("𝗖𝗵𝗲𝗰𝗸 𝗡𝗼𝘄", b"auto_check_now")],
            [Button.inline("𝗤𝘂𝗮𝗹𝗶𝘁𝘆 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"quality_settings")],
            [Button.inline("𝗖𝗵𝗮𝗻𝗴𝗲 𝗜𝗻𝘁𝗲𝗿𝘃𝗮𝗹", b"auto_interval"), Button.inline("𝗕𝗮𝗰𝗸", b"back_to_main")]
        ]
        await safe_edit(event, status_text, buttons=buttons, parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_enable"))
    async def auto_enable_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        auto_download_state.enabled = True
        await safe_edit(event, "<b><blockquote>ᴀᴜᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴇɴᴀʙʟᴇᴅ.</b></blockquote>", 
            buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_disable"))
    async def auto_disable_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        auto_download_state.enabled = False
        await safe_edit(event, "<b><blockquote>ᴀᴜᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴅɪsᴀʙʟᴇᴅ.</blockquote></b>", 
            buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_check_now"))
    async def auto_check_now_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        await safe_edit(event, "<b><blockquote>ᴄʜᴇᴄᴋɪɴɢ ғᴏʀ ɴᴇᴡ ᴇᴘɪsᴏᴅᴇs...</blockquote></b>", parse_mode='html')
        asyncio.create_task(check_for_new_episodes(client))
        await asyncio.sleep(10)
        await safe_edit(event, "<b><blockquote>ᴄʜᴇᴄᴋ ɪɴɪᴛɪᴀᴛᴇᴅ.</b></blockquote>", 
            buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]], parse_mode='html')

    @client.on(events.CallbackQuery(data=b"auto_interval"))
    async def auto_interval_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        current_interval = auto_download_state.interval
        await safe_edit(event, 
            f"<b><blockquote>ᴄᴜʀʀᴇɴᴛ ɪɴᴛᴇʀᴠᴀʟ: {current_interval}s\n"
            "sᴇɴᴅ ɴᴇᴡ ɪɴᴛᴇʀᴠᴀʟ (60-86400):</b></blockquote>",
            parse_mode='html', buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]])
        if event.chat_id not in user_states:
            user_states[event.chat_id] = UserState()
        user_states[event.chat_id]._waiting_for_interval = True

    @client.on(events.CallbackQuery(data=b"back_to_main"))
    async def back_to_main_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        user = await event.get_sender()
        mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
        chnl_user = CHANNEL_USERNAME.lstrip("@")
        if is_admin(event.chat_id):
            buttons = [[Button.inline("𝗠𝗮𝗻𝗮𝗴𝗲", b"manage_menu"), Button.inline("𝗔𝘂𝘁𝗼 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"auto_settings")], [Button.inline("𝗛𝗲𝗹𝗽", b"show_help")]]
        else:
            buttons = [[Button.url("𝗗𝗲𝘃𝗲𝗹𝗼𝗽𝗲𝗿", "https://t.me/KamiKaito"), Button.url("𝗠𝗮𝗶𝗻 𝗖𝗵𝗮𝗻𝗻𝗲𝗹", "https://t.me/GenAnimeOngoing")]]
        await safe_edit(event,
            f"<blockquote><b>🍁 Hᴇʏ, {mention}!</b></blockquote>\n"
            f"<blockquote><b><i>I'ᴍ ᴀɴ ᴀᴜᴛᴏ ᴀɴɪᴍᴇ ʙᴏᴛ.</i></b></blockquote>\n"
            f"<blockquote><b>ᴘᴏᴡᴇʀᴇᴅ ʙʏ - <a href='https://t.me/{chnl_user}'>{CHANNEL_NAME}</a></b></blockquote>",
            buttons=buttons, parse_mode='html')

    @client.on(events.CallbackQuery(data=b"quality_settings"))
    async def quality_settings_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return

        enabled_qualities = quality_settings.enabled_qualities
        batch_status = "𝗢𝗡" if quality_settings.batch_mode else "𝗢𝗙𝗙"

        quality_row = []
        for quality in ["360p", "720p", "1080p"]:
            checked = "✓" if quality in enabled_qualities else "✗"
            quality_row.append(
                Button.inline(f"{checked} {quality}", f"toggle_{quality}".encode())
            )

        buttons = [
        quality_row,
            [Button.inline(f"𝗕𝗮𝘁𝗰𝗵 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱: {batch_status}", b"toggle_batch_mode")],
            [Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]
        ]

        await safe_edit(
            event,
            f"<b><blockquote>✦ 𝗤𝗨𝗔𝗟𝗜𝗧𝗬 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦 ✦</blockquote>\n"
            f"<blockquote>Eɴᴀʙʟᴇᴅ: {', '.join(enabled_qualities)}\n"
            f"Bᴀᴛᴄʜ Mᴏᴅᴇ: {batch_status}</blockquote></b>",
            buttons=buttons,
            parse_mode='html'
        )

    @client.on(events.CallbackQuery(data=b"toggle_360p"))
    async def toggle_360p_callback(event):
        if not is_admin(event.chat_id):
            return
        eq = quality_settings.enabled_qualities
        if "360p" in eq: eq.remove("360p")
        else: eq.append("360p")
        quality_settings.enabled_qualities = eq
        await event.answer(f"360p {'enabled' if '360p' in eq else 'disabled'}")

    @client.on(events.CallbackQuery(data=b"toggle_720p"))
    async def toggle_720p_callback(event):
        if not is_admin(event.chat_id):
            return
        eq = quality_settings.enabled_qualities
        if "720p" in eq: eq.remove("720p")
        else: eq.append("720p")
        quality_settings.enabled_qualities = eq
        await event.answer(f"720p {'enabled' if '720p' in eq else 'disabled'}")

    @client.on(events.CallbackQuery(data=b"toggle_1080p"))
    async def toggle_1080p_callback(event):
        if not is_admin(event.chat_id):
            return
        eq = quality_settings.enabled_qualities
        if "1080p" in eq: eq.remove("1080p")
        else: eq.append("1080p")
        quality_settings.enabled_qualities = eq
        await event.answer(f"1080p {'enabled' if '1080p' in eq else 'disabled'}")

    @client.on(events.CallbackQuery(data=b"toggle_batch_mode"))
    async def toggle_batch_mode_callback(event):
        if not is_admin(event.chat_id):
            return

        quality_settings.batch_mode = not quality_settings.batch_mode
        batch_status = "𝗢𝗡" if quality_settings.batch_mode else "𝗢𝗙𝗙"

        await event.answer(f"Batch Download: {batch_status}")

        enabled_qualities = quality_settings.enabled_qualities

        quality_row = []
        for quality in ["360p", "720p", "1080p"]:
            checked = "✓" if quality in enabled_qualities else "✗"
            quality_row.append(
                Button.inline(f"{checked} {quality}", f"toggle_{quality}".encode())
            )

        buttons = [
            quality_row,
            [Button.inline(f"𝗕𝗮𝘁𝗰𝗵 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱: {batch_status}", b"toggle_batch_mode")],
            [Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]
        ]

        await safe_edit(
            event,
            f"<b><blockquote>✦ 𝗤𝗨𝗔𝗟𝗜𝗧𝗬 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦 ✦</blockquote>\n"
            f"<blockquote>Eɴᴀʙʟᴇᴅ: {', '.join(enabled_qualities)}\n"
            f"Bᴀᴛᴄʜ Mᴏᴅᴇ: {batch_status}</blockquote></b>",
            buttons=buttons,
            parse_mode='html'
        )

    @client.on(events.NewMessage)
    async def handle_message(event):
        if event.out:
            return
        if not isinstance(event.peer_id, PeerUser):
            return
        if not is_admin(event.chat_id):
            return
        if event.chat_id not in user_states:
            user_states[event.chat_id] = UserState()
        user_state = user_states[event.chat_id]
        media_step = getattr(user_state, '_manage_media_step', None)
        if media_step:
            text_value = (event.text or '').strip()
            if media_step in ('poster_anime', 'thumb_anime', 'sticker_anime') and not getattr(user_state, '_manage_media_anime', None):
                if not text_value:
                    await safe_respond(event, "<b><blockquote>Send the anime title as text first.</blockquote></b>", parse_mode='html')
                    return
                user_state._manage_media_anime = text_value
                next_prompt = {
                    'poster_anime': "Send the poster photo, or send a direct image URL.",
                    'thumb_anime': "Send the thumbnail photo, or send a direct image URL.",
                    'sticker_anime': "Send the sticker message now."
                }[media_step]
                user_state._manage_media_step = media_step.replace('_anime', '_media')
                await safe_respond(event, f"<b><blockquote>{next_prompt}</blockquote></b>", buttons=[[Button.inline("✦ 𝗖𝗮𝗻𝗰𝗲𝗹", b"manage_menu")]], parse_mode='html')
                return
            # The second stage accepts actual media without text.
            if media_step in ('poster_media', 'thumb_media', 'sticker_media'):
                anime = getattr(user_state, '_manage_media_anime', '')
                key = {'poster_media':'anime_posters', 'thumb_media':'anime_thumbs', 'sticker_media':'anime_stickers'}[media_step]
                store = _setting_map(key)
                if event.message and event.message.media:
                    store[anime] = {'chat_id': event.chat_id, 'message_id': event.message.id}
                elif text_value and media_step != 'sticker_media':
                    store[anime] = {'url': text_value}
                else:
                    await safe_respond(event, "<b><blockquote>Unsupported input. Send the requested media.</blockquote></b>", parse_mode='html')
                    return
                _save_setting_map(key, store)
                user_state._manage_media_step = None
                user_state._manage_media_anime = None
                label = key.replace('anime_', '').title()
                await safe_respond(event, f"<b><blockquote>✓ {label} saved for {escape(anime)}.</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_menu")]], parse_mode='html')
                return
            if media_step == 'channel_anime':
                if not text_value: return
                user_state._manage_channel_anime = text_value
                user_state._manage_media_step = 'channel_value'
                await safe_respond(event, "<b><blockquote>Send @channelusername or numeric channel ID.</blockquote></b>", parse_mode='html')
                return
            if media_step == 'channel_value':
                anime = getattr(user_state, '_manage_channel_anime', '')
                raw = text_value
                username = raw if raw.startswith('@') else None
                try:
                    entity = await event.client.get_entity(raw.lstrip('@') if username else int(raw))
                    channel_id = getattr(entity, 'id', None)
                    if not channel_id: raise ValueError('No channel id')
                    ok = await add_anime_channel(anime, channel_id, username or getattr(entity, 'username', None))
                except Exception as exc:
                    logger.error(f"Could not resolve management channel {raw}: {exc}")
                    await safe_respond(event, "<b><blockquote>Could not resolve that channel. Make sure the Telethon account has access.</blockquote></b>", parse_mode='html')
                    return
                user_state._manage_media_step = None; user_state._manage_channel_anime = None
                await safe_respond(event, f"<b><blockquote>{'✓ Saved' if ok else '✗ Failed'}: {escape(anime)} → {escape(username or str(channel_id))}</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹𝘀", b"mg_channels")]], parse_mode='html')
                return
            if media_step == 'remove_button':
                data=_setting_map('anime_custom_buttons'); ok=text_value in data; data.pop(text_value,None); _save_setting_map('anime_custom_buttons',data); user_state._manage_media_step=None
                await safe_respond(event, f"<b><blockquote>{'✓ Removed' if ok else 'No buttons found'} for {escape(text_value)}.</blockquote></b>", buttons=[[Button.inline("✦ 𝗖𝘂𝘀𝘁𝗼𝗺 𝗕𝘂𝘁𝘁𝗼𝗻𝘀",b"mg_custom_button")]], parse_mode='html'); return
            if media_step in ('remove_channel','remove_poster','remove_thumb','remove_sticker'):
                keymap={'remove_poster':'anime_posters','remove_thumb':'anime_thumbs','remove_sticker':'anime_stickers'}
                if media_step=='remove_channel': ok=await remove_anime_channel(text_value)
                else:
                    key=keymap[media_step]; data=_setting_map(key); ok=text_value in data; data.pop(text_value, None); _save_setting_map(key,data)
                user_state._manage_media_step=None
                await safe_respond(event, f"<b><blockquote>{'✓ Removed' if ok else 'No entry found'}: {escape(text_value)}</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_menu")]], parse_mode='html')
                return
            if media_step == 'button_anime':
                if not text_value: return
                user_state._manage_button_anime=text_value; user_state._manage_media_step='button_value'
                await safe_respond(event, "<b><blockquote>Send button as: BUTTON NAME | https://example.com</blockquote></b>", parse_mode='html')
                return
            if media_step == 'button_value':
                parts=[x.strip() for x in text_value.split('|',1)]
                if len(parts)!=2 or not parts[0] or not parts[1].startswith(('http://','https://','tg://')):
                    await safe_respond(event, "<b><blockquote>Use: BUTTON NAME | https://example.com</blockquote></b>", parse_mode='html'); return
                anime=getattr(user_state,'_manage_button_anime',''); data=_setting_map('anime_custom_buttons'); data.setdefault(anime,[]).append({'text':parts[0][:64],'url':parts[1]}); _save_setting_map('anime_custom_buttons',data)
                user_state._manage_media_step=None; user_state._manage_button_anime=None
                await safe_respond(event, f"<b><blockquote>✓ Custom button saved for {escape(anime)}.</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_menu")]], parse_mode='html'); return
            if media_step == 'main_channel':
                raw=text_value; username=raw if raw.startswith('@') else None
                try:
                    entity=await event.client.get_entity(raw.lstrip('@') if username else int(raw)); cid=getattr(entity,'id',None)
                    if not cid: raise ValueError
                    main={'id':cid,'username':username or getattr(entity,'username',None)}; bot_settings.set('main_channel',main)
                    import core.config as cfg
                    cfg.CHANNEL_ID=cid; cfg.CHANNEL_USERNAME=main.get('username')
                    globals()['CHANNEL_ID']=cid; globals()['CHANNEL_USERNAME']=main.get('username')
                    import core.scheduler as sched
                    sched.CHANNEL_ID=cid; sched.CHANNEL_USERNAME=main.get('username')
                    user_state._manage_media_step=None
                    await safe_respond(event, f"<b><blockquote>✓ Main channel set to {escape(str(main.get('username') or cid))}.</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_menu")]], parse_mode='html')
                except Exception as exc:
                    await safe_respond(event, f"<b><blockquote>Could not resolve channel: {escape(str(exc))}</blockquote></b>", parse_mode='html')
                return
            if media_step == 'bot_username':
                value=text_value.lstrip('@')
                if not value: return
                bot_settings.set('bot_username_override', value)
                import core.config as cfg
                cfg.BOT_USERNAME=value; globals()['BOT_USERNAME']=value
                import core.scheduler as sched; sched.BOT_USERNAME=value
                user_state._manage_media_step=None
                await safe_respond(event, f"<b><blockquote>✓ Bot username setting saved: @{escape(value)}</blockquote></b>", buttons=[[Button.inline("✦ 𝗔𝗻𝗶𝗺𝗲 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_menu")]], parse_mode='html'); return
            if media_step in ('request_max','request_time','request_group'):
                try:
                    if media_step == 'request_max':
                        n=int(text_value);
                        if n<1: raise ValueError
                        await set_max_requests_setting(n); msg=f"Maximum requests set to {n}."
                    elif media_step == 'request_time':
                        datetime.strptime(text_value,'%H:%M'); await set_request_process_time(text_value); msg=f"Process time set to {text_value}."
                    else:
                        username=text_value if text_value.startswith('@') else None
                        ent=await event.client.get_entity(text_value.lstrip('@') if username else int(text_value)); gid=getattr(ent,'id',None)
                        await set_request_group_chat(gid, username or getattr(ent,'username',None)); msg=f"Request group set to {username or gid}."
                    user_state._manage_media_step=None
                    await safe_respond(event,f"<b><blockquote>✓ {escape(msg)}</blockquote></b>",buttons=[[Button.inline("✦ 𝗥𝗲𝗾𝘂𝗲𝘀𝘁𝘀",b"manage_requests")]],parse_mode='html')
                except Exception:
                    await safe_respond(event,"<b><blockquote>Invalid value. Try again.</blockquote></b>",parse_mode='html')
                return
            if media_step == 'timer':
                try:
                    seconds=int(text_value)
                    if seconds < 60: raise ValueError
                    bot_settings.set('file_delete_timer', seconds)
                    user_state._manage_media_step=None
                    await safe_respond(event, f"<b><blockquote>✓ Timer set to {seconds}s.</blockquote></b>", buttons=[[Button.inline("✦ 𝗠𝗮𝗻𝗮𝗴𝗲", b"manage_menu")]], parse_mode='html')
                except Exception:
                    await safe_respond(event, "<b><blockquote>Send a number of seconds, minimum 60.</blockquote></b>", parse_mode='html')
                return
            if media_step in ('format_caption','format_filename','format_layout'):
                fmt=bot_settings.get('post_formats',{}) or {}
                if media_step=='format_caption': fmt['caption_template']=text_value
                elif media_step=='format_filename': fmt['filename_template']=text_value
                else:
                    try: n=int(text_value); assert n in (1,2,3); fmt['button_layout']=n
                    except Exception:
                        await safe_respond(event, "<b><blockquote>Send 1, 2, or 3.</blockquote></b>", parse_mode='html'); return
                bot_settings.set('post_formats',fmt); user_state._manage_media_step=None
                await safe_respond(event, "<b><blockquote>✓ Post format saved.</blockquote></b>", buttons=[[Button.inline("✦ 𝗣𝗼𝘀𝘁 𝗙𝗼𝗿𝗺𝗮𝘁𝘀", b"mg_formats")]], parse_mode='html'); return

        if not event.text:
            return
        if event.text.startswith('/'):
            return
        
        if hasattr(user_state, '_waiting_for_interval') and user_state._waiting_for_interval:
            try:
                interval = int(event.text.strip())
                if 60 <= interval <= 86400:
                    auto_download_state.interval = interval
                    await safe_respond(event, f"<blockquote><b>ɪɴᴛᴇʀᴠᴀʟ sᴇᴛ ᴛᴏ {interval}s.</b></blockquote>", 
                        buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"auto_settings")]], parse_mode='html')
                else:
                    await safe_respond(event, "<b><blockquote>ᴍᴜsᴛ ʙᴇ 60-86400.</blockquote></b>", parse_mode='html')
                user_state._waiting_for_interval = False
                return
            except ValueError:
                await safe_respond(event, "<b><blockquote>ɪɴᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ.</blockquote></b>", parse_mode='html')
                return
        
        # Management text-input states
        if getattr(user_state, '_manage_channel_step', None):
            step = user_state._manage_channel_step
            value = event.text.strip()
            if step == 'anime':
                user_state._manage_channel_anime = value
                user_state._manage_channel_step = 'channel'
                await safe_respond(event,
                    "<b><blockquote>Now send the destination channel username (e.g. @MyChannel) or numeric channel ID.</blockquote>"
                    "<blockquote>The Telethon account must have access to the channel.</blockquote></b>",
                    parse_mode='html')
            elif step == 'channel':
                anime = getattr(user_state, '_manage_channel_anime', '')
                raw = value.strip()
                username = raw if raw.startswith('@') else None
                channel_id = None
                try:
                    if not username:
                        channel_id = int(raw)
                    else:
                        entity = await event.client.get_entity(raw.lstrip('@'))
                        channel_id = getattr(entity, 'id', None)
                except Exception as exc:
                    logger.error(f"Could not resolve management channel {raw}: {exc}")
                    await safe_respond(event, "<b><blockquote>Could not resolve that channel. Use a valid @username or a channel ID accessible to the Telegram account.</blockquote></b>", parse_mode='html')
                    return
                if not channel_id:
                    await safe_respond(event, "<b><blockquote>Channel ID could not be determined.</blockquote></b>", parse_mode='html')
                    return
                ok = await add_anime_channel(anime, channel_id, username)
                user_state._manage_channel_step = None
                user_state._manage_channel_anime = None
                await safe_respond(event, f"<b><blockquote>{'Saved' if ok else 'Failed to save'}:</blockquote><blockquote>{anime} → {username or channel_id}</blockquote></b>", buttons=[[Button.inline("𝗠𝗮𝗻𝗮𝗴𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹𝘀", b"manage_channels")]], parse_mode='html')
            elif step == 'remove':
                ok = await remove_anime_channel(value)
                user_state._manage_channel_step = None
                await safe_respond(event, f"<b><blockquote>{'Removed' if ok else 'No assignment found'}: {value}</blockquote></b>", buttons=[[Button.inline("𝗠𝗮𝗻𝗮𝗴𝗲 𝗖𝗵𝗮𝗻𝗻𝗲𝗹𝘀", b"manage_channels")]], parse_mode='html')
            return

        if getattr(user_state, '_manage_timer', False):
            try:
                seconds = int(event.text.strip())
                if seconds < 60: raise ValueError
                bot_settings.set("file_delete_timer", seconds)
                user_state._manage_timer = False
                await safe_respond(event, f"<b><blockquote>Timer set to {seconds}s ({seconds/60:.1f} min).</blockquote></b>", buttons=[[Button.inline("𝗕𝗮𝗰𝗸", b"manage_menu")]], parse_mode='html')
            except ValueError:
                await safe_respond(event, "<b><blockquote>Send a valid number of seconds (minimum 60).</blockquote></b>", parse_mode='html')
            return

        request_step = getattr(user_state, '_manage_request_step', None)
        if request_step:
            value = event.text.strip()
            try:
                if request_step == 'max':
                    n = int(value)
                    if n < 1: raise ValueError
                    await set_max_requests_setting(n)
                    msg = f"Maximum requests set to {n}."
                elif request_step == 'time':
                    datetime.strptime(value, '%H:%M')
                    await set_request_process_time(value)
                    msg = f"Request process time set to {value}."
                else:
                    group_id = None
                    username = value if value.startswith('@') else None
                    if not username:
                        group_id = int(value)
                    else:
                        entity = await event.client.get_entity(username.lstrip('@'))
                        group_id = getattr(entity, 'id', None)
                    await set_request_group_chat(group_id, username)
                    msg = f"Request group set to {username or group_id}."
                user_state._manage_request_step = None
                await safe_respond(event, f"<b><blockquote>{msg}</blockquote></b>", buttons=[[Button.inline("𝗥𝗲𝗾𝘂𝗲𝘀𝘁 𝗦𝗲𝘁𝘁𝗶𝗻𝗴𝘀", b"manage_requests")]], parse_mode='html')
            except Exception:
                await safe_respond(event, "<b><blockquote>Invalid value. Please try again.</blockquote></b>", parse_mode='html')
            return

        query = event.text.strip()
        if not query:
            return
        
        current_time = time.time()
        if current_time - user_state.last_command_time < 5:
            return
        user_state.last_command_time = current_time
        
        search_msg = await safe_respond(event, f"<blockquote><b>sᴇᴀʀᴄʜɪɴɢ: {query}...</b></blockquote>", parse_mode='html')
        try:
            anime_results = await search_anime(query)
            if not anime_results:
                await safe_edit(search_msg, "<b><blockquote>ᴀɴɪᴍᴇ ɴᴏᴛ ғᴏᴜɴᴅ.</blockquote></b>", parse_mode='html')
                return
        except Exception as e:
            await safe_edit(search_msg, "<b><blockquote>sᴇᴀʀᴄʜ ᴇʀʀᴏʀ.</blockquote></b>", parse_mode='html')
            return
        
        buttons = []
        for i, anime in enumerate(anime_results[:10]):
            buttons.append([Button.inline(
                f"{anime['title']} ({anime['year']}) - {anime['episodes']} eps",
                f"anime_{i}".encode()
            )])
        buttons.append([Button.inline("𝗖𝗮𝗻𝗰𝗲𝗹", b"cancel_search")])
        user_state.anime_results = anime_results
        await safe_respond(event, "<b>Sᴇᴀʀᴄʜ Rᴇsᴜʟᴛs:</b>", buttons=buttons, parse_mode='html')

    @client.on(events.CallbackQuery())
    async def handle_callback(event):
        if not is_admin(event.chat_id):
            await event.answer("ᴀᴅᴍɪɴ ᴏɴʟʏ!", alert=True)
            return
        
        data = event.data.decode('utf-8')
        
        if event.chat_id not in user_states:
            user_states[event.chat_id] = UserState()
        user_state = user_states[event.chat_id]
        
        if data == 'cancel_search':
            await safe_edit(event, "<blockquote><b>ᴄᴀɴᴄᴇʟᴇᴅ.</b></blockquote>", parse_mode='html')
            return
        
        if data.startswith('anime_'):
            if not user_state.anime_results:
                await safe_edit(event, "<blockquote><b>ᴇxᴘɪʀᴇᴅ.</b></blockquote>", parse_mode='html')
                return
            
            anime_index = int(data.split('_')[1])
            if anime_index >= len(user_state.anime_results):
                return
            
            selected_anime = user_state.anime_results[anime_index]
            anime_session = selected_anime['session']
            anime_title = selected_anime['title']
            
            if quality_settings.batch_mode:
                await safe_edit(event, f"<b><blockquote>Bᴀᴛᴄʜ ᴅᴏᴡɴʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ: {anime_title}</blockquote></b>", parse_mode='html')
                await download_anime_batch(event, anime_session, anime_title)
                return
            
            user_state.anime_session = anime_session
            user_state.anime_title = anime_title
            
            await safe_edit(event, f"<b><blockquote>Fᴇᴛᴄʜɪɴɢ ᴇᴘɪsᴏᴅᴇs ғᴏʀ {anime_title}...</blockquote></b>", parse_mode='html')
            
            episode_data = await get_episode_list(anime_session)
            if not episode_data or 'data' not in episode_data:
                await safe_edit(event, "<b><blockquote>ɴᴏ ᴇᴘɪsᴏᴅᴇs ғᴏᴜɴᴅ.</blockquote></b>", parse_mode='html')
                return
            
            episodes = episode_data['data']
            user_state.episodes = episodes
            user_state.current_page = 1
            user_state.total_pages = episode_data.get('last_page', 1)
            
            buttons = []
            for ep in episodes[:10]:
                buttons.append([Button.inline(
                    f"Episode {ep['episode']}",
                    f"eps_{ep['episode']}".encode()
                )])
            
            if len(episodes) > 10 or user_state.total_pages > 1:
                buttons.append([Button.inline("𝗡𝗲𝘅𝘁", b"ep_next")])
            buttons.append([Button.inline("𝗖𝗮𝗻𝗰𝗲𝗹", b"cancel_search")])
            
            await safe_edit(event,
                f"<b><blockquote>{anime_title}</blockquote>\n<blockquote>Sᴇʟᴇᴄᴛ ᴇᴘɪsᴏᴅᴇ:</blockquote></b>",
                buttons=buttons, parse_mode='html')
        
        elif data.startswith('eps_'):
            episode_num = int(data.split('_')[1])
            episodes = user_state.episodes
            
            selected_episode = None
            for ep in episodes:
                if int(ep['episode']) == episode_num:
                    selected_episode = ep
                    break
            
            if not selected_episode:
                await safe_edit(event, "<b><blockquote>ᴇᴘɪsᴏᴅᴇ ɴᴏᴛ ғᴏᴜɴᴅ.</blockquote></b>", parse_mode='html')
                return
            
            anime_session = user_state.anime_session
            anime_title = user_state.anime_title
            episode_session = selected_episode['session']
            
            await safe_edit(event, f"<b><blockquote>Fᴇᴛᴄʜɪɴɢ sᴛʀᴇᴀᴍs ғᴏʀ Eᴘ {episode_num}...</blockquote></b>", parse_mode='html')
            
            stream_links = get_stream_links(anime_session, episode_session)
            if not stream_links:
                await safe_edit(event, "<b><blockquote>ɴᴏ sᴛʀᴇᴀᴍs ғᴏᴜɴᴅ.</blockquote></b>", parse_mode='html')
                return
            
            user_state.stream_links = stream_links
            user_state.episode_number = episode_num
            user_state.episode_session = episode_session
            
            buttons = []
            for i, stream in enumerate(stream_links):
                label = f"{stream['fansub']} · {stream['resolution']}p ({stream['audio'].upper()})"
                buttons.append([Button.inline(label, f"stream_{i}".encode())])
            buttons.append([Button.inline("𝗖𝗮𝗻𝗰𝗲𝗹", b"cancel_search")])
            
            await safe_edit(event,
                f"<b><blockquote>{anime_title} - Eᴘ {episode_num}</blockquote>\n"
                f"<blockquote>Sᴇʟᴇᴄᴛ ǫᴜᴀʟɪᴛʏ:</blockquote></b>",
                buttons=buttons, parse_mode='html')
        
        elif data.startswith('stream_'):
            stream_index = int(data.split('_')[1])
            stream_links = user_state.stream_links
            
            if not stream_links or stream_index >= len(stream_links):
                await safe_edit(event, "<b><blockquote>ɪɴᴠᴀʟɪᴅ sᴇʟᴇᴄᴛɪᴏɴ.</blockquote></b>", parse_mode='html')
                return
            
            selected_stream = stream_links[stream_index]
            anime_title = user_state.anime_title
            anime_session = user_state.anime_session
            episode_number = user_state.episode_number
            episode_session = user_state.episode_session
            
            await download_episode(event, anime_title, anime_session, episode_number, 
                                 episode_session, selected_stream)
        
        elif data in ['ep_prev', 'ep_next']:
            pass

