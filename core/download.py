from __future__ import annotations
import os
import asyncio
import logging
import time
from typing import Optional

from telethon.errors import FloodWaitError

from core.config import (
    DUMP_CHANNEL_ID, DUMP_CHANNEL_USERNAME, FFMPEG_PATH, DOWNLOAD_DIR,
    API_ID, API_HASH, BOT_TOKEN
)
from core.client import (client, FFMPEG_AVAILABLE, telegram_operation_lock,
                         wait_for_global_flood_wait, set_global_flood_wait)
from core.utils import format_size

logger = logging.getLogger(__name__)


async def rename_video_with_ffmpeg(input_path: str, output_path: str) -> bool:
    if not FFMPEG_AVAILABLE:
        logger.warning("FFmpeg not available. Skipping video conversion.")
        return False

    try:
        cmd = [
            FFMPEG_PATH,
            '-y',
            '-i', input_path,
            '-c', 'copy',
            output_path
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return True
        else:
            logger.warning(f"FFmpeg remux error: {stderr.decode()[-300:]}")
            return False
    except Exception as e:
        logger.warning(f"Error renaming video with FFmpeg: {e}")
        return False


async def _upload_via_pyrogram(file_path, caption, thumb_path, target_channel,
                                progress_callback, timeout):
    from pyrogram import Client as PyroClient

    pyro = PyroClient(
        "pyro_upload",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        no_updates=True,
    )

    try:
        await asyncio.wait_for(pyro.start(), timeout=30)
        logger.info("Pyrogram upload client started")

        pyro_msg = await asyncio.wait_for(
            pyro.send_document(
                chat_id=target_channel,
                document=file_path,
                caption=caption,
                thumb=thumb_path,
                file_name=os.path.basename(file_path),
                progress=progress_callback,
                force_document=True,
            ),
            timeout=timeout,
        )

        logger.info(f"Fast upload completed using Pyrogram: msg_id={pyro_msg.id}")
        return pyro_msg.id

    finally:
        try:
            await pyro.stop()
        except Exception:
            pass


async def fast_upload_file(file_path: str, caption: str, thumb_path: str = None,
                           progress_callback=None) -> Optional[int]:
    """Upload using the one persistent Telethon client.

    A second Pyrogram bot session is deliberately not created per file. That
    pattern was causing repeated ImportBotAuthorization/FLOOD_WAIT errors.
    Uploads are serialized and a Telegram flood wait is shared process-wide.
    """
    target_channel = DUMP_CHANNEL_ID or DUMP_CHANNEL_USERNAME
    if not target_channel:
        logger.warning("No dump channel configured")
        return None
    if not os.path.exists(file_path):
        logger.error("File does not exist: %s", file_path)
        return None

    file_size = os.path.getsize(file_path)
    if file_size < 1000:
        logger.error("File too small: %s (%s bytes)", file_path, file_size)
        return None

    TELETHON_TIMEOUT = 1800

    # Only one media upload at a time. This also prevents multiple scheduler
    # paths from hitting Telegram simultaneously.
    async with telegram_operation_lock:
        await wait_for_global_flood_wait()

        try:
            logger.info("Uploading with persistent Telethon: %s (%s)",
                        os.path.basename(file_path), format_size(file_size))

            def _telethon_progress(sent, total):
                if progress_callback:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(_safe_progress(progress_callback, sent, total))
                    except Exception:
                        pass

            msg = await asyncio.wait_for(
                client.send_file(
                    target_channel,
                    file_path,
                    caption=caption,
                    thumb=thumb_path,
                    force_document=True,
                    attributes=None,
                    supports_streaming=False,
                    part_size_kb=512,
                    progress_callback=_telethon_progress if progress_callback else None,
                    link_preview=False
                ),
                timeout=TELETHON_TIMEOUT
            )

            logger.info("Upload completed with persistent Telethon: msg_id=%s", msg.id)
            return msg.id

        except FloodWaitError as e:
            set_global_flood_wait(e.seconds)
            logger.warning("Telegram FLOOD_WAIT during upload: %ss. Pausing all Telegram uploads.", e.seconds)
            await wait_for_global_flood_wait()
            return None
        except asyncio.TimeoutError:
            logger.error("Telethon upload timed out after %ss", TELETHON_TIMEOUT)
            return None
        except Exception as e:
            logger.error("Telethon upload failed: %s", e)
            return None


async def _safe_progress(callback, current, total):
    try:
        await callback(current, total)
    except Exception:
        pass


async def robust_upload_file(
    file_path: str,
    caption: str,
    thumb_path: str = None,
    max_retries: int = 3,
    progress_callback=None
) -> Optional[int]:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Upload attempt {attempt}/{max_retries}: {os.path.basename(file_path)}")

            result = await fast_upload_file(
                file_path=file_path,
                caption=caption,
                thumb_path=thumb_path,
                progress_callback=progress_callback
            )

            if result:
                return result

            logger.warning(f"Upload attempt {attempt} returned None")

        except Exception as e:
            last_error = e
            logger.error(f"Upload attempt {attempt} failed: {e}")

        if attempt < max_retries:
            wait_time = 10 * attempt
            logger.info(f"Waiting {wait_time}s before retry...")
            await asyncio.sleep(wait_time)

    logger.error(f"All {max_retries} upload attempts failed. Last error: {last_error}")
    return None
