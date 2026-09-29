from __future__ import annotations
import logging
import subprocess
import asyncio
import time

from telethon import TelegramClient

from core.config import (
    API_ID, API_HASH, BOT_TOKEN, SESSION_FILE,
    FFMPEG_PATH
)

logger = logging.getLogger(__name__)

client = TelegramClient(str(SESSION_FILE), API_ID, API_HASH)

currently_processing = False
processing_lock = asyncio.Lock()

# Telegram upload policy: use the single persistent Telethon client.
# Starting a second bot client for every upload can repeatedly trigger
# ImportBotAuthorization/FLOOD_WAIT. Pyrogram remains optional for compatibility
# with the existing requirements, but is intentionally not started here.
PYROFORK_AVAILABLE = False
pyro_client = None

# Serialize Telegram media/API operations that can be expensive.
telegram_operation_lock = asyncio.Lock()
_flood_wait_until = 0.0

async def wait_for_global_flood_wait():
    """Wait if another Telegram operation has put the process into flood-wait."""
    global _flood_wait_until
    while True:
        remaining = _flood_wait_until - time.time()
        if remaining <= 0:
            return
        logger.warning("Global Telegram flood wait active: %.0fs remaining", remaining)
        await asyncio.sleep(min(remaining, 30))

def set_global_flood_wait(seconds: int):
    global _flood_wait_until
    _flood_wait_until = max(_flood_wait_until, time.time() + int(seconds) + 5)

FFMPEG_AVAILABLE = False
try:
    subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
    FFMPEG_AVAILABLE = True
    logger.info("FFmpeg is available")
except (subprocess.CalledProcessError, FileNotFoundError):
    logger.warning("FFmpeg is not available. Some features may not work.")

def install_ffmpeg():
    try:
        subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
        return True
    except:
        try:
            logger.info("Attempting to install FFmpeg with apt-get...")
            subprocess.run(["apt-get", "update"], check=True)
            subprocess.run(["apt-get", "install", "-y", "ffmpeg"], check=True)
            subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error(f"Failed to install FFmpeg with apt-get: {e}")
            try:
                logger.info("Attempting to install FFmpeg with yum...")
                subprocess.run(["yum", "install", "-y", "ffmpeg"], check=True)
                subprocess.run([FFMPEG_PATH, "-version"], check=True, capture_output=True)
                return True
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                logger.error(f"Failed to install FFmpeg with yum: {e}")
                return False

if not FFMPEG_AVAILABLE and not install_ffmpeg():
    logger.error("FFmpeg is not available. Video conversion will be skipped.")
else:
    FFMPEG_AVAILABLE = True

def install_ytdlp():
    try:
        subprocess.run(["pip", "install", "yt-dlp"], check=True)
        return True
    except:
        return False

try:
    import yt_dlp
except ImportError:
    if install_ytdlp():
        import yt_dlp

