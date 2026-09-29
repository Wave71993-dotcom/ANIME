from __future__ import annotations
import asyncio
import logging
from datetime import datetime

import aiohttp
from aiohttp import web

from core.config import PORT, BOT_TOKEN, START_PIC_URL
from core.client import client
from core.utils import download_start_pic_if_not_exists, get_fixed_thumbnail
from core.handlers import register_handlers
from core.scheduler import setup_scheduler

logger = logging.getLogger(__name__)
health_logger = logging.getLogger('health_monitor')

_start_time = datetime.now()

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_handler(request):
    uptime = str(datetime.now() - _start_time).split('.')[0]
    return web.json_response({
        "status": "alive",
        "service": "AutoAnime-Bot",
        "uptime": uptime
    })

@routes.get("/health")
async def health_handler(request):
    uptime = str(datetime.now() - _start_time).split('.')[0]
    return web.json_response({
        "status": "healthy",
        "uptime": uptime,
        "version": "3.0.0",
        "platform": "huggingface"
    })

@routes.get("/status")
async def status_handler(request):
    uptime = str(datetime.now() - _start_time).split('.')[0]
    return web.json_response({
        "bot": "running",
        "uptime": uptime,
        "version": "3.0.0",
        "platform": "huggingface"
    })

async def start_web_server():
    app = web.Application()
    app.add_routes(routes)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logger.info(f"Web server running on port {PORT}")
    return runner

async def _health_monitor_loop():
    health_url = f"http://127.0.0.1:{PORT}/health"
    
    await asyncio.sleep(30)
    health_logger.info(f"Health monitor started - pinging {health_url} every 60s")
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        health_logger.debug(f"Health OK at {datetime.now().strftime('%H:%M:%S')}")
                    else:
                        health_logger.warning(f"Health WARN: status={resp.status}")
        except asyncio.CancelledError:
            return
        except Exception as e:
            health_logger.error(f"Health FAIL: {str(e)}")
        
        await asyncio.sleep(60)

async def main():
    # HTTP server is started exactly once. Telegram reconnects never restart it.
    await start_web_server()
    register_handlers()
    asyncio.create_task(_health_monitor_loop())
    scheduler_started = False

    while True:
        try:
            if not client.is_connected():
                await client.start(bot_token=BOT_TOKEN)
            logger.info("𝙇𝙤𝙖𝙙𝙞𝙣𝙜.")
            await asyncio.sleep(1)
            logger.info("𝙇𝙤𝙖𝙙𝙞𝙣𝙜..")
            await asyncio.sleep(1.5)
            logger.info("𝙇𝙤𝙖𝙙𝙞𝙣𝙜...")
            if not scheduler_started:
                setup_scheduler(client)
                scheduler_started = True
            await asyncio.sleep(3)
            logger.info("𝘼𝙪𝙩𝙤𝘼𝙣𝙞𝙢𝙚 𝙞𝙨 𝘼𝙇𝙄𝙑𝙀!")
            download_start_pic_if_not_exists(START_PIC_URL)
            await get_fixed_thumbnail()
            await client.run_until_disconnected()
            break
        except Exception as e:
            seconds = getattr(e, 'seconds', None)
            if seconds is not None:
                wait = max(int(seconds) + 5, 5)
                logger.error(f"Telegram flood wait: waiting {wait}s: {e}")
                await asyncio.sleep(wait)
                continue
            logger.error(f"𝙀𝙧𝙧𝙤𝙧: {e}")
            await asyncio.sleep(15)
            if client.is_connected():
                try: await client.disconnect()
                except Exception: pass

if __name__ == '__main__':
    asyncio.run(main())

