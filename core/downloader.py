from __future__ import annotations

import os
import re
import time
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

_RE_DURATION = re.compile(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)")
_RE_PROGRESS = re.compile(
    r"size=\s*(?P<size>[\-\d]+(?:\.\d+)?)(?P<size_unit>kB|KiB|MiB|mB|B)?"
    r".*?time=\s*(?P<time>\d+:\d+:\d+\.\d+|N/A)"
    r".*?bitrate=\s*(?P<bitrate>[\-\d.]+\s*\w+/s|N/A)"
    r"(?:.*?speed=\s*(?P<speed>[\d.]+x|N/A))?",
    re.IGNORECASE,
)

def guess_headers(url: str, page_referer: Optional[str] = None) -> str:
    if page_referer:
        parsed_ref = urlparse(page_referer)
        if parsed_ref.scheme and parsed_ref.netloc:
            referer = f"{parsed_ref.scheme}://{parsed_ref.netloc}/"
            origin = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
            return f"Referer: {referer}\r\nOrigin: {origin}\r\n"

    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    referer = f"{origin}/"
    return f"Referer: {referer}\r\nOrigin: {origin}\r\n"

def build_headers_string(
    m3u8_url: str,
    extra_headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
) -> str:
    page_referer: Optional[str] = None
    accept_lang: Optional[str] = None

    if extra_headers:
        for k, v in extra_headers.items():
            kl = k.lower()
            if kl == "referer":
                page_referer = v
            elif kl == "accept-language":
                accept_lang = v

    headers_str = guess_headers(m3u8_url, page_referer=page_referer)

    if cookies:
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        headers_str += f"Cookie: {cookie_str}\r\n"

    headers_str += f"Accept-Language: {accept_lang or 'en-US,en;q=0.9'}\r\n"

    if extra_headers:
        for k, v in extra_headers.items():
            kl = k.lower()
            if kl in ("referer", "origin", "cookie", "user-agent",
                      "accept-language", "host", "content-length"):
                continue
            headers_str += f"{k}: {v}\r\n"

    return headers_str

@dataclass
class DownloadProgress:
    status: str = "starting"
    downloaded_bytes: int = 0
    total_bytes: int = 0
    speed_bps: float = 0.0
    elapsed: float = 0.0
    eta: float = 0.0
    current_time: str = "00:00:00"
    duration: str = "N/A"
    raw: Dict[str, str] = field(default_factory=dict)

def _parse_size_to_bytes(size_str: str, unit: Optional[str]) -> int:
    try:
        value = float(size_str)
    except (TypeError, ValueError):
        return 0
    if value < 0:
        return 0
    unit = (unit or "").lower()
    if unit in ("kb", "kib"):
        return int(value * 1024)
    if unit in ("mb", "mib"):
        return int(value * 1024 * 1024)
    return int(value)

def _hms_to_seconds(hms: str) -> float:
    try:
        h, m, s = hms.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        return 0.0

ProgressCb = Optional[Callable[[DownloadProgress], Awaitable[None]]]

async def download_m3u8(
    m3u8_url: str,
    output_path: str,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    progress_callback: ProgressCb = None,
    progress_interval: float = 3.0,
    timeout: int = 1800,
) -> bool:
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except OSError:
            pass

    headers_str = build_headers_string(m3u8_url, extra_headers=headers, cookies=cookies)

    local_m3u8 = None
    local_key = None
    local_segments_dir = None
    actual_input = m3u8_url

    try:
        import cloudscraper
        from urllib.parse import urljoin
        import re as _re

        req_headers = {}
        if headers:
            req_headers.update(headers)
        req_headers.setdefault("User-Agent", UA)

        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'linux', 'mobile': False}
        )

        m3u8_resp = scraper.get(m3u8_url, headers=req_headers, timeout=30)
        m3u8_resp.raise_for_status()
        m3u8_text = m3u8_resp.text

        local_segments_dir = output_path + "_segments"
        os.makedirs(local_segments_dir, exist_ok=True)

        rewritten_lines = []
        segment_count = 0
        total_segments = sum(1 for line in m3u8_text.split("\n")
                           if line.strip() and not line.strip().startswith("#"))

        for line in m3u8_text.split("\n"):
            stripped = line.strip()

            key_match = _re.search(r'#EXT-X-KEY:METHOD=AES-128,URI="([^"]+)"', stripped)
            if key_match:
                key_url = key_match.group(1)
                if not key_url.startswith("http"):
                    key_url = urljoin(m3u8_url, key_url)

                logger.info("Downloading AES key: %s", key_url[:80])
                key_resp = scraper.get(key_url, headers=req_headers, timeout=15)
                key_resp.raise_for_status()

                local_key = os.path.join(local_segments_dir, "key.bin")
                with open(local_key, "wb") as f:
                    f.write(key_resp.content)

                abs_key = os.path.abspath(local_key)
                new_line = stripped.replace(
                    f'URI="{key_match.group(1)}"',
                    f'URI="file://{abs_key}"'
                )
                rewritten_lines.append(new_line)
                continue

            if stripped and not stripped.startswith("#"):
                seg_url = stripped
                if not seg_url.startswith("http"):
                    seg_url = urljoin(m3u8_url, seg_url)

                segment_count += 1
                seg_filename = os.path.basename(stripped.split("?")[0])
                local_seg = os.path.join(local_segments_dir, seg_filename)

                logger.debug("Downloading segment %d/%d: %s", segment_count, total_segments, seg_filename)
                seg_resp = None
                last_seg_error = None
                for attempt in range(4):
                    try:
                        seg_resp = scraper.get(seg_url, headers=req_headers, timeout=60)
                        if seg_resp.status_code == 429:
                            retry_after = seg_resp.headers.get("Retry-After")
                            try:
                                delay = min(max(float(retry_after), 1.0), 15.0) if retry_after else 2.0 * (attempt + 1)
                            except (TypeError, ValueError):
                                delay = 2.0 * (attempt + 1)
                            logger.warning("HLS segment rate-limited (429), retry %d/4 in %.1fs: %s", attempt + 1, delay, seg_url[:120])
                            time.sleep(delay)
                            continue
                        seg_resp.raise_for_status()
                        break
                    except Exception as exc:
                        last_seg_error = exc
                        if attempt < 3:
                            time.sleep(1.5 * (attempt + 1))
                        else:
                            raise

                if seg_resp is None:
                    raise last_seg_error or RuntimeError("Failed to download HLS segment")

                # Hiyori/Miruro sometimes uses .jpg/.txt names for actual
                # MPEG-TS/fMP4 media. Keep the original playlist URL semantics
                # but use a media-safe local filename so FFmpeg can probe it.
                safe_ext = os.path.splitext(seg_filename)[1].lower()
                if safe_ext not in {".ts", ".m4s", ".mp4", ".aac", ".m4a", ".mp3", ".webvtt", ".vtt"}:
                    local_seg = os.path.join(local_segments_dir, f"seg-{segment_count:06d}.ts")

                with open(local_seg, "wb") as f:
                    f.write(seg_resp.content)

                rewritten_lines.append(os.path.abspath(local_seg))

                if segment_count % 20 == 0 or segment_count == total_segments:
                    logger.info("Downloaded %d/%d segments", segment_count, total_segments)

                if progress_callback and segment_count % 10 == 0:
                    progress = DownloadProgress(
                        status="downloading",
                        downloaded_bytes=segment_count * 200_000,
                        current_time=f"seg {segment_count}/{total_segments}",
                    )
                    try:
                        await progress_callback(progress)
                    except Exception:
                        pass
                continue

            rewritten_lines.append(line)

        local_m3u8 = output_path + ".m3u8"
        with open(local_m3u8, "w") as f:
            f.write("\n".join(rewritten_lines))

        actual_input = local_m3u8
        logger.info("Downloaded %d segments + key → %s", segment_count, local_segments_dir)

    except Exception as e:
        logger.warning("Full HLS local download failed (%s), will try direct FFmpeg anyway", e)
        actual_input = m3u8_url

    is_local = (actual_input != m3u8_url)

    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "info",
    ]

    if not is_local:
        cmd.extend([
            "-user_agent", UA,
            "-headers", headers_str,
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_at_eof", "1",
            "-reconnect_delay_max", "10",
            "-rw_timeout", "30000000",
        ])

    cmd.extend([
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-allowed_extensions", "ALL",
        "-extension_picky", "0",
        "-i", actual_input,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-map", "0:v:0?",
        "-map", "0:a:0?",
        "-err_detect", "ignore_err",
        "-max_muxing_queue_size", "4096",
        output_path,
    ])

    logger.info(
        "FFmpeg HLS download → %s (referer=%s)",
        os.path.basename(output_path),
        headers_str.splitlines()[0] if headers_str else "n/a",
    )

    progress = DownloadProgress(status="starting")
    if progress_callback:
        try:
            await progress_callback(progress)
        except Exception as e:
            logger.debug("progress_callback (start) raised: %s", e)

    started_at = time.time()

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        logger.error("FFmpeg binary not found in PATH")
        progress.status = "failed"
        if progress_callback:
            try:
                await progress_callback(progress)
            except Exception:
                pass
        return False
    except Exception as e:
        logger.error("Failed to spawn FFmpeg: %s", e)
        return False

    duration_seconds: float = 0.0
    last_emit = 0.0
    last_bytes = 0
    last_emit_ts = started_at
    err_tail: list[str] = []
    err_tail_max = 40

    async def _read_stderr():
        nonlocal duration_seconds, last_emit, last_bytes, last_emit_ts, progress

        progress.status = "downloading"

        assert process.stderr is not None
        while True:
            line_bytes = await process.stderr.readline()
            if not line_bytes:
                break

            line = line_bytes.decode("utf-8", errors="ignore").rstrip()
            if not line:
                continue

            err_tail.append(line)
            if len(err_tail) > err_tail_max:
                err_tail.pop(0)

            if duration_seconds == 0.0:
                m_dur = _RE_DURATION.search(line)
                if m_dur:
                    h, mm, ss = m_dur.groups()
                    duration_seconds = int(h) * 3600 + int(mm) * 60 + float(ss)
                    progress.duration = f"{int(h):02d}:{int(mm):02d}:{int(float(ss)):02d}"

            m_prog = _RE_PROGRESS.search(line)
            if not m_prog:
                continue

            size_str = m_prog.group("size")
            size_unit = m_prog.group("size_unit")
            ts_str = m_prog.group("time") or "00:00:00.00"
            speed_str = m_prog.group("speed") or "N/A"

            downloaded = _parse_size_to_bytes(size_str, size_unit)

            now = time.time()
            interval = max(now - last_emit_ts, 1e-3)
            speed_bps = max((downloaded - last_bytes) / interval, 0.0)

            elapsed = now - started_at
            current_seconds = _hms_to_seconds(ts_str)
            eta = 0.0
            if duration_seconds > 0 and current_seconds > 0:
                remain = max(duration_seconds - current_seconds, 0.0)
                speed_mult = 0.0
                if speed_str.endswith("x"):
                    try:
                        speed_mult = float(speed_str[:-1])
                    except ValueError:
                        speed_mult = 0.0
                if speed_mult > 0:
                    eta = remain / speed_mult

            progress.downloaded_bytes = downloaded
            progress.speed_bps = speed_bps
            progress.elapsed = elapsed
            progress.eta = eta
            progress.current_time = ts_str
            progress.raw = {
                "size": size_str,
                "time": ts_str,
                "bitrate": m_prog.group("bitrate") or "N/A",
                "speed": speed_str,
            }

            if progress_callback and (now - last_emit) >= progress_interval:
                last_emit = now
                last_emit_ts = now
                last_bytes = downloaded
                try:
                    await progress_callback(progress)
                except Exception as e:
                    logger.debug("progress_callback raised: %s", e)
            else:
                last_emit_ts = now
                last_bytes = downloaded

    stderr_task = asyncio.create_task(_read_stderr())

    try:
        await asyncio.wait_for(process.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        logger.error("FFmpeg timed out after %ss", timeout)
        try:
            process.kill()
        except Exception:
            pass
        progress.status = "failed"
        if progress_callback:
            try:
                await progress_callback(progress)
            except Exception:
                pass
        try:
            await stderr_task
        except Exception:
            pass
        return False

    try:
        await stderr_task
    except Exception:
        pass

    return_code = process.returncode

    if return_code != 0:
        logger.error(
            "FFmpeg exited with code %s. Tail:\n%s",
            return_code,
            "\n".join(err_tail[-15:]),
        )
        logger.info("Falling back to yt-dlp for %s", os.path.basename(output_path))
        ytdl_ok = await _ytdlp_download(
            m3u8_url=m3u8_url,
            output_path=output_path,
            headers=headers,
            cookies=cookies,
        )
        if ytdl_ok and os.path.exists(output_path) and os.path.getsize(output_path) >= 1000:
            final_size = os.path.getsize(output_path)
            progress.downloaded_bytes = final_size
            progress.total_bytes = final_size
            progress.status = "done"
            progress.elapsed = time.time() - started_at
            if progress_callback:
                try:
                    await progress_callback(progress)
                except Exception:
                    pass
            logger.info(
                "yt-dlp HLS download SUCCESS: %s (%.2f MB in %.1fs)",
                os.path.basename(output_path),
                final_size / (1024 * 1024),
                progress.elapsed,
            )
            return True

        progress.status = "failed"
        if progress_callback:
            try:
                await progress_callback(progress)
            except Exception:
                pass
        return False

    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
        logger.error("FFmpeg returned 0 but output file missing/too small: %s", output_path)
        progress.status = "failed"
        if progress_callback:
            try:
                await progress_callback(progress)
            except Exception:
                pass
        return False

    final_size = os.path.getsize(output_path)
    progress.downloaded_bytes = final_size
    progress.total_bytes = final_size
    progress.status = "done"
    progress.elapsed = time.time() - started_at
    if progress_callback:
        try:
            await progress_callback(progress)
        except Exception:
            pass

    logger.info(
        "FFmpeg HLS download SUCCESS: %s (%.2f MB in %.1fs)",
        os.path.basename(output_path),
        final_size / (1024 * 1024),
        progress.elapsed,
    )

    for tmp in (local_m3u8, local_key):
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    if local_segments_dir and os.path.exists(local_segments_dir):
        import shutil
        try:
            shutil.rmtree(local_segments_dir, ignore_errors=True)
        except Exception:
            pass

    return True

async def _ytdlp_download(
    m3u8_url: str,
    output_path: str,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    timeout: int = 1800,
) -> bool:
    page_referer: Optional[str] = None
    if headers:
        for k, v in headers.items():
            if k.lower() == "referer":
                page_referer = v
                break

    if page_referer:
        parsed_ref = urlparse(page_referer)
        referer_val = f"{parsed_ref.scheme}://{parsed_ref.netloc}/"
        origin_val = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
    else:
        parsed = urlparse(m3u8_url)
        origin_val = f"{parsed.scheme}://{parsed.netloc}"
        referer_val = f"{origin_val}/"

    cmd = [
        "yt-dlp",
        "--no-warnings",
        "--no-part",
        "--no-mtime",
        "--concurrent-fragments", "8",
        "--retries", "10",
        "--fragment-retries", "10",
        "--hls-prefer-native",
        "--fixup", "warn",
        "--user-agent", UA,
        "--add-header", f"Referer:{referer_val}",
        "--add-header", f"Origin:{origin_val}",
        "--add-header", "Accept-Language:en-US,en;q=0.9",
        "-o", output_path,
        m3u8_url,
    ]

    if cookies:
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        cmd.insert(-1, "--add-header")
        cmd.insert(-1, f"Cookie:{cookie_str}")

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        logger.warning("yt-dlp not installed — fallback unavailable. "
                       "Install with: pip install yt-dlp")
        return False
    except Exception as e:
        logger.warning("Failed to spawn yt-dlp: %s", e)
        return False

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.error("yt-dlp timed out after %ss", timeout)
        try:
            process.kill()
        except Exception:
            pass
        return False

    if process.returncode != 0:
        tail = (stderr.decode("utf-8", errors="ignore") or "")[-1500:]
        logger.error("yt-dlp exited %s. Tail:\n%s", process.returncode, tail)
        return False

    return os.path.exists(output_path) and os.path.getsize(output_path) >= 1000

async def download_m3u8_compat(
    m3u8_url: str,
    headers: Optional[Dict[str, str]],
    output_path: str,
    progress_callback: ProgressCb = None,
) -> bool:
    cookies: Optional[Dict[str, str]] = None
    extra: Dict[str, str] = {}

    if isinstance(headers, dict):
        for k, v in headers.items():
            if k == "cookies" and isinstance(v, dict):
                cookies = v
            else:
                extra[k] = v

    return await download_m3u8(
        m3u8_url=m3u8_url,
        output_path=output_path,
        headers=extra or None,
        cookies=cookies,
        progress_callback=progress_callback,
    )

