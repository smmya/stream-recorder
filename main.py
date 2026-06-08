"""
流媒体自动录制系统 v2.5
支持 SRS / nginx-rtmp API 多源轮询 + 直接 RTMP/RTMPS 订阅
FFmpeg 自动录制 · 时间段控制 · 录制期间暂停轮询 · TOTP 零信任登录 · IP 封禁 · 日志系统
"""
import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import re
import signal
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp
import pyotp
import qrcode
import yaml
from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt

# ============================================
# 日志系统
# ============================================


class RingBufferHandler(logging.Handler):
    """内存环形缓冲区日志处理器，保存最近 N 条日志供 Web 查看"""

    def __init__(self, capacity: int = 2000):
        super().__init__()
        self.buffer = deque(maxlen=capacity)
        self.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"))

    def emit(self, record):
        self.buffer.append({
            "time": self.formatter.formatTime(record, self.formatter.default_time_format),
            "level": record.levelname,
            "name": record.name,
            "message": self.format(record),
            "levelno": record.levelno,
        })

    def get_logs(self, level: str = None, limit: int = 200, search: str = None):
        """获取缓冲日志，支持级别筛选和搜索"""
        records = list(self.buffer)
        if level:
            levelno = getattr(logging, level.upper(), 0)
            records = [r for r in records if r["levelno"] >= levelno]
        if search:
            search_lower = search.lower()
            records = [r for r in records if search_lower in r["message"].lower()]
        return list(reversed(records))[-limit:]


def setup_logging():
    """配置日志系统：文件滚动日志（可配置级别+大小+切割） + 内存环形缓冲"""
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_cfg = config.get("logging", {})
    file_level_str = log_cfg.get("file_level", "INFO").upper()
    file_level = getattr(logging, file_level_str, logging.INFO)
    max_bytes = (log_cfg.get("max_size_mb", 10) or 10) * 1024 * 1024
    backup_count = log_cfg.get("backup_count", 5) or 5

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # 根 logger 保持 INFO，各 handler 自己过滤

    # 清除已有的 handler（避免重复）
    root_logger.handlers.clear()

    # 控制台输出（始终 INFO）
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(console)

    # 文件滚动日志（级别、大小、备份数可配置）
    global file_handler_ref
    file_handler_ref = RotatingFileHandler(
        log_dir / "app.log", maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
    )
    file_handler_ref.setLevel(file_level)
    file_handler_ref.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s"))
    root_logger.addHandler(file_handler_ref)

    # 内存环形缓冲区（始终 INFO，Web 查看用）
    global ring_handler
    ring_handler = RingBufferHandler(capacity=2000)
    root_logger.addHandler(ring_handler)

    return logging.getLogger("app")


ring_handler: RingBufferHandler = None
file_handler_ref: RotatingFileHandler = None
logger = None  # 将在 setup_logging() 调用后初始化


def reconfigure_logging():
    """动态重配置日志：文件级别、大小、备份数变更后调用"""
    log_cfg = config.get("logging", {})
    file_level_str = log_cfg.get("file_level", "INFO").upper()
    file_level = getattr(logging, file_level_str, logging.INFO)
    max_bytes = (log_cfg.get("max_size_mb", 10) or 10) * 1024 * 1024
    backup_count = log_cfg.get("backup_count", 5) or 5

    if file_handler_ref:
        file_handler_ref.setLevel(file_level)
        # RotatingFileHandler 不支持动态修改 maxBytes/backupCount，但下次创建时会生效
        logger.info(f"日志配置更新: 级别={file_level_str}, 大小={log_cfg.get('max_size_mb', 10)}MB, 备份={backup_count}")

# ============================================
# 配置加载
# ============================================
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.yaml"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_config(config):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


config = load_config()
logger = setup_logging()  # 初始化日志系统

SECRET_KEY = config["server"].get("secret_key", "default-secret")
TOKEN_EXPIRE_HOURS = 24
ALGORITHM = "HS256"

app = FastAPI(title="流媒体自动录制系统", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# 全局状态
# ============================================
# 活跃的录制任务: {key: {"process": ..., "started_at": ..., ...}}
active_recordings: dict = {}
# 暂停轮询的 RTMP 订阅（正在录制中，跳过轮询，录制完成后恢复）
paused_subscriptions: set = set()
# 每个 RTMP 订阅的上次轮询时间: {subscription_key: datetime}
last_poll_times: dict = {}
# 每个 API 流源的上次轮询时间: {source_name: datetime}
last_source_poll_times: dict = {}
polling_task: Optional[asyncio.Task] = None
polling_running = False

# ============================================
# 安全模块 — IP 封禁 & TOTP
# ============================================
# IP 登录失败追踪: {ip: {"attempts": int, "last_attempt": datetime, "banned_until": datetime|None}}
failed_logins: dict = {}
# TOTP 密钥（存在 config 中，运行时缓存）


def get_client_ip(request: Request) -> str:
    """获取客户端真实 IP"""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("X-Real-IP", "")
    if real_ip:
        return real_ip.strip()
    if request.client:
        return request.client.host
    return "127.0.0.1"


def check_ip_banned(ip: str) -> Optional[str]:
    """检查 IP 是否被封禁，返回封禁原因或 None"""
    security = config.get("security", {})
    max_attempts = security.get("max_failed_attempts", 5)
    ban_minutes = security.get("ban_duration_minutes", 15)
    info = failed_logins.get(ip)
    if not info:
        return None
    if info.get("banned_until"):
        if datetime.now() < info["banned_until"]:
            remaining = (info["banned_until"] - datetime.now()).seconds
            return f"IP 已被封禁，剩余 {remaining // 60} 分 {remaining % 60} 秒"
        else:
            # 封禁已过期，清理
            failed_logins.pop(ip, None)
    return None


def record_failed_login(ip: str):
    """记录一次登录失败"""
    security = config.get("security", {})
    max_attempts = security.get("max_failed_attempts", 5)
    ban_minutes = security.get("ban_duration_minutes", 15)

    info = failed_logins.setdefault(ip, {"attempts": 0, "last_attempt": None, "banned_until": None})
    info["attempts"] += 1
    info["last_attempt"] = datetime.now()
    if info["attempts"] >= max_attempts:
        info["banned_until"] = datetime.now() + timedelta(minutes=ban_minutes)
        logger.warning(f"[安全] IP {ip} 被封禁 {ban_minutes} 分钟（失败 {info['attempts']} 次）")


def reset_failed_logins(ip: str):
    """登录成功后重置失败计数"""
    failed_logins.pop(ip, None)


def get_totp_secret() -> Optional[str]:
    """获取已保存的 TOTP 密钥"""
    return config.get("auth", {}).get("totp_secret")


def is_totp_required() -> bool:
    """是否需要 TOTP 二次验证"""
    return bool(get_totp_secret()) and config.get("security", {}).get("totp_enabled", True)

# ============================================
# 认证模块
# ============================================


def create_token(username: str) -> str:
    expire_utc = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    expire = expire_utc.replace(tzinfo=None)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None


async def get_current_user(request: Request) -> str:
    token = request.cookies.get("token")
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="未登录")
    username = verify_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="登录已过期")
    return username


# ============================================
# API 解析器（SRS / nginx-rtmp）
# ============================================


async def parse_srs_api(api_url: str, rtmp_base: str, timeout: int = 5) -> list:
    """解析 SRS HTTP API 返回的流列表"""
    streams = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                data = await resp.json()
                if data.get("code") != 0:
                    return streams
                for s in data.get("streams", []):
                    publish = s.get("publish", {})
                    if publish.get("active"):
                        video = s.get("video", {})
                        audio = s.get("audio", {})
                        stream_name = s.get("name", "")
                        source_url = s.get("url", f"{rtmp_base}{stream_name}")
                        streams.append({
                            "stream_name": stream_name,
                            "source_url": source_url,
                            "clients": s.get("clients", 0),
                            "video_codec": video.get("codec", "N/A"),
                            "audio_codec": audio.get("codec", "N/A"),
                            "resolution": f"{video.get('width', '?')}x{video.get('height', '?')}",
                            "kbps_recv": s.get("kbps", {}).get("recv_30s", 0),
                            "active": True,
                        })
    except Exception as e:
        logger.warning(f"[SRS] 解析失败 {api_url}: {e}")
    return streams


async def parse_nginx_rtmp_stat(api_url: str, rtmp_base: str, timeout: int = 5) -> list:
    """解析 nginx-rtmp stat XML 返回的流列表"""
    streams = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                xml_text = await resp.text()
                root = ET.fromstring(xml_text)
                for app in root.findall(".//application"):
                    app_name = app.findtext("name", "")
                    for live in app.findall("live"):
                        for stream in live.findall("stream"):
                            stream_name = stream.findtext("name", "")
                            active_elem = stream.find("active")
                            is_active = active_elem is not None and active_elem.text == "1"
                            if is_active:
                                full_name = f"{app_name}/{stream_name}" if app_name else stream_name
                                source_url = f"{rtmp_base}{stream_name}"
                                streams.append({
                                    "stream_name": full_name,
                                    "source_url": source_url,
                                    "clients": int(stream.findtext("nclients", "0")),
                                    "video_codec": stream.findtext(".//meta/video/codec", "N/A"),
                                    "audio_codec": stream.findtext(".//meta/audio/codec", "N/A"),
                                    "resolution": f"{stream.findtext('.//meta/video/width', '?')}x{stream.findtext('.//meta/video/height', '?')}",
                                    "kbps_recv": 0,
                                    "active": True,
                                })
    except Exception as e:
        logger.warning(f"[Nginx-RTMP] 解析失败 {api_url}: {e}")
    return streams


async def fetch_streams(source: dict, timeout: int = 5) -> list:
    """根据流源类型获取实时流列表"""
    api_url = source.get("api_url", "")
    rtmp_base = source.get("rtmp_base", "rtmp://127.0.0.1/live/")
    source_type = source.get("type", "srs")

    if source_type == "nginx-rtmp":
        return await parse_nginx_rtmp_stat(api_url, rtmp_base, timeout)
    else:
        return await parse_srs_api(api_url, rtmp_base, timeout)


# ============================================
# RTMP 直接探测（ffprobe）
# ============================================


def sub_key(subscription: dict) -> str:
    """生成 RTMP 订阅的唯一键（基于 URL 的 MD5，保证同事增加删除不冲突）"""
    url = subscription.get("url", "")
    return f"rtmp_sub::{hashlib.md5(url.encode()).hexdigest()[:12]}"


async def probe_rtmp_stream(url: str, timeout: int = 5) -> Optional[dict]:
    """
    用 ffprobe 探测 RTMP/RTMPS 流是否在线
    返回流信息字典，若不在线则返回 None
    """
    ffprobe_path = config["recording"].get("ffprobe_path", "ffprobe")
    timeout_us = timeout * 1000000  # ffprobe 使用微秒
    cmd = f'"{ffprobe_path}" -v quiet -print_format json -show_streams -timeout {timeout_us} "{url}"'

    try:
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout + 5)

        if process.returncode != 0:
            return None

        data = json.loads(stdout.decode("utf-8", errors="replace"))
        streams = data.get("streams", [])
        if not streams:
            return None

        # 提取视频/音频信息
        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        return {
            "online": True,
            "video_codec": video_stream.get("codec_name", "N/A") if video_stream else "N/A",
            "audio_codec": audio_stream.get("codec_name", "N/A") if audio_stream else "N/A",
            "width": video_stream.get("width", 0) if video_stream else 0,
            "height": video_stream.get("height", 0) if video_stream else 0,
            "resolution": f"{video_stream.get('width', '?')}x{video_stream.get('height', '?')}" if video_stream else "N/A",
        }
    except asyncio.TimeoutError:
        return None
    except Exception as e:
        logger.warning(f"[RTMP探测] 失败 {url}: {e}")
        return None


# ============================================
# 时间段判断
# ============================================


def is_in_poll_window(subscription: dict) -> bool:
    """判断当前时间是否在订阅的轮询时间窗口内"""
    poll_start = subscription.get("poll_start", "").strip()
    poll_end = subscription.get("poll_end", "").strip()

    # 如果都没设置，表示全天轮询
    if not poll_start and not poll_end:
        return True

    now = datetime.now().time()

    if poll_start and poll_end:
        try:
            start_h, start_m = map(int, poll_start.split(":"))
            end_h, end_m = map(int, poll_end.split(":"))
            start_time = datetime.now().replace(hour=start_h, minute=start_m, second=0).time()
            end_time = datetime.now().replace(hour=end_h, minute=end_m, second=0).time()

            if start_time <= end_time:
                # 正常区间：如 08:00 - 22:00
                return start_time <= now <= end_time
            else:
                # 跨天区间：如 22:00 - 08:00
                return now >= start_time or now <= end_time
        except ValueError:
            return True

    if poll_start:
        try:
            start_h, start_m = map(int, poll_start.split(":"))
            start_time = datetime.now().replace(hour=start_h, minute=start_m, second=0).time()
            return now >= start_time
        except ValueError:
            return True

    if poll_end:
        try:
            end_h, end_m = map(int, poll_end.split(":"))
            end_time = datetime.now().replace(hour=end_h, minute=end_m, second=0).time()
            return now <= end_time
        except ValueError:
            return True

    return True


# ============================================
# 脚本钩子系统
# ============================================


def resolve_vars(template: str, ctx: dict) -> str:
    """替换脚本中的变量占位符"""
    result = template
    for key, value in ctx.items():
        result = result.replace(f"{{{key}}}", str(value) if value else "")
    return result


def get_stream_hooks(source_url: str = "", record_type: str = "api") -> dict:
    """
    获取流的脚本钩子配置。
    优先返回专用脚本；如果专用脚本为空，则返回全局脚本。
    返回 {"on_start": str|None, "on_stop": str|None}
    """
    # 检查是否有专用脚本（仅在 rtmp_sub 模式下）
    if record_type == "rtmp_sub":
        for sub in (config.get("rtmp_subscriptions") or []):
            if sub.get("url") == source_url:
                sub_hooks = sub.get("hooks", {})
                if sub_hooks.get("on_start") or sub_hooks.get("on_stop"):
                    return {
                        "on_start": sub_hooks.get("on_start", ""),
                        "on_stop": sub_hooks.get("on_stop", ""),
                    }
                break

    # 回退到全局脚本
    global_hooks = config.get("hooks", {})
    return {
        "on_start": global_hooks.get("on_recording_start", ""),
        "on_stop": global_hooks.get("on_recording_stop", ""),
    }


async def execute_hook(script: str, ctx: dict, hook_type: str):
    """异步执行脚本钩子"""
    if not script or not script.strip():
        return

    resolved = resolve_vars(script, ctx)
    logger.info(f"[Hook] 执行 {hook_type}: {resolved[:200]}")

    try:
        process = await asyncio.create_subprocess_shell(
            resolved,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # 不等待，后台执行（避免阻塞录制流程），但记录结果
        asyncio.create_task(_log_hook_result(process, hook_type))
    except Exception as e:
        logger.error(f"[Hook] 执行异常 ({hook_type}): {e}")


async def _log_hook_result(process, hook_type: str):
    """记录钩子执行结果"""
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        if process.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace")[:500]
            logger.error(f"[Hook] {hook_type} 失败 (code={process.returncode}): {err_msg}")
        else:
            out_msg = stdout.decode("utf-8", errors="replace")[:200]
            if out_msg.strip():
                logger.info(f"[Hook] {hook_type} 完成: {out_msg.strip()}")
    except asyncio.TimeoutError:
        logger.warning(f"[Hook] {hook_type} 超时（被忽略）")
    except Exception as e:
        logger.error(f"[Hook] 结果处理异常: {e}")


# ============================================
# FFmpeg 录制管理器
# ============================================


def make_stream_key(source_name: str, stream_name: str) -> str:
    """生成录制任务的唯一键"""
    return f"{source_name}::{stream_name}"


def make_sub_filename(remarks: str, url: str) -> str:
    """根据备注和 URL 生成录制文件名"""
    if remarks:
        safe = re.sub(r'[<>:"/\\|?*]', "_", remarks.strip())
    else:
        # 从 URL 提取可读片段
        slug = url.rstrip("/").split("/")[-1] if "/" in url else url
        safe = re.sub(r'[<>:"/\\|?*]', "_", slug)[:30]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{safe}_{ts}.mp4"


def make_output_filename(source_name: str, stream_name: str) -> str:
    """生成录制文件名称（API 模式用）"""
    safe_name = re.sub(r'[<>:"/\\|?*]', "_", f"{source_name}_{stream_name}")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{safe_name}_{ts}.mp4"


async def start_recording(
    source_name: str,
    stream_name: str,
    source_url: str,
    record_type: str = "api",
    remarks: str = "",
) -> bool:
    """启动 FFmpeg 录制。record_type: 'api' 或 'rtmp_sub'"""
    key = make_stream_key(source_name, stream_name)
    if key in active_recordings:
        logger.info(f"[录制] 已在录制中: {key}")
        return False

    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    output_dir.mkdir(parents=True, exist_ok=True)

    if record_type == "rtmp_sub":
        filename = make_sub_filename(remarks, source_url)
    else:
        filename = make_output_filename(source_name, stream_name)

    output_path = output_dir / filename

    ffmpeg_path = config["recording"].get("ffmpeg_path", "ffmpeg")
    ffmpeg_args = config["recording"].get("ffmpeg_args", "-c copy -f mp4")
    cmd = f'"{ffmpeg_path}" -i "{source_url}" {ffmpeg_args} "{output_path}" -y'

    logger.info(f"[FFmpeg] 开始录制 [{record_type}]: {cmd}")

    try:
        if sys.platform == "win32":
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=0x00000200,  # CREATE_NEW_PROCESS_GROUP
            )
        else:
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        active_recordings[key] = {
            "process": process,
            "started_at": datetime.now(),
            "source_name": source_name,
            "stream_name": stream_name,
            "source_url": source_url,
            "output_file": str(output_path),
            "filename": filename,
            "record_type": record_type,
        }
        logger.warning(f"[录制] 已启动: {key} → {filename}")

        # 执行录制开始钩子
        hooks = get_stream_hooks(source_url, record_type)
        if hooks.get("on_start"):
            ctx = {
                "stream_name": stream_name,
                "source_url": source_url,
                "source_name": source_name,
                "filename": filename,
                "output_file": str(output_path),
                "remarks": remarks,
                "record_type": record_type,
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            await execute_hook(hooks["on_start"], ctx, "on_start")

        return True
    except Exception as e:
        logger.error(f"[FFmpeg] 启动失败: {e}")
        return False


async def stop_recording(key: str) -> bool:
    """停止 FFmpeg 录制，返回被停止录制的 source_url（用于恢复轮询）"""
    info = active_recordings.get(key)
    if not info:
        return False

    process = info["process"]
    source_url = info.get("source_url", "")
    record_type = info.get("record_type", "api")
    logger.info(f"[FFmpeg] 停止录制: {key}")

    try:
        if process.stdin:
            process.stdin.write(b"q")
            await process.stdin.drain()
            process.stdin.close()
    except Exception:
        pass

    try:
        await asyncio.wait_for(process.wait(), timeout=15)
    except asyncio.TimeoutError:
        logger.warning(f"[FFmpeg] 超时，强制终止: {key}")
        try:
            process.kill()
            await process.wait()
        except Exception:
            pass

    # 执行录制停止钩子（在删除记录前）
    hooks = get_stream_hooks(info.get("source_url", ""), info.get("record_type", "api"))
    if hooks.get("on_stop"):
        ctx = {
            "stream_name": info.get("stream_name", ""),
            "source_url": info.get("source_url", ""),
            "source_name": info.get("source_name", ""),
            "filename": info.get("filename", ""),
            "output_file": info.get("output_file", ""),
            "remarks": info.get("stream_name", ""),
            "record_type": info.get("record_type", "api"),
            "start_time": info["started_at"].strftime("%Y-%m-%d %H:%M:%S") if info.get("started_at") else "",
            "stop_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        await execute_hook(hooks["on_stop"], ctx, "on_stop")

    del active_recordings[key]

    # RTMP 订阅录制停止后，恢复该订阅的轮询
    if record_type == "rtmp_sub" and source_url:
        # 找到对应的订阅并恢复轮询
        for sub in (config.get("rtmp_subscriptions") or []):
            if sub.get("url", "") == source_url:
                s_key = sub_key(sub)
                paused_subscriptions.discard(s_key)
                logger.info(f"[轮询] 恢复订阅轮询: {source_url}")
                break

    logger.warning(f"[录制] 已停止: {key}")
    return True


async def stop_all_recordings():
    """停止所有录制"""
    for key in list(active_recordings.keys()):
        await stop_recording(key)


# ============================================
# 统一轮询引擎
# ============================================


async def probe_and_record_subscription(sub: dict):
    """
    探测单个 RTMP 订阅：在线 → 开始录制 + 暂停轮询；不在线 → 不处理
    """
    url = sub.get("url", "")
    remarks = sub.get("remarks", "")
    s_key = sub_key(sub)

    if not url:
        return

    # 已暂停轮询（正在录制中），跳过
    if s_key in paused_subscriptions:
        return

    # 检查是否已在录制
    for r_key, info in active_recordings.items():
        if info.get("source_url") == url and info.get("record_type") == "rtmp_sub":
            # 已在录制，加入暂停集合
            paused_subscriptions.add(s_key)
            return

    # 时间段检查
    if not is_in_poll_window(sub):
        return

    # 探测流是否在线
    timeout = sub.get("probe_timeout", 5)
    probe_result = await probe_rtmp_stream(url, timeout)

    if probe_result and probe_result.get("online"):
        # 构造来源名称
        source_name = remarks or f"RTMP订阅"
        stream_name = url

        # 开始录制
        started = await start_recording(
            source_name=source_name,
            stream_name=stream_name,
            source_url=url,
            record_type="rtmp_sub",
            remarks=remarks,
        )
        if started:
            # 录制开始，暂停该订阅的轮询
            paused_subscriptions.add(s_key)
            logger.info(f"[轮询] 暂停订阅轮询（录制中）: {url}")


async def check_and_stop_finished_sub_recordings():
    """
    检查 RTMP 订阅的录制是否已完成（进程退出 + 流不在线）
    已完成 → 恢复轮询
    """
    subscriptions = (config.get("rtmp_subscriptions") or [])

    for key in list(active_recordings.keys()):
        info = active_recordings.get(key)
        if not info or info.get("record_type") != "rtmp_sub":
            continue

        process = info["process"]
        source_url = info.get("source_url", "")

        # 检查 FFmpeg 进程是否已退出
        if process.returncode is not None:
            # 进程已退出，检查流是否还在线
            probe_result = await probe_rtmp_stream(source_url, 3)
            if not probe_result or not probe_result.get("online"):
                # 流不在线，可以安全地清理录制记录
                logger.info(f"[轮询] FFmpeg 已退出，流不在线，恢复轮询: {source_url}")
                await stop_recording(key)
            else:
                # 流还在线但 FFmpeg 退出了（可能是错误），重新开始录制
                logger.warning(f"[轮询] FFmpeg 异常退出，流仍在线，重新录制: {source_url}")
                del active_recordings[key]

                # 找到对应的订阅信息
                for sub in subscriptions:
                    if sub.get("url", "") == source_url:
                        await start_recording(
                            source_name=sub.get("remarks", "") or "RTMP订阅",
                            stream_name=source_url,
                            source_url=source_url,
                            record_type="rtmp_sub",
                            remarks=sub.get("remarks", ""),
                        )
                        s_key = sub_key(sub)
                        paused_subscriptions.add(s_key)
                        break


async def polling_loop():
    """主轮询循环：API 模式（全局间隔） + RTMP 直接订阅模式（独立间隔）"""
    global polling_running

    global_interval = config["polling"].get("interval", 10)
    loop_tick = 1  # 每秒检查一次

    while polling_running:
        try:
            # ---- 1. API 模式：按各自间隔扫描 SRS/nginx-rtmp 流源 ----
            sources = config.get("stream_sources", [])
            all_active_api_streams = set()
            now = datetime.now()

            for source in sources:
                source_name = source.get("name", "Unknown")
                src_interval = source.get("poll_interval", global_interval)
                if src_interval < 1:
                    src_interval = global_interval

                # 时间段检查
                if not is_in_poll_window(source):
                    continue

                last_time = last_source_poll_times.get(source_name)
                if last_time and (now - last_time).total_seconds() < src_interval:
                    continue

                last_source_poll_times[source_name] = now

                try:
                    streams = await fetch_streams(source)
                    for s in streams:
                        key = make_stream_key(source_name, s["stream_name"])
                        all_active_api_streams.add(key)

                        if key not in active_recordings:
                            logger.warning(f"[轮询-API] 检测到新流: {source_name}/{s['stream_name']}")
                            await start_recording(
                                source_name,
                                s["stream_name"],
                                s["source_url"],
                                record_type="api",
                            )
                except Exception as e:
                    logger.warning(f"[轮询-API] 扫描失败 {source_name}: {e}")

            # API 模式：检查流失效
            for key in list(active_recordings.keys()):
                info = active_recordings.get(key, {})
                if info.get("record_type") == "api" and key not in all_active_api_streams:
                    logger.info(f"[轮询-API] 流已下线，停止录制: {key}")
                    await stop_recording(key)

            # ---- 2. RTMP 直接订阅模式（独立间隔）----
            await check_and_stop_finished_sub_recordings()

            subscriptions = (config.get("rtmp_subscriptions") or [])
            now = datetime.now()

            for sub in subscriptions:
                if not sub.get("enabled", True):
                    continue

                s_key = sub_key(sub)
                url = sub.get("url", "")

                # 已在录制中 → 跳过
                if s_key in paused_subscriptions:
                    continue

                # 时间段检查
                if not is_in_poll_window(sub):
                    continue

                # 独立间隔检查：每个订阅按自己的 poll_interval 控制频率
                sub_interval = sub.get("poll_interval", global_interval)
                if sub_interval < 1:
                    sub_interval = 1  # 最小 1 秒

                last_time = last_poll_times.get(s_key)
                if last_time and (now - last_time).total_seconds() < sub_interval:
                    continue  # 还没到该订阅的轮询间隔

                last_poll_times[s_key] = now

                try:
                    await probe_and_record_subscription(sub)
                except Exception as e:
                    logger.warning(f"[轮询-订阅] 探测失败 {url}: {e}")

        except Exception as e:
            logger.error(f"[轮询] 循环异常: {e}")

        await asyncio.sleep(loop_tick)


async def start_polling():
    global polling_running, polling_task
    if polling_running:
        return
    polling_running = True
    polling_task = asyncio.create_task(polling_loop())
    print("[系统] 轮询已启动")


async def stop_polling():
    global polling_running, polling_task
    polling_running = False
    if polling_task:
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
        polling_task = None
    paused_subscriptions.clear()
    await stop_all_recordings()
    print("[系统] 轮询已停止")


# ============================================
# API 路由
# ============================================


# --- 认证 ---
@app.post("/api/login")
async def login(request: Request):
    body = await request.json()
    username = body.get("username", "")
    password = body.get("password", "")
    totp_code = body.get("totp_code", "")
    ip = get_client_ip(request)

    # 1. IP 封禁检查
    ban_reason = check_ip_banned(ip)
    if ban_reason:
        raise HTTPException(status_code=403, detail=ban_reason)

    # 2. 用户名密码验证
    auth_config = config.get("auth", {})
    pw_valid = (username == auth_config.get("username") and password == auth_config.get("password"))

    if not pw_valid:
        record_failed_login(ip)
        info = failed_logins.get(ip, {})
        remaining = (config.get("security", {}).get("max_failed_attempts", 5) - info.get("attempts", 0))
        detail = f"用户名或密码错误" + (f"，剩余尝试次数: {max(0, remaining)}" if remaining > 0 else "，IP 已封禁")
        raise HTTPException(status_code=401, detail=detail)

    # 3. TOTP 二次验证
    if is_totp_required():
        secret = get_totp_secret()
        if not totp_code:
            reset_failed_logins(ip)  # 密码正确，不清零但先不封
            return JSONResponse({"success": True, "require_totp": True})
        totp = pyotp.TOTP(secret)
        if not totp.verify(totp_code):
            record_failed_login(ip)
            raise HTTPException(status_code=401, detail="TOTP 验证码错误")

    # 登录成功
    reset_failed_logins(ip)
    token = create_token(username)
    response = JSONResponse({"success": True, "username": username, "ip": ip})
    response.set_cookie("token", token, httponly=True, max_age=TOKEN_EXPIRE_HOURS * 3600)
    return response


@app.get("/api/logout")
async def logout():
    response = JSONResponse({"success": True})
    response.delete_cookie("token")
    return response


@app.get("/api/check_auth")
async def check_auth(username: str = None):
    if username:
        return {"authenticated": True, "username": username}
    return {"authenticated": False}


# --- 流源管理（API 模式） ---
@app.get("/api/sources")
async def get_sources(user: str = Depends(get_current_user)):
    sources = config.get("stream_sources", [])
    for i, s in enumerate(sources):
        s["_id"] = i
    return {"sources": sources}


@app.post("/api/sources")
async def add_source(request: Request, user: str = Depends(get_current_user)):
    body = await request.json()
    name = body.get("name", "").strip()
    api_url = body.get("api_url", "").strip()
    source_type = body.get("type", "srs")
    rtmp_base = body.get("rtmp_base", "").strip()

    if not name or not api_url:
        raise HTTPException(status_code=400, detail="名称和API地址不能为空")

    config.setdefault("stream_sources", []).append({
        "name": name, "type": source_type, "api_url": api_url, "rtmp_base": rtmp_base,
        "poll_interval": body.get("poll_interval"),
        "poll_start": body.get("poll_start", "").strip(),
        "poll_end": body.get("poll_end", "").strip(),
    })
    save_config(config)
    return {"success": True, "message": "流源添加成功"}


@app.put("/api/sources/{source_id}")
async def update_source(source_id: int, request: Request, user: str = Depends(get_current_user)):
    sources = config.get("stream_sources", [])
    if source_id < 0 or source_id >= len(sources):
        raise HTTPException(status_code=404, detail="流源不存在")
    body = await request.json()
    for key in ["name", "type", "api_url", "rtmp_base", "poll_interval", "poll_start", "poll_end"]:
        if key in body:
            sources[source_id][key] = body[key]
    save_config(config)
    return {"success": True, "message": "流源已更新"}


@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: int, user: str = Depends(get_current_user)):
    sources = config.get("stream_sources", [])
    if source_id < 0 or source_id >= len(sources):
        raise HTTPException(status_code=404, detail="流源不存在")
    deleted = sources.pop(source_id)
    save_config(config)
    logger.info(f"[配置] 已删除流源: {deleted.get('name')}")
    return {"success": True, "message": f"流源 '{deleted.get('name')}' 已删除"}


@app.post("/api/sources/{source_id}/test")
async def test_source(source_id: int, user: str = Depends(get_current_user)):
    """测试流源连通性：先测试 HTTP 连通性，再解析流列表"""
    sources = config.get("stream_sources", [])
    if source_id < 0 or source_id >= len(sources):
        raise HTTPException(status_code=404, detail="流源不存在")

    source = sources[source_id]
    api_url = source.get("api_url", "")

    # 第一步：测试 HTTP 连通性
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status >= 400:
                    raise HTTPException(
                        status_code=400,
                        detail=f"API 返回错误状态码 {resp.status}",
                    )
                # 读取响应内容
                content_type = resp.headers.get("Content-Type", "")
                body = await resp.read()
    except aiohttp.ClientConnectorError as e:
        raise HTTPException(status_code=400, detail=f"无法连接: {e}")
    except asyncio.TimeoutError:
        raise HTTPException(status_code=400, detail="连接超时（5秒）")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"网络错误: {e}")

    # 第二步：解析流列表
    source_type = source.get("type", "srs")
    streams = []
    try:
        if source_type == "nginx-rtmp":
            body_text = body.decode("utf-8", errors="replace")
            root = ET.fromstring(body_text)
            for app in root.findall(".//application"):
                app_name = app.findtext("name", "")
                for live in app.findall("live"):
                    for stream in live.findall("stream"):
                        name = stream.findtext("name", "")
                        active = stream.find("active")
                        is_active = active is not None and active.text == "1"
                        if is_active:
                            streams.append({
                                "stream_name": f"{app_name}/{name}" if app_name else name,
                                "resolution": f"{stream.findtext('.//meta/video/width', '?')}x{stream.findtext('.//meta/video/height', '?')}",
                                "video_codec": stream.findtext(".//meta/video/codec", "N/A"),
                                "clients": int(stream.findtext("nclients", "0")),
                            })
        else:
            data = json.loads(body.decode("utf-8", errors="replace"))
            if data.get("code") == 0:
                for s in data.get("streams", []):
                    pub = s.get("publish", {})
                    if pub.get("active"):
                        video = s.get("video", {})
                        streams.append({
                            "stream_name": s.get("name", ""),
                            "resolution": f"{video.get('width', '?')}x{video.get('height', '?')}",
                            "video_codec": video.get("codec", "N/A"),
                            "clients": s.get("clients", 0),
                        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"解析响应失败: {e}")

    return {
        "success": True,
        "source_name": source.get("name"),
        "source_type": source.get("type"),
        "api_url": api_url,
        "stream_count": len(streams),
        "streams": streams[:20],
    }


# --- RTMP 直接订阅管理 ---
@app.get("/api/subscriptions")
async def get_subscriptions(user: str = Depends(get_current_user)):
    """获取所有 RTMP 直接订阅"""
    subs = (config.get("rtmp_subscriptions") or [])
    for i, s in enumerate(subs):
        s["_id"] = i
        s_key = sub_key(s)
        # 检查是否在录制中
        is_recording = False
        recording_started = None
        for r_key, info in active_recordings.items():
            if info.get("source_url") == s.get("url") and info.get("record_type") == "rtmp_sub":
                is_recording = True
                recording_started = info["started_at"].isoformat()
                break
        s["is_recording"] = is_recording
        s["recording_started"] = recording_started
        s["is_paused"] = s_key in paused_subscriptions
        s["in_poll_window"] = is_in_poll_window(s)
        # 返回在线状态
        s["is_online"] = s.get("_last_online", False)
    return {"subscriptions": subs}


@app.post("/api/subscriptions")
async def add_subscription(request: Request, user: str = Depends(get_current_user)):
    """添加 RTMP 直接订阅"""
    body = await request.json()
    url = body.get("url", "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="RTMP 地址不能为空")
    if not (url.startswith("rtmp://") or url.startswith("rtmps://")):
        raise HTTPException(status_code=400, detail="地址必须以 rtmp:// 或 rtmps:// 开头")

    # 检查重复
    for s in (config.get("rtmp_subscriptions") or []):
        if s.get("url") == url:
            raise HTTPException(status_code=400, detail="该地址已存在")

    new_sub = {
        "url": url,
        "remarks": body.get("remarks", "").strip(),
        "enabled": body.get("enabled", True),
        "poll_start": body.get("poll_start", "").strip(),
        "poll_end": body.get("poll_end", "").strip(),
        "poll_interval": body.get("poll_interval"),
        "probe_timeout": body.get("probe_timeout", 5),
        "hooks": body.get("hooks", {}),
    }
    config.setdefault("rtmp_subscriptions", []).append(new_sub)
    save_config(config)
    return {"success": True, "message": "RTMP 订阅添加成功"}


@app.put("/api/subscriptions/{sub_id}")
async def update_subscription(sub_id: int, request: Request, user: str = Depends(get_current_user)):
    """更新 RTMP 直接订阅"""
    subs = (config.get("rtmp_subscriptions") or [])
    if sub_id < 0 or sub_id >= len(subs):
        raise HTTPException(status_code=404, detail="订阅不存在")

    body = await request.json()
    for key in ["url", "remarks", "enabled", "poll_start", "poll_end", "poll_interval", "probe_timeout", "hooks"]:
        if key in body:
            subs[sub_id][key] = body[key]

    # 如果录制的订阅被禁用，停止录制
    if not subs[sub_id].get("enabled", True):
        await _stop_subscription_recording(subs[sub_id])

    save_config(config)
    return {"success": True, "message": "订阅已更新"}


@app.delete("/api/subscriptions/{sub_id}")
async def delete_subscription(sub_id: int, user: str = Depends(get_current_user)):
    """删除 RTMP 直接订阅"""
    subs = (config.get("rtmp_subscriptions") or [])
    if sub_id < 0 or sub_id >= len(subs):
        raise HTTPException(status_code=404, detail="订阅不存在")

    deleted = subs.pop(sub_id)
    await _stop_subscription_recording(deleted)

    # 清理暂停状态
    paused_subscriptions.discard(sub_key(deleted))

    save_config(config)
    logger.info(f"[配置] 已删除RTMP订阅: {deleted.get('url')}")
    return {"success": True, "message": f"订阅 '{deleted.get('remarks', deleted.get('url'))}' 已删除"}


async def _stop_subscription_recording(sub: dict):
    """停止某个订阅对应的录制任务"""
    url = sub.get("url", "")
    for key, info in list(active_recordings.items()):
        if info.get("source_url") == url and info.get("record_type") == "rtmp_sub":
            await stop_recording(key)


@app.post("/api/subscriptions/{sub_id}/probe")
async def probe_subscription_now(sub_id: int, user: str = Depends(get_current_user)):
    """立即探测某个 RTMP 订阅（手动触发）"""
    subs = (config.get("rtmp_subscriptions") or [])
    if sub_id < 0 or sub_id >= len(subs):
        raise HTTPException(status_code=404, detail="订阅不存在")

    sub = subs[sub_id]
    result = await probe_rtmp_stream(sub.get("url"), sub.get("probe_timeout", 5))
    return {"online": result is not None and result.get("online"), "details": result}


# --- 实时状态 ---
@app.get("/api/streams/status")
async def get_streams_status(user: str = Depends(get_current_user)):
    """获取所有流的状态（API 模式 + RTMP 订阅模式）"""
    results = []

    # API 模式
    sources = config.get("stream_sources", [])
    for source in sources:
        source_name = source.get("name", "Unknown")
        try:
            streams = await fetch_streams(source, timeout=2)
            for s in streams:
                key = make_stream_key(source_name, s["stream_name"])
                s["source_name"] = source_name
                s["source_type"] = "api"
                s["is_recording"] = key in active_recordings
                s["recording_started"] = (
                    active_recordings[key]["started_at"].isoformat()
                    if key in active_recordings else None
                )
                results.append(s)
        except Exception as e:
            results.append({
                "source_name": source_name, "source_type": "api",
                "stream_name": "ERROR", "active": False,
                "error": str(e), "is_recording": False,
            })

    # RTMP 订阅模式
    subs = (config.get("rtmp_subscriptions") or [])
    for sub in subs:
        if not sub.get("enabled", True):
            continue
        url = sub.get("url", "")
        remarks = sub.get("remarks", "")
        s_key = sub_key(sub)

        # 检查录制状态
        is_recording = False
        recording_started = None
        for r_key, info in active_recordings.items():
            if info.get("source_url") == url and info.get("record_type") == "rtmp_sub":
                is_recording = True
                recording_started = info["started_at"].isoformat()
                break

        # 如果在录制中，不重复探测（节省资源）
        if is_recording:
            in_window = is_in_poll_window(sub)
            results.append({
                "source_name": remarks or url,
                "source_type": "rtmp_sub",
                "stream_name": url,
                "source_url": url,
                "active": True,
                "is_recording": True,
                "recording_started": recording_started,
                "in_poll_window": in_window,
                "is_paused": True,
            })
        elif s_key not in paused_subscriptions and is_in_poll_window(sub):
            try:
                probe = await probe_rtmp_stream(url, min(sub.get("probe_timeout", 5), 3))
                sub["_last_online"] = probe is not None and probe.get("online")
                if probe and probe.get("online"):
                    results.append({
                        "source_name": remarks or url,
                        "source_type": "rtmp_sub",
                        "stream_name": url,
                        "source_url": url,
                        "active": True,
                        "clients": 0,
                        "video_codec": probe.get("video_codec", "N/A"),
                        "audio_codec": probe.get("audio_codec", "N/A"),
                        "resolution": probe.get("resolution", "N/A"),
                        "is_recording": False,
                        "in_poll_window": True,
                        "is_paused": False,
                    })
                else:
                    results.append({
                        "source_name": remarks or url,
                        "source_type": "rtmp_sub",
                        "stream_name": url,
                        "source_url": url,
                        "active": False,
                        "is_recording": False,
                        "in_poll_window": True,
                        "is_paused": False,
                    })
            except Exception as e:
                pass

    return {"streams": results, "count": len(results)}


# --- 轮询控制 ---
@app.get("/api/polling/status")
async def get_polling_status(user: str = Depends(get_current_user)):
    """获取轮询状态"""
    subs = (config.get("rtmp_subscriptions") or [])
    return {
        "running": polling_running,
        "interval": config["polling"].get("interval", 10),
        "active_recordings": len(active_recordings),
        "paused_subscriptions": len(paused_subscriptions),
        "total_subscriptions": len(subs),
        "recording_list": [
            {
                "key": key,
                "source_name": info["source_name"],
                "stream_name": info["stream_name"],
                "started_at": info["started_at"].isoformat(),
                "output_file": info["filename"],
                "record_type": info.get("record_type", "api"),
            }
            for key, info in active_recordings.items()
        ],
    }


@app.post("/api/polling/start")
async def api_start_polling(user: str = Depends(get_current_user)):
    await start_polling()
    return {"success": True, "message": "轮询已启动"}


@app.post("/api/polling/stop")
async def api_stop_polling(user: str = Depends(get_current_user)):
    await stop_polling()
    return {"success": True, "message": "轮询已停止"}


@app.put("/api/polling/interval")
async def set_polling_interval(request: Request, user: str = Depends(get_current_user)):
    body = await request.json()
    interval = body.get("interval", 10)
    if interval < 3:
        raise HTTPException(status_code=400, detail="轮询间隔不能小于 3 秒")
    config["polling"]["interval"] = interval
    save_config(config)
    return {"success": True, "interval": interval}


# --- 录制管理 ---
@app.get("/api/recordings")
async def get_recordings(user: str = Depends(get_current_user)):
    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    output_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for f in sorted(output_dir.glob("*.mp4"), key=lambda x: x.stat().st_mtime, reverse=True):
        stat = f.stat()
        size_mb = stat.st_size / (1024 * 1024)
        is_active = False
        for info in active_recordings.values():
            if info.get("output_file") == str(f):
                is_active = True
                break
        files.append({
            "id": f.name,
            "filename": f.name,
            "size_mb": round(size_mb, 2),
            "size_bytes": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "is_recording": is_active,
        })
    return {"files": files, "total": len(files)}


@app.get("/api/recordings/{filename}/play")
async def play_recording(filename: str, user: str = Depends(get_current_user)):
    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    filepath = output_dir / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(filepath, media_type="video/mp4")


@app.delete("/api/recordings/{filename}")
async def delete_recording(filename: str, user: str = Depends(get_current_user)):
    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    filepath = output_dir / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    for key, info in list(active_recordings.items()):
        if info.get("output_file") == str(filepath):
            raise HTTPException(status_code=400, detail="该文件正在录制中，请先停止轮询")

    os.remove(filepath)
    return {"success": True, "message": f"文件 '{filename}' 已删除"}


# --- 视频流媒体播放（支持 Range 请求） ---
@app.get("/api/video/{filename}")
async def stream_video(filename: str, request: Request):
    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    filepath = output_dir / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    file_size = filepath.stat().st_size
    range_header = request.headers.get("range")

    if range_header:
        start, end = 0, file_size - 1
        match = re.search(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            start = int(match.group(1))
            end_str = match.group(2)
            if end_str:
                end = int(end_str)

        if start >= file_size:
            raise HTTPException(status_code=416, detail="Range 不可用")

        chunk_size = end - start + 1

        async def range_stream():
            async with aiofiles.open(filepath, "rb") as f:
                await f.seek(start)
                remaining = chunk_size
                while remaining > 0:
                    chunk_size_read = min(8192, remaining)
                    data = await f.read(chunk_size_read)
                    if not data:
                        break
                    yield data
                    remaining -= len(data)

        return StreamingResponse(
            range_stream(),
            status_code=206,
            media_type="video/mp4",
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(chunk_size),
            },
        )
    else:
        return FileResponse(filepath, media_type="video/mp4")


# --- 配置管理 ---
@app.put("/api/config/auth")
async def update_auth(request: Request, user: str = Depends(get_current_user)):
    body = await request.json()
    if "username" in body:
        config["auth"]["username"] = body["username"]
    if "password" in body:
        config["auth"]["password"] = body["password"]
    save_config(config)
    return {"success": True, "message": "认证信息已更新"}


# --- 安全功能 ---
@app.get("/api/security/status")
async def get_security_status(request: Request, user: str = Depends(get_current_user)):
    """获取安全状态：当前 IP、TOTP 状态、封禁列表"""
    ip = get_client_ip(request)
    totp_enabled = is_totp_required()
    ban_info = []
    for banned_ip, info in list(failed_logins.items()):
        if info.get("banned_until") and datetime.now() < info["banned_until"]:
            remaining = (info["banned_until"] - datetime.now()).seconds
            ban_info.append({
                "ip": banned_ip,
                "attempts": info["attempts"],
                "banned_until": info["banned_until"].isoformat(),
                "remaining_seconds": remaining,
                "is_current": banned_ip == ip,
            })
    return {
        "current_ip": ip,
        "totp_enabled": totp_enabled,
        "banned_ips": ban_info,
        "total_banned": len(ban_info),
        "max_attempts": config.get("security", {}).get("max_failed_attempts", 5),
        "ban_duration": config.get("security", {}).get("ban_duration_minutes", 15),
    }


@app.post("/api/security/totp/setup")
async def setup_totp(request: Request, user: str = Depends(get_current_user)):
    """生成 TOTP 密钥，返回密钥和二维码（base64 PNG）"""
    secret = pyotp.random_base32()
    issuer = "StreamRecorder"
    username = config.get("auth", {}).get("username", "admin")
    uri = pyotp.totp.TOTP(secret).provisioning_uri(name=username, issuer_name=issuer)

    # 生成二维码图片
    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    qr_b64 = base64.b64encode(buf.getvalue()).decode()

    # 暂存密钥（未确认前不生效）
    config["auth"]["_pending_totp_secret"] = secret
    save_config(config)
    return {"secret": secret, "uri": uri, "qr_base64": qr_b64}


@app.post("/api/security/totp/verify")
async def verify_totp(request: Request, user: str = Depends(get_current_user)):
    """验证 TOTP 设置（需提供当前有效验证码）"""
    body = await request.json()
    code = body.get("code", "").strip()
    pending_secret = config.get("auth", {}).get("_pending_totp_secret")
    if not pending_secret:
        raise HTTPException(status_code=400, detail="请先生成 TOTP 密钥")

    totp = pyotp.TOTP(pending_secret)
    if not totp.verify(code):
        raise HTTPException(status_code=400, detail="TOTP 验证码错误，请确认时间同步")

    # 确认生效
    config["auth"]["totp_secret"] = pending_secret
    config["auth"].pop("_pending_totp_secret", None)
    config.setdefault("security", {})["totp_enabled"] = True
    save_config(config)
    return {"success": True, "message": "TOTP 二次验证已启用"}


@app.post("/api/security/totp/disable")
async def disable_totp(request: Request, user: str = Depends(get_current_user)):
    """禁用 TOTP"""
    body = await request.json()
    code = body.get("code", "").strip()
    secret = get_totp_secret()

    if secret:
        totp = pyotp.TOTP(secret)
        if not totp.verify(code):
            raise HTTPException(status_code=400, detail="TOTP 验证码错误，无法禁用")

    config["auth"].pop("totp_secret", None)
    config["auth"].pop("_pending_totp_secret", None)
    config.setdefault("security", {})["totp_enabled"] = False
    save_config(config)
    return {"success": True, "message": "TOTP 二次验证已禁用"}


@app.post("/api/security/unban")
async def unban_ip(request: Request, user: str = Depends(get_current_user)):
    """手动解封 IP"""
    body = await request.json()
    ip = body.get("ip", "").strip()
    if not ip:
        raise HTTPException(status_code=400, detail="请指定 IP")
    failed_logins.pop(ip, None)
    logger.warning(f"[安全] 手动解封 IP: {ip}")
    return {"success": True, "message": f"IP {ip} 已解封"}


# --- 日志查看 ---
@app.get("/api/logs")
async def get_logs(
    level: str = Query(None, description="日志级别: DEBUG/INFO/WARNING/ERROR"),
    limit: int = Query(200, ge=10, le=2000, description="返回条数"),
    search: str = Query(None, description="搜索关键词"),
    user: str = Depends(get_current_user),
):
    """获取系统日志"""
    if ring_handler is None:
        return {"logs": [], "total": 0}
    logs = ring_handler.get_logs(level=level, limit=limit, search=search)
    return {"logs": logs, "total": len(logs)}


@app.get("/api/logs/download")
async def download_logs(user: str = Depends(get_current_user)):
    """下载完整日志文件"""
    log_file = BASE_DIR / "logs" / "app.log"
    if log_file.exists():
        return FileResponse(log_file, media_type="text/plain", filename="stream-recorder.log")
    raise HTTPException(status_code=404, detail="日志文件不存在")


# --- 文件系统 & 配置 ---
@app.get("/api/config")
async def get_config_info(user: str = Depends(get_current_user)):
    """获取当前配置中的关键信息"""
    return {
        "port": config["server"].get("port", 5000),
        "host": config["server"].get("host", "127.0.0.1"),
        "recording_dir": config["recording"].get("output_dir", "./recordings"),
        "log_level": config.get("logging", {}).get("file_level", "INFO"),
        "log_max_size_mb": config.get("logging", {}).get("max_size_mb", 10),
        "log_backup_count": config.get("logging", {}).get("backup_count", 5),
    }


@app.put("/api/config/recording_dir")
async def update_recording_dir(request: Request, user: str = Depends(get_current_user)):
    """更新录制文件保存目录"""
    body = await request.json()
    new_dir = body.get("path", "").strip()
    if not new_dir:
        raise HTTPException(status_code=400, detail="路径不能为空")

    path = Path(new_dir).resolve()
    # 测试可读写
    try:
        path.mkdir(parents=True, exist_ok=True)
        test_file = path / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"目录不可写: {e}")

    config["recording"]["output_dir"] = str(path)
    save_config(config)
    return {"success": True, "path": str(path), "message": "录制目录已更新"}


@app.put("/api/config/port")
async def update_port(request: Request, user: str = Depends(get_current_user)):
    """更新 Web 端口（需重启生效）"""
    body = await request.json()
    port = body.get("port", 0)
    if not 1024 <= port <= 65535:
        raise HTTPException(status_code=400, detail="端口范围: 1024-65535")
    config["server"]["port"] = port
    save_config(config)
    return {"success": True, "port": port, "message": "端口已保存，重启服务后生效"}


@app.put("/api/config/logging")
async def update_logging_config(request: Request, user: str = Depends(get_current_user)):
    """更新日志配置（即时生效）"""
    body = await request.json()
    log_cfg = config.setdefault("logging", {})

    if "file_level" in body:
        level = body["file_level"].upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise HTTPException(status_code=400, detail="级别必须是 DEBUG/INFO/WARNING/ERROR")
        log_cfg["file_level"] = level

    if "max_size_mb" in body:
        size = body["max_size_mb"]
        if not (1 <= size <= 500):
            raise HTTPException(status_code=400, detail="大小范围: 1-500 MB")
        log_cfg["max_size_mb"] = size

    if "backup_count" in body:
        count = body["backup_count"]
        if not (1 <= count <= 30):
            raise HTTPException(status_code=400, detail="备份数范围: 1-30")
        log_cfg["backup_count"] = count

    save_config(config)
    reconfigure_logging()
    return {"success": True, "message": "日志配置已更新（即时生效）"}


@app.get("/api/filesystem/browse")
async def browse_directory(path: str = "/", user: str = Depends(get_current_user)):
    """浏览文件系统目录（用于文件夹选择器）"""
    try:
        target = Path(path).resolve()
        if not target.exists():
            target = Path("/")
        if not target.is_dir():
            target = target.parent

        # 获取父目录
        parent = str(target.parent) if target.parent != target else None

        # 列出目录内容（只显示目录）
        items = []
        try:
            for item in sorted(target.iterdir()):
                if item.is_dir() and not item.name.startswith("."):
                    items.append({
                        "name": item.name,
                        "path": str(item),
                    })
        except PermissionError:
            pass

        # 构建路径面包屑
        breadcrumbs = []
        current = target
        while current != current.parent:
            breadcrumbs.insert(0, {"name": current.name or str(current), "path": str(current)})
            current = current.parent
            if len(breadcrumbs) > 10:
                break
        # 确保根路径在里面
        if not breadcrumbs or breadcrumbs[0]["path"] != str(target):
            breadcrumbs.insert(0, {"name": target.name or str(target), "path": str(target)})

        return {
            "current": str(target),
            "parent": parent,
            "items": items,
            "breadcrumbs": breadcrumbs,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/filesystem/test")
async def test_directory(request: Request, user: str = Depends(get_current_user)):
    """测试目录是否可读写"""
    body = await request.json()
    path_str = body.get("path", "").strip()
    if not path_str:
        raise HTTPException(status_code=400, detail="路径不能为空")

    path = Path(path_str).resolve()
    result = {"path": str(path), "exists": False, "writable": False, "readable": False}

    if path.exists():
        result["exists"] = True
        result["readable"] = os.access(path, os.R_OK)
        try:
            test_file = path / ".write_test"
            test_file.write_text("test")
            test_file.unlink()
            result["writable"] = True
        except Exception:
            pass
    else:
        # 尝试创建目录
        try:
            path.mkdir(parents=True, exist_ok=True)
            result["exists"] = True
            result["readable"] = True
            test_file = path / ".write_test"
            test_file.write_text("test")
            test_file.unlink()
            result["writable"] = True
            # 清理刚创建的目录（如果之前不存在）
            try:
                path.rmdir()
                # 如果目录非空则无法删除，保留就行
            except OSError:
                pass
        except Exception as e:
            result["error"] = str(e)

    return result


# ============================================
# 静态文件 & 前端
# ============================================

static_dir = BASE_DIR / "static"
static_dir.mkdir(parents=True, exist_ok=True)


@app.get("/")
async def index():
    html_path = static_dir / "index.html"
    if html_path.exists():
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>前端文件不存在，请将 index.html 放入 static 目录</h1>")


# ============================================
# 生命周期
# ============================================


@app.on_event("startup")
async def on_startup():
    print("=" * 55)
    print("  流媒体自动录制系统 v2.0.0")
    logger.info(f"  服务地址: http://{config['server']['host']}:{config['server']['port']}")
    logger.info(f"  录制目录: {config['recording']['output_dir']}")
    logger.info(f"  API 流源: {len(config.get('stream_sources', []))} 个")
    logger.info(f"  RTMP 订阅: {len(config.get('rtmp_subscriptions') or [])} 个")
    print("=" * 55)

    output_dir = Path(config["recording"].get("output_dir", "./recordings"))
    output_dir.mkdir(parents=True, exist_ok=True)

    if config["polling"].get("enabled_on_start", True):
        await start_polling()


@app.on_event("shutdown")
async def on_shutdown():
    print("[系统] 正在关闭...")
    await stop_polling()


# ============================================
# 主入口
# ============================================
if __name__ == "__main__":
    import argparse
    import sys
    import uvicorn

    parser = argparse.ArgumentParser(description="流媒体自动录制系统")
    parser.add_argument(
        "--daemon", "-d",
        action="store_true",
        help="后台模式：进程脱离终端在后台运行（日志输出到 logs/ 目录）",
    )
    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="测试模式：监听 0.0.0.0（允许外部访问），默认仅监听 127.0.0.1",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="绑定的主机地址（覆盖配置文件和 --test）",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=config["server"].get("port", 5000),
        help="绑定的端口",
    )
    args = parser.parse_args()

    # 确定监听地址：命令行 --host > --test > config 默认值
    if args.host:
        host = args.host
    elif args.test:
        host = "0.0.0.0"
    else:
        host = config["server"].get("host", "127.0.0.1")

    # 添加 --test 到 daemon 模式参数透传
    test_flag = ["--test"] if args.test else []
    if args.host:
        test_flag = ["--host", args.host]  # host 覆盖 test

    if args.daemon:
        # 后台模式：派生子进程运行服务，父进程立即退出
        import subprocess

        log_dir = BASE_DIR / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        out_log = log_dir / "server.log"
        err_log = log_dir / "server_error.log"

        # 构造前台启动命令（去掉 --daemon/-d，保留 --test/-t/--host）
        filtered_argv = [a for a in sys.argv[1:] if a not in ("--daemon", "-d")]
        cmd = [sys.executable, __file__] + filtered_argv

        logger.info(f"[启动] 后台模式：服务将在后台启动")
        logger.info(f"[启动] 监听地址: {host}:{args.port}")
        logger.info(f"[启动] 标准输出 → {out_log}")
        logger.info(f"[启动] 错误输出 → {err_log}")
        logger.info(f"[启动] PID 文件 → {log_dir / 'server.pid'}")

        with open(out_log, "a") as out_f, open(err_log, "a") as err_f:
            out_f.write(f"\n--- 启动于 {__import__('datetime').datetime.now()} ---\n")
            popen_kwargs = {
                "stdout": out_f,
                "stderr": err_f,
                "stdin": subprocess.DEVNULL,
                "start_new_session": True,
            }
            if sys.platform == "win32":
                popen_kwargs["creationflags"] = subprocess.DETACHED_PROCESS
            proc = subprocess.Popen(cmd, **popen_kwargs)

        # 写入 PID 文件
        with open(log_dir / "server.pid", "w") as f:
            f.write(str(proc.pid))

        logger.info(f"[启动] 后台进程 PID: {proc.pid}，主进程退出")
        sys.exit(0)

    # 前台模式（默认）：进程保持在前台，Ctrl+C 停止
    access_note = "仅本地访问" if host == "127.0.0.1" else "[!] 允许外部访问 (--test 模式)"
    print("=" * 55)
    print("  流媒体自动录制系统 v2.4")
    logger.info(f"  模式: 前台运行 (适合 supervisor/systemd 管理)")
    logger.info(f"  地址: http://{host}:{args.port}")
    logger.info(f"  访问: {access_note}")
    logger.info(f"  按 Ctrl+C 停止服务")
    print("=" * 55)

    uvicorn.run(
        "main:app",
        host=host,
        port=args.port,
        reload=False,
        log_config=None,
    )
