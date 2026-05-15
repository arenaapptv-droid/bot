

import asyncio
import json
import os
import signal
import threading
import time
import uuid
import logging
import re
import shutil
from datetime import datetime
from typing import Dict, Optional, List, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.enums import ParseMode

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ============================================
# الإعدادات
# ============================================
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
API_HOST = "0.0.0.0"
API_PORT = 8000
API_KEY = "CHANGE_THIS_SECRET_KEY_NOW"

WORKER_POOL_SIZE = 5
FFMPEG_PATH = "ffmpeg"
HLS_BASE_URL = "http://164.68.102.28"
HLS_DIR = "/tmp/hls"
os.makedirs(HLS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================
# نماذج البيانات
# ============================================
class LogoConfig(BaseModel):
    enabled: bool = False
    image_url: Optional[str] = None
    full_overlay: bool = True
    x: int = 0
    y: int = 0

class StreamConfig(BaseModel):
    name: str
    slug: str                     # اسم فريد للرابط
    input_url: str
    output_url: str               # لـ RTMP فقط، HLS يهمل
    stream_type: str = "rtmp"
    quality: str = "medium"
    logo: LogoConfig = LogoConfig()
    fallback_url: Optional[str] = None
    schedule: Optional[str] = None
    is_local_file: bool = False

# ============================================
# تخزين الحالة
# ============================================
class MemoryState:
    def __init__(self):
        self._data = {}
        self._lock = threading.RLock()
        self._save_thread = None
        self._running = True
        self._load_backup()
        self._start_save_worker()

    def _to_serializable(self, obj):
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if isinstance(obj, dict):
            return {k: self._to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._to_serializable(i) for i in obj]
        return obj

    def _load_backup(self):
        if os.path.exists("state_backup.json"):
            try:
                with open("state_backup.json", "r") as f:
                    raw = json.load(f)
                for key, value in raw.items():
                    if key.startswith("stream:") and "logo" in value and isinstance(value["logo"], dict):
                        value["logo"] = LogoConfig(**value["logo"])
                    self._data[key] = value
                logger.info("تم تحميل النسخة الاحتياطية")
            except Exception as e:
                logger.error(f"خطأ في تحميل النسخة: {e}")

    def _start_save_worker(self):
        def save_loop():
            while self._running:
                time.sleep(30)
                with self._lock:
                    data_serializable = self._to_serializable(self._data)
                    with open("state_backup.json", "w") as f:
                        json.dump(data_serializable, f, indent=2)
        self._save_thread = threading.Thread(target=save_loop, daemon=True)
        self._save_thread.start()

    async def set(self, key: str, value: Any):
        with self._lock:
            self._data[key] = value

    async def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._data.get(key)

    async def delete(self, key: str):
        with self._lock:
            self._data.pop(key, None)

    async def keys(self, pattern: str = "*") -> List[str]:
        with self._lock:
            if pattern == "*":
                return list(self._data.keys())
            return [k for k in self._data.keys() if pattern in k]

    async def get_all_streams(self) -> Dict[str, StreamConfig]:
        stream_ids = await self.keys("stream:")
        streams = {}
        for sid in stream_ids:
            data = await self.get(sid)
            if data:
                if "logo" in data and isinstance(data["logo"], dict):
                    data["logo"] = LogoConfig(**data["logo"])
                streams[sid.replace("stream:", "")] = StreamConfig(**data)
        return streams

    async def save_stream(self, slug: str, config: StreamConfig):
        await self.set(f"stream:{slug}", config.model_dump())

    async def delete_stream(self, slug: str):
        await self.delete(f"stream:{slug}")
        await self.delete(f"status:{slug}")

    async def update_status(self, slug: str, status: dict):
        await self.set(f"status:{slug}", status)

    async def get_status(self, slug: str) -> dict:
        return await self.get(f"status:{slug}") or {}

    def shutdown(self):
        self._running = False
        if self._save_thread:
            self._save_thread.join(timeout=5)
        with self._lock:
            data_serializable = self._to_serializable(self._data)
            with open("state_backup.json", "w") as f:
                json.dump(data_serializable, f, indent=2)

state = MemoryState()

# ============================================
# Worker Manager
# ============================================
class FFmpegWorker:
    def __init__(self, max_workers=WORKER_POOL_SIZE):
        self.max_workers = max_workers
        self.active_workers = 0
        self._lock = asyncio.Lock()
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self.fps_monitor_tasks: Dict[str, asyncio.Task] = {}

    async def start_stream(self, slug: str, config: StreamConfig) -> bool:
        async with self._lock:
            if self.active_workers >= self.max_workers:
                return False
            self.active_workers += 1

        try:
            cmd = await self._build_ffmpeg_cmd(slug, config)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            self.processes[slug] = proc
            await state.update_status(slug, {
                "status": "running",
                "pid": proc.pid,
                "started_at": time.time(),
                "fps": 0,
                "bitrate": 0,
                "viewers": 0
            })
            monitor_task = asyncio.create_task(self._monitor_fps(slug, proc))
            self.fps_monitor_tasks[slug] = monitor_task
            asyncio.create_task(self._monitor_process(slug, proc, config))
            return True
        except Exception as e:
            async with self._lock:
                self.active_workers -= 1
            logger.error(f"فشل تشغيل البث {slug}: {e}")
            await state.update_status(slug, {"status": "error", "error": str(e)})
            return False

    async def stop_stream(self, slug: str):
        proc = self.processes.pop(slug, None)
        if proc and proc.returncode is None:
            proc.terminate()
            await asyncio.sleep(1)
            if proc.returncode is None:
                proc.kill()
        if slug in self.fps_monitor_tasks:
            self.fps_monitor_tasks[slug].cancel()
            del self.fps_monitor_tasks[slug]
        async with self._lock:
            self.active_workers -= 1
        await state.update_status(slug, {"status": "stopped", "pid": None})
        hls_path = os.path.join(HLS_DIR, slug)
        if os.path.exists(hls_path):
            shutil.rmtree(hls_path, ignore_errors=True)

    async def _monitor_fps(self, slug: str, proc: asyncio.subprocess.Process):
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode('utf-8', errors='ignore')
                fps_match = re.search(r'fps=\s*([\d.]+)', text)
                bitrate_match = re.search(r'bitrate=\s*([\d.]+)kbits?', text)
                if fps_match:
                    fps = float(fps_match.group(1))
                    status = await state.get_status(slug)
                    status['fps'] = fps
                    if bitrate_match:
                        status['bitrate'] = int(float(bitrate_match.group(1)))
                    await state.update_status(slug, status)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"خطأ في مراقبة fps للبث {slug}: {e}")

    async def _monitor_process(self, slug: str, proc: asyncio.subprocess.Process, config: StreamConfig):
        await proc.wait()
        current_status = await state.get_status(slug)
        if current_status.get("status") == "running":
            logger.warning(f"البث {slug} توقف فجأة، إعادة تشغيل تلقائي...")
            await asyncio.sleep(3)
            await self.start_stream(slug, config)
        else:
            async with self._lock:
                self.active_workers -= 1

    async def _build_ffmpeg_cmd(self, slug: str, config: StreamConfig) -> list:
        quality_params = {
            "low":   ["-b:v", "1000k", "-maxrate", "1000k", "-bufsize", "2000k", "-r", "25"],
            "medium":["-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", "-r", "30"],
            "high":  ["-b:v", "5000k", "-maxrate", "5000k", "-bufsize", "10000k", "-r", "30"]
        }.get(config.quality, ["-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", "-r", "30"])

        input_option = ["-re"] if not config.is_local_file else []
        cmd = [FFMPEG_PATH, *input_option, "-i", config.input_url]

        if config.logo.enabled and config.logo.image_url:
            cmd.extend(["-i", config.logo.image_url])
            if config.logo.full_overlay:
                filter_complex = "[1:v]scale=iw:ih[img];[0:v][img]overlay=0:0"
            else:
                filter_complex = f"[1:v]scale=iw*0.2:-1[logo];[0:v][logo]overlay={config.logo.x}:{config.logo.y}"
            cmd.extend(["-filter_complex", filter_complex])

        cmd.extend([
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            *quality_params,
            "-c:a", "aac", "-b:a", "128k"
        ])

        if config.stream_type == "hls":
            out_dir = os.path.join(HLS_DIR, slug)
            os.makedirs(out_dir, exist_ok=True)
            out_file = os.path.join(out_dir, "index.m3u8")
            cmd.extend([
                "-f", "hls", "-hls_time", "2", "-hls_list_size", "5",
                "-hls_flags", "delete_segments+append_list", "-y", out_file
            ])
        else:
            cmd.extend(["-f", "flv", config.output_url])
        return cmd

worker_manager = FFmpegWorker()

# ============================================
# FastAPI Backend
# ============================================
app = FastAPI(title="Stream Manager API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return True

@app.get("/live/{slug}/{file:path}")
async def serve_hls(slug: str, file: str):
    file_path = os.path.join(HLS_DIR, slug, file)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(file_path)

@app.get("/")
async def root():
    return {"status": "online", "version": "5.0"}

@app.post("/streams")
async def create_stream(config: StreamConfig, _=Depends(verify_api_key)):
    existing = await state.get(f"stream:{config.slug}")
    if existing:
        raise HTTPException(400, f"Slug '{config.slug}' already exists")
    await state.save_stream(config.slug, config)
    await state.update_status(config.slug, {"status": "stopped"})
    return {"slug": config.slug, "hls_url": f"{HLS_BASE_URL}/live/{config.slug}/index.m3u8" if config.stream_type == "hls" else None}

@app.get("/streams")
async def list_streams(_=Depends(verify_api_key)):
    streams = await state.get_all_streams()
    return {"streams": {slug: cfg.model_dump() for slug, cfg in streams.items()}}

@app.get("/streams/{slug}")
async def get_stream(slug: str, _=Depends(verify_api_key)):
    config = await state.get(f"stream:{slug}")
    if not config:
        raise HTTPException(404, "Not found")
    return config

@app.put("/streams/{slug}")
async def update_stream(slug: str, config: StreamConfig, _=Depends(verify_api_key)):
    current_status = await state.get_status(slug)
    was_running = current_status.get("status") == "running"
    if was_running:
        await worker_manager.stop_stream(slug)
    await state.save_stream(slug, config)
    if was_running:
        await worker_manager.start_stream(slug, config)
    return {"success": True}

@app.delete("/streams/{slug}")
async def delete_stream(slug: str, _=Depends(verify_api_key)):
    await worker_manager.stop_stream(slug)
    await state.delete_stream(slug)
    return {"success": True}

@app.post("/streams/{slug}/start")
async def start_stream(slug: str, background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    config_data = await state.get(f"stream:{slug}")
    if not config_data:
        raise HTTPException(404, "Not found")
    config = StreamConfig(**config_data)
    status = await state.get_status(slug)
    if status.get("status") == "running":
        raise HTTPException(400, "Already running")
    background_tasks.add_task(worker_manager.start_stream, slug, config)
    return {"message": "Start requested"}

@app.post("/streams/{slug}/stop")
async def stop_stream(slug: str, _=Depends(verify_api_key)):
    await worker_manager.stop_stream(slug)
    return {"message": "Stopped"}

@app.post("/streams/{slug}/restart")
async def restart_stream(slug: str, background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    await worker_manager.stop_stream(slug)
    config_data = await state.get(f"stream:{slug}")
    if not config_data:
        raise HTTPException(404, "Not found")
    config = StreamConfig(**config_data)
    background_tasks.add_task(worker_manager.start_stream, slug, config)
    return {"message": "Restart requested"}

@app.get("/streams/{slug}/status")
async def get_status(slug: str, _=Depends(verify_api_key)):
    status = await state.get_status(slug)
    config = await state.get(f"stream:{slug}")
    if not config:
        raise HTTPException(404, "Not found")
    return {"slug": slug, "name": config["name"], **status}

@app.get("/system/stats")
async def system_stats(_=Depends(verify_api_key)):
    if not PSUTIL_AVAILABLE:
        raise HTTPException(503, "psutil not installed")
    net_io = psutil.net_io_counters()
    stats = {
        "cpu": psutil.cpu_percent(interval=0.5),
        "ram": psutil.virtual_memory().percent,
        "ram_used_mb": psutil.virtual_memory().used // (1024**2),
        "ram_total_mb": psutil.virtual_memory().total // (1024**2),
        "disk": psutil.disk_usage("/").percent,
        "disk_used_gb": psutil.disk_usage("/").used // (1024**3),
        "disk_total_gb": psutil.disk_usage("/").total // (1024**3),
        "active_streams": len(worker_manager.processes),
        "total_streams": len(await state.get_all_streams()),
        "bandwidth_mb": (net_io.bytes_sent + net_io.bytes_recv) / (1024**2)
    }
    return stats

# ============================================
# Telegram Bot
# ============================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📡 قائمة القنوات")],
        [KeyboardButton(text="➕ إضافة قناة"), KeyboardButton(text="✏️ تعديل قناة")],
        [KeyboardButton(text="❌ حذف قناة"), KeyboardButton(text="📊 مراقبة السيرفر")],
        [KeyboardButton(text="🔄 إعادة تشغيل الكل"), KeyboardButton(text="⏹ إيقاف الكل")]
    ],
    resize_keyboard=True
)

user_data = {}

async def api_request(method: str, endpoint: str, json_data: dict = None):
    url = f"http://{API_HOST}:{API_PORT}{endpoint}"
    headers = {"X-API-Key": API_KEY}
    try:
        async with aiohttp.ClientSession() as session:
            if method == "GET":
                async with session.get(url, headers=headers) as resp:
                    return await resp.json()
            elif method == "POST":
                async with session.post(url, headers=headers, json=json_data) as resp:
                    return await resp.json()
            elif method == "PUT":
                async with session.put(url, headers=headers, json=json_data) as resp:
                    return await resp.json()
            elif method == "DELETE":
                async with session.delete(url, headers=headers) as resp:
                    return await resp.json()
    except Exception as e:
        return {"error": str(e)}

# ---------- مراقبة السيرفر الحية ----------
monitor_tasks = {}

async def live_monitor_loop(chat_id: int, message_id: int):
    last_text = ""
    while True:
        try:
            stats_res = await api_request("GET", "/system/stats")
            if "error" in stats_res:
                text = "❌ خطأ في جلب الإحصائيات"
            else:
                s = stats_res
                text = (
                    "🖥️ *مراقبة الخادم (حية)*\n\n"
                    f"📡 البثوث النشطة: `{s.get('active_streams',0)}` / `{s.get('total_streams',0)}`\n"
                    f"🖥️ CPU: `{s.get('cpu',0)}%`\n"
                    f"🧠 RAM: `{s.get('ram',0)}%` ({s.get('ram_used_mb',0)}/{s.get('ram_total_mb',0)} MB)\n"
                    f"💾 Disk: `{s.get('disk',0)}%` ({s.get('disk_used_gb',0)}/{s.get('disk_total_gb',0)} GB)\n"
                    f"🌐 Bandwidth: `{s.get('bandwidth_mb',0):.1f} MB`"
                )
            if text == last_text:
                await asyncio.sleep(3)
                continue
            last_text = text
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏹ إيقاف المراقبة", callback_data="stop_monitor")]
            ])
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id,
                                        reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(3)
        except Exception as e:
            if "message is not modified" not in str(e):
                logger.error(f"خطأ في حلقة المراقبة: {e}")
            await asyncio.sleep(3)

@dp.message(lambda msg: msg.text == "📊 مراقبة السيرفر")
async def start_server_monitor(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    stats_res = await api_request("GET", "/system/stats")
    if "error" in stats_res:
        text = "❌ خطأ في جلب الإحصائيات"
    else:
        s = stats_res
        text = (
            "🖥️ *مراقبة الخادم (حية)*\n\n"
            f"📡 البثوث النشطة: `{s.get('active_streams',0)}` / `{s.get('total_streams',0)}`\n"
            f"🖥️ CPU: `{s.get('cpu',0)}%`\n"
            f"🧠 RAM: `{s.get('ram',0)}%` ({s.get('ram_used_mb',0)}/{s.get('ram_total_mb',0)} MB)\n"
            f"💾 Disk: `{s.get('disk',0)}%` ({s.get('disk_used_gb',0)}/{s.get('disk_total_gb',0)} GB)\n"
            f"🌐 Bandwidth: `{s.get('bandwidth_mb',0):.1f} MB`"
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ إيقاف المراقبة", callback_data="stop_monitor")]
    ])
    sent = await message.reply(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    task = asyncio.create_task(live_monitor_loop(message.chat.id, sent.message_id))
    monitor_tasks[message.chat.id] = task

@dp.callback_query(lambda c: c.data == "stop_monitor")
async def stop_monitor(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    if chat_id in monitor_tasks:
        monitor_tasks[chat_id].cancel()
        del monitor_tasks[chat_id]
    await callback.message.edit_text("✅ تم إيقاف المراقبة الحية.", reply_markup=None)
    await callback.answer()

# ---------- الأوامر الرئيسية ----------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("🚫 غير مصرح")
        return
    await message.reply(
        "🎬 *نظام إدارة البث المتقدم*\n\n"
        "✅ يدعم RTMP و HLS.\n"
        "✅ رابط HLS بصيغة: `http://164.68.102.28/live/اسم_القناة/index.m3u8`\n"
        "✅ إضافة شعارات عبر رابط صورة (تغطية كاملة 16:9).\n\n"
        "استخدم الأزرار أدناه.",
        reply_markup=main_kb,
        parse_mode=ParseMode.MARKDOWN
    )

def slugify(name: str) -> str:
    slug = re.sub(r'[^a-zA-Z0-9\u0600-\u06FF\s-]', '', name)
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug.lower()

@dp.message(lambda msg: msg.text == "📡 قائمة القنوات")
async def list_channels(message: types.Message):
    res = await api_request("GET", "/streams")
    if "error" in res:
        await message.reply("❌ خطأ في الاتصال بالـ API")
        return
    streams_data = res.get("streams", {})
    if not streams_data:
        await message.reply("لا توجد قنوات. استخدم ➕ إضافة قناة.")
        return
    for slug, cfg in streams_data.items():
        status_res = await api_request("GET", f"/streams/{slug}/status")
        status = status_res.get("status", "unknown")
        s_type = cfg.get("stream_type", "rtmp")
        type_icon = "📺 HLS" if s_type == "hls" else "📡 RTMP"
        icon = "🟢" if status == "running" else "🔴"
        hls_link = f"\n🔗 {HLS_BASE_URL}/live/{slug}/index.m3u8" if s_type == "hls" else ""
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=f"{icon} {cfg['name']}", callback_data=f"view_{slug}"),
                InlineKeyboardButton(text="▶️", callback_data=f"start_{slug}"),
                InlineKeyboardButton(text="⏹️", callback_data=f"stop_{slug}")
            ],
            [
                InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
                InlineKeyboardButton(text="✏️ تعديل", callback_data=f"edit_{slug}"),
                InlineKeyboardButton(text="📊 تحديث الحالة", callback_data=f"refresh_{slug}")
            ]
        ])
        await message.answer(
            f"🎬 *{cfg['name']}*\n📛 المعرف: `{slug}`\n{type_icon}\n📡 الحالة: {status}{hls_link}",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )

# ---------- إضافة قناة جديدة ----------
@dp.message(lambda msg: msg.text == "➕ إضافة قناة")
async def add_channel_start(message: types.Message):
    user_data[message.from_user.id] = {"step": "add_name"}
    await message.reply("📝 أرسل اسم القناة (سيُستخدم في رابط HLS):")

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_name")
async def add_name_step(message: types.Message):
    name = message.text
    slug = slugify(name)
    # التحقق من عدم وجود نفس slug
    existing = await api_request("GET", f"/streams/{slug}")
    if "error" not in existing and existing.get("name"):
        await message.reply(f"⚠️ الاسم `{slug}` موجود مسبقاً. الرجاء استخدام اسم آخر.", parse_mode=ParseMode.MARKDOWN)
        return
    user_data[message.from_user.id] = {"step": "add_type", "name": name, "slug": slug}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 RTMP", callback_data="type_rtmp"),
         InlineKeyboardButton(text="📺 HLS", callback_data="type_hls")]
    ])
    await message.reply("اختر نوع البث:", reply_markup=kb)

@dp.callback_query(lambda c: c.data in ["type_rtmp", "type_hls"])
async def select_type(callback: types.CallbackQuery):
    stream_type = "rtmp" if callback.data == "type_rtmp" else "hls"
    user_data[callback.from_user.id]["stream_type"] = stream_type
    if stream_type == "hls":
        user_data[callback.from_user.id]["step"] = "add_input"
        await callback.message.reply("📥 أرسل رابط المصدر (يمكن أن يكون URL أو مسار ملف محلي):")
    else:
        user_data[callback.from_user.id]["step"] = "add_output"
        await callback.message.reply("📤 أرسل رابط الإخراج RTMP (مثال: rtmp://...):")
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_input")
async def add_input_step(message: types.Message):
    input_url = message.text
    user_data[message.from_user.id]["input"] = input_url
    stream_type = user_data[message.from_user.id].get("stream_type")
    if stream_type == "hls":
        await create_stream_final(message)
    else:
        user_data[message.from_user.id]["step"] = "add_output"
        await message.reply("📤 أرسل رابط الإخراج RTMP:")

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_output")
async def add_output_step(message: types.Message):
    output_url = message.text
    user_data[message.from_user.id]["output"] = output_url
    await create_stream_final(message)

async def create_stream_final(message: types.Message):
    data = user_data[message.from_user.id]
    name = data["name"]
    slug = data["slug"]
    stream_type = data["stream_type"]
    input_url = data["input"]
    output_url = data.get("output", "") if stream_type == "rtmp" else "dummy"
    is_local = os.path.exists(input_url)
    logo = {"enabled": False, "image_url": None, "full_overlay": True, "x": 0, "y": 0}
    config = {
        "name": name,
        "slug": slug,
        "input_url": input_url,
        "output_url": output_url,
        "stream_type": stream_type,
        "quality": "medium",
        "logo": logo,
        "fallback_url": None,
        "schedule": None,
        "is_local_file": is_local
    }
    res = await api_request("POST", "/streams", config)
    if "slug" in res:
        msg = f"✅ تم إنشاء القناة `{name}`\n📛 المعرف: `{slug}`\n"
        if stream_type == "hls":
            hls_url = f"{HLS_BASE_URL}/live/{slug}/index.m3u8"
            msg += f"🔗 رابط HLS: {hls_url}\n"
        msg += "يمكنك الآن إضافة شعار أو تعديل الإعدادات."
        await message.reply(msg, parse_mode=ParseMode.MARKDOWN)
    else:
        await message.reply("❌ فشل إنشاء القناة: " + str(res))
    del user_data[message.from_user.id]

# ---------- تعديل قناة ----------
@dp.message(lambda msg: msg.text == "✏️ تعديل قناة")
async def edit_channel_list(message: types.Message):
    res = await api_request("GET", "/streams")
    streams = res.get("streams", {})
    if not streams:
        await message.reply("لا توجد قنوات")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cfg['name'], callback_data=f"edit_{slug}")] for slug, cfg in streams.items()
    ])
    await message.reply("اختر القناة لتعديلها:", reply_markup=kb)

@dp.callback_query(lambda c: c.data.startswith("edit_"))
async def edit_stream_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    user_data[callback.from_user.id] = {"edit_slug": slug, "step": "choose_field"}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏷️ الاسم", callback_data=f"field_name_{slug}"),
         InlineKeyboardButton(text="📥 المصدر", callback_data=f"field_input_{slug}")],
        [InlineKeyboardButton(text="📤 الإخراج RTMP", callback_data=f"field_output_{slug}"),
         InlineKeyboardButton(text="⚙️ الجودة", callback_data=f"field_quality_{slug}")],
        [InlineKeyboardButton(text="🖼️ شعار (رابط)", callback_data=f"field_logo_{slug}"),
         InlineKeyboardButton(text="🔁 النوع (RTMP/HLS)", callback_data=f"field_type_{slug}")],
        [InlineKeyboardButton(text="⏰ جدولة", callback_data=f"field_schedule_{slug}"),
         InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_edit")]
    ])
    await callback.message.reply("اختر الحقل لتعديله:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("field_"))
async def edit_field_prompt(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    field = parts[1]
    slug = parts[2]
    user_data[callback.from_user.id] = {"edit_slug": slug, "edit_field": field}
    prompts = {
        "name": "🏷️ أرسل الاسم الجديد:",
        "input": "📥 أرسل رابط المصدر الجديد:",
        "output": "📤 أرسل رابط الإخراج RTMP الجديد:",
        "quality": "⚙️ أرسل الجودة (low, medium, high):",
        "type": "🔁 أرسل نوع البث (rtmp أو hls):",
        "logo": "🖼️ أرسل رابط الصورة للتغطية الكاملة (http://...) أو 'none' لإلغاء الشعار.\nمثال: https://example.com/logo.png",
        "schedule": "⏰ أرسل جدولة cron (مثال: 0 9 * * *) أو 'none':"
    }
    await callback.message.reply(prompts.get(field, "أرسل القيمة الجديدة:"))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_edit")
async def cancel_edit(callback: types.CallbackQuery):
    if callback.from_user.id in user_data:
        del user_data[callback.from_user.id]
    await callback.message.reply("❌ تم إلغاء التعديل")
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_data and "edit_field" in user_data[msg.from_user.id])
async def process_edit_value(message: types.Message):
    data = user_data[message.from_user.id]
    slug = data["edit_slug"]
    field = data["edit_field"]
    new_value = message.text.strip()
    if new_value.lower() == "none":
        new_value = None
    current = await api_request("GET", f"/streams/{slug}")
    if "error" in current:
        await message.reply("❌ خطأ في جلب البيانات")
        del user_data[message.from_user.id]
        return
    if field == "quality" and new_value not in ["low", "medium", "high"]:
        await message.reply("❌ جودة غير صالحة. استخدم low, medium, high")
        return
    if field == "type" and new_value not in ["rtmp", "hls"]:
        await message.reply("❌ نوع غير صالح. استخدم rtmp أو hls")
        return
    if field == "logo":
        if new_value and new_value.lower() != "none":
            if new_value.startswith(("http://", "https://")):
                current["logo"] = {
                    "enabled": True,
                    "image_url": new_value,
                    "full_overlay": True,
                    "x": 0,
                    "y": 0
                }
            else:
                await message.reply("❌ الرابط غير صالح. يجب أن يبدأ بـ http:// أو https://")
                return
        else:
            current["logo"] = {"enabled": False, "image_url": None, "full_overlay": True, "x": 0, "y": 0}
        res = await api_request("PUT", f"/streams/{slug}", json_data=current)
        if res.get("success"):
            await message.reply("✅ تم تحديث الشعار. إذا كان البث يعمل، سيتم إعادة تشغيله لتطبيق التغيير.")
        else:
            await message.reply("❌ فشل التحديث")
        del user_data[message.from_user.id]
        return
    # باقي الحقول
    if field in current:
        current[field] = new_value
    res = await api_request("PUT", f"/streams/{slug}", json_data=current)
    if res.get("success"):
        await message.reply(f"✅ تم تحديث {field}")
    else:
        await message.reply("❌ فشل التحديث")
    del user_data[message.from_user.id]

# ---------- حذف قناة ----------
@dp.message(lambda msg: msg.text == "❌ حذف قناة")
async def delete_channel_list(message: types.Message):
    res = await api_request("GET", "/streams")
    streams = res.get("streams", {})
    if not streams:
        await message.reply("لا توجد قنوات")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cfg['name'], callback_data=f"del_{slug}")] for slug, cfg in streams.items()
    ])
    await message.reply("اختر القناة لحذفها:", reply_markup=kb)

@dp.callback_query(lambda c: c.data.startswith("del_"))
async def delete_stream_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    await api_request("DELETE", f"/streams/{slug}")
    await callback.message.edit_text("🗑 تم حذف القناة")
    await callback.answer()

# ---------- أوامر التحكم بالبث ----------
@dp.callback_query(lambda c: c.data.startswith("start_"))
async def start_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{slug}/start")
    await callback.answer("✅ تم طلب التشغيل", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("stop_"))
async def stop_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{slug}/stop")
    await callback.answer("⏹️ تم طلب الإيقاف", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("restart_"))
async def restart_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{slug}/restart")
    await callback.answer("🔄 تم طلب إعادة التشغيل", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("refresh_"))
async def refresh_status_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    status_res = await api_request("GET", f"/streams/{slug}/status")
    name = status_res.get("name", slug)
    status = status_res.get("status", "unknown")
    fps = status_res.get("fps", 0)
    bitrate = status_res.get("bitrate", 0)
    started_at = status_res.get("started_at")
    uptime = ""
    if started_at:
        elapsed = time.time() - started_at
        uptime = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    text = (
        f"🎬 *{name}*\n📛 المعرف: `{slug}`\n"
        f"📡 الحالة: {status}\n"
        f"🎬 FPS: {fps}\n"
        f"📡 Bitrate: {bitrate} kbps\n"
        f"⏱️ مدة التشغيل: {uptime}\n"
        f"🆔 PID: {status_res.get('pid', '—')}"
    )
    await callback.message.edit_text(text, reply_markup=None, parse_mode=ParseMode.MARKDOWN)
    await callback.answer("تم تحديث الحالة", show_alert=False)

@dp.callback_query(lambda c: c.data.startswith("view_"))
async def view_stream_cb(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    status_res = await api_request("GET", f"/streams/{slug}/status")
    config_res = await api_request("GET", f"/streams/{slug}")
    name = config_res.get("name", slug)
    status = status_res.get("status", "unknown")
    fps = status_res.get("fps", 0)
    bitrate = status_res.get("bitrate", 0)
    started_at = status_res.get("started_at")
    uptime = ""
    if started_at:
        elapsed = time.time() - started_at
        uptime = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    hls_link = ""
    if config_res.get("stream_type") == "hls":
        hls_link = f"\n🔗 {HLS_BASE_URL}/live/{slug}/index.m3u8"
    text = (
        f"🎬 *{name}*\n📛 المعرف: `{slug}`\n"
        f"📡 الحالة: {status}\n"
        f"🎬 FPS: {fps}\n"
        f"📡 Bitrate: {bitrate} kbps\n"
        f"⏱️ مدة التشغيل: {uptime}\n"
        f"🆔 PID: {status_res.get('pid', '—')}{hls_link}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"start_{slug}"),
         InlineKeyboardButton(text="⏹️ إيقاف", callback_data=f"stop_{slug}")],
        [InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
         InlineKeyboardButton(text="✏️ تعديل", callback_data=f"edit_{slug}"),
         InlineKeyboardButton(text="📊 تحديث", callback_data=f"refresh_{slug}")],
        [InlineKeyboardButton(text="🗑 حذف", callback_data=f"del_{slug}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# ---------- أوامر كلية ----------
@dp.message(lambda msg: msg.text == "🔄 إعادة تشغيل الكل")
async def restart_all(message: types.Message):
    res = await api_request("GET", "/streams")
    for slug in res.get("streams", {}):
        await api_request("POST", f"/streams/{slug}/restart")
    await message.reply("✅ تم طلب إعادة تشغيل جميع البثوث")

@dp.message(lambda msg: msg.text == "⏹ إيقاف الكل")
async def stop_all(message: types.Message):
    res = await api_request("GET", "/streams")
    for slug in res.get("streams", {}):
        await api_request("POST", f"/streams/{slug}/stop")
    await message.reply("✅ تم طلب إيقاف جميع البثوث")

# ============================================
# تشغيل الخدمات
# ============================================
async def shutdown(sig):
    logger.info(f"استقبل إشارة {sig.name}, جاري الإيقاف...")
    for slug in list(worker_manager.processes.keys()):
        await worker_manager.stop_stream(slug)
    state.shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    api_task = asyncio.create_task(server.serve())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    logger.info(f"🚀 النظام يعمل - API على http://{API_HOST}:{API_PORT}")
    logger.info(f"🌐 روابط HLS: {HLS_BASE_URL}/live/<slug>/index.m3u8")
    await asyncio.gather(api_task, bot_task)

if __name__ == "__main__":
    if os.system(f"{FFMPEG_PATH} -version > /dev/null 2>&1") != 0:
        print("خطأ: ffmpeg غير مثبت.")
        exit(1)
    asyncio.run(main())
EOF

# 4. تشغيل البوت من جديد
python3 main.py