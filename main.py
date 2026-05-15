#!/usr/bin/env python3
"""
نظام بث متكامل بمعمارية:
Telegram Bot -> FastAPI Backend -> Redis/Database -> Workers -> FFmpeg/OME
كل التحكم يتم عبر API، والبوت لا يشغل البث مباشرة.
"""

import asyncio
import json
import os
import signal
import subprocess
import time
import logging
import threading
from typing import Dict, Optional, List, Any
from datetime import datetime
import uuid

# المكتبات الخارجية
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
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

# محاولة استيراد redis (اختياري)
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ redis غير مثبت، سيتم استخدام تخزين مؤقت بالذاكرة (غير موصى به للإنتاج).")

# محاولة استيراد psutil للمراقبة
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ============================================
# الإعدادات الأساسية
# ============================================
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
API_HOST = "0.0.0.0"
API_PORT = 8000
API_KEY = "CHANGE_THIS_SECRET_KEY_NOW"

# إعدادات Redis (اختياري)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# إعدادات العمال (Workers)
WORKER_POOL_SIZE = 5  # عدد العمليات المتزامنة القصوى
FFMPEG_PATH = "ffmpeg"
HLS_OUTPUT_DIR = "/var/www/hls"   # المجلد الذي يخدم منه OvenMediaEngine أو nginx
os.makedirs(HLS_OUTPUT_DIR, exist_ok=True)

# ============================================
# نماذج البيانات (Pydantic)
# ============================================
class StreamConfig(BaseModel):
    name: str
    input_url: str
    output_url: str   # rtmp أو hls مسار
    quality: str = "medium"   # low, medium, high
    enable_logo: bool = False
    logo_path: Optional[str] = None
    fallback_url: Optional[str] = None
    schedule: Optional[str] = None  # cron expression

class StreamStatus(BaseModel):
    stream_id: str
    name: str
    status: str  # stopped, starting, running, error, stopping
    pid: Optional[int] = None
    started_at: Optional[float] = None
    viewers: int = 0
    fps: float = 0.0
    bitrate: int = 0

# ============================================
# قاعدة البيانات / تخزين الحالة
# ============================================
class StateManager:
    def __init__(self):
        self.use_redis = REDIS_AVAILABLE
        if self.use_redis:
            self.redis = None
        else:
            self._memory_store = {}
            self._lock = threading.Lock()
            self._save_thread = None
            self._running = True
            self._start_save_worker()

    async def connect_redis(self):
        if self.use_redis:
            self.redis = await redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
                                              decode_responses=True)
            await self.redis.ping()
            print("✅ Redis متصل")

    async def set(self, key: str, value: Any, ttl: int = None):
        if self.use_redis and self.redis:
            if ttl:
                await self.redis.setex(key, ttl, json.dumps(value))
            else:
                await self.redis.set(key, json.dumps(value))
        else:
            with self._lock:
                self._memory_store[key] = value

    async def get(self, key: str) -> Optional[Any]:
        if self.use_redis and self.redis:
            data = await self.redis.get(key)
            return json.loads(data) if data else None
        else:
            with self._lock:
                return self._memory_store.get(key)

    async def delete(self, key: str):
        if self.use_redis and self.redis:
            await self.redis.delete(key)
        else:
            with self._lock:
                self._memory_store.pop(key, None)

    async def keys(self, pattern: str = "*") -> List[str]:
        if self.use_redis and self.redis:
            return await self.redis.keys(pattern)
        else:
            with self._lock:
                return [k for k in self._memory_store.keys() if pattern == "*" or pattern in k]

    async def get_all_streams(self) -> Dict[str, StreamConfig]:
        """إرجاع جميع القنوات المسجلة"""
        stream_ids = await self.keys("stream:*")
        streams = {}
        for sid in stream_ids:
            data = await self.get(sid)
            if data:
                streams[sid.replace("stream:", "")] = StreamConfig(**data)
        return streams

    async def save_stream(self, stream_id: str, config: StreamConfig):
        await self.set(f"stream:{stream_id}", config.dict())

    async def delete_stream(self, stream_id: str):
        await self.delete(f"stream:{stream_id}")
        await self.delete(f"status:{stream_id}")

    async def update_status(self, stream_id: str, status: dict):
        await self.set(f"status:{stream_id}", status, ttl=3600)

    async def get_status(self, stream_id: str) -> dict:
        return await self.get(f"status:{stream_id}") or {}

    def _start_save_worker(self):
        """حفظ دوري للذاكرة (بديل Redis)"""
        def save_loop():
            while self._running:
                time.sleep(30)
                with self._lock:
                    with open("state_backup.json", "w") as f:
                        json.dump(self._memory_store, f, indent=2)
        if not self.use_redis:
            self._save_thread = threading.Thread(target=save_loop, daemon=True)
            self._save_thread.start()

    def load_backup(self):
        if not self.use_redis and os.path.exists("state_backup.json"):
            with open("state_backup.json", "r") as f:
                self._memory_store = json.load(f)

state = StateManager()

# ============================================
# Worker Manager (يدير عمليات FFmpeg)
# ============================================
class FFmpegWorker:
    def __init__(self, max_workers=WORKER_POOL_SIZE):
        self.max_workers = max_workers
        self.active_workers = 0
        self._lock = asyncio.Lock()
        self.processes: Dict[str, asyncio.subprocess.Process] = {}

    async def start_stream(self, stream_id: str, config: StreamConfig) -> bool:
        async with self._lock:
            if self.active_workers >= self.max_workers:
                return False
            self.active_workers += 1

        try:
            cmd = self._build_ffmpeg_cmd(config)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            self.processes[stream_id] = proc
            # تحديث الحالة
            await state.update_status(stream_id, {
                "status": "running",
                "pid": proc.pid,
                "started_at": time.time(),
                "fps": 0,
                "bitrate": 0
            })
            asyncio.create_task(self._monitor_process(stream_id, proc, config))
            return True
        except Exception as e:
            async with self._lock:
                self.active_workers -= 1
            raise e

    async def stop_stream(self, stream_id: str):
        proc = self.processes.pop(stream_id, None)
        if proc and proc.returncode is None:
            proc.terminate()
            await asyncio.sleep(1)
            if proc.returncode is None:
                proc.kill()
        async with self._lock:
            self.active_workers -= 1
        await state.update_status(stream_id, {"status": "stopped", "pid": None})

    async def _monitor_process(self, stream_id: str, proc: asyncio.subprocess.Process, config: StreamConfig):
        await proc.wait()
        # إذا توقف بشكل غير متوقع، حاول إعادة التشغيل إذا كان status لا يزال running
        status = await state.get_status(stream_id)
        if status.get("status") == "running":
            # إعادة تشغيل تلقائي مع تأخير
            await asyncio.sleep(5)
            await self.start_stream(stream_id, config)
        else:
            async with self._lock:
                self.active_workers -= 1

    def _build_ffmpeg_cmd(self, config: StreamConfig) -> list:
        # تحديد معاملات الجودة
        quality_params = {
            "low":   ["-b:v", "1000k", "-maxrate", "1000k", "-bufsize", "2000k", "-r", "25"],
            "medium":["-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", "-r", "30"],
            "high":  ["-b:v", "5000k", "-maxrate", "5000k", "-bufsize", "10000k", "-r", "30"]
        }.get(config.quality, ["-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", "-r", "30"])

        cmd = [
            FFMPEG_PATH, "-re",
            "-i", config.input_url,
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            *quality_params,
            "-c:a", "aac", "-b:a", "128k",
            "-f", "flv",
            config.output_url
        ]
        if config.enable_logo and config.logo_path and os.path.exists(config.logo_path):
            cmd = [
                FFMPEG_PATH, "-re",
                "-i", config.input_url,
                "-i", config.logo_path,
                "-filter_complex", "overlay=W-w-20:20",
                "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                *quality_params,
                "-c:a", "aac", "-b:a", "128k",
                "-f", "flv",
                config.output_url
            ]
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

@app.get("/")
async def root():
    return {"status": "online", "version": "2.0"}

# ========== إدارة القنوات ==========
@app.post("/streams")
async def create_stream(config: StreamConfig, _=Depends(verify_api_key)):
    stream_id = str(uuid.uuid4())[:8]
    await state.save_stream(stream_id, config)
    await state.update_status(stream_id, {"status": "stopped"})
    return {"stream_id": stream_id, "config": config.dict()}

@app.get("/streams")
async def list_streams(_=Depends(verify_api_key)):
    streams = await state.get_all_streams()
    return {"streams": streams}

@app.get("/streams/{stream_id}")
async def get_stream(stream_id: str, _=Depends(verify_api_key)):
    config = await state.get(f"stream:{stream_id}")
    if not config:
        raise HTTPException(404, "Stream not found")
    return config

@app.put("/streams/{stream_id}")
async def update_stream(stream_id: str, config: StreamConfig, _=Depends(verify_api_key)):
    await state.save_stream(stream_id, config)
    return {"success": True}

@app.delete("/streams/{stream_id}")
async def delete_stream(stream_id: str, _=Depends(verify_api_key)):
    # إيقاف البث إذا كان يعمل
    await worker_manager.stop_stream(stream_id)
    await state.delete_stream(stream_id)
    return {"success": True}

# ========== التحكم في البث ==========
@app.post("/streams/{stream_id}/start")
async def start_stream(stream_id: str, background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    config_data = await state.get(f"stream:{stream_id}")
    if not config_data:
        raise HTTPException(404, "Stream not found")
    config = StreamConfig(**config_data)
    status = await state.get_status(stream_id)
    if status.get("status") == "running":
        raise HTTPException(400, "Stream already running")
    background_tasks.add_task(worker_manager.start_stream, stream_id, config)
    return {"message": "Stream start requested"}

@app.post("/streams/{stream_id}/stop")
async def stop_stream(stream_id: str, _=Depends(verify_api_key)):
    await worker_manager.stop_stream(stream_id)
    return {"message": "Stream stopped"}

@app.post("/streams/{stream_id}/restart")
async def restart_stream(stream_id: str, background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    await worker_manager.stop_stream(stream_id)
    config_data = await state.get(f"stream:{stream_id}")
    if not config_data:
        raise HTTPException(404, "Stream not found")
    config = StreamConfig(**config_data)
    background_tasks.add_task(worker_manager.start_stream, stream_id, config)
    return {"message": "Stream restart requested"}

@app.get("/streams/{stream_id}/status")
async def get_stream_status(stream_id: str, _=Depends(verify_api_key)):
    status = await state.get_status(stream_id)
    config = await state.get(f"stream:{stream_id}")
    if not config:
        raise HTTPException(404, "Stream not found")
    return {"stream_id": stream_id, "name": config["name"], **status}

# ========== مراقبة السيرفر ==========
@app.get("/system/stats")
async def system_stats(_=Depends(verify_api_key)):
    if not PSUTIL_AVAILABLE:
        raise HTTPException(503, "psutil not installed")
    stats = {
        "cpu": psutil.cpu_percent(interval=0.5),
        "ram": psutil.virtual_memory().percent,
        "ram_used_mb": psutil.virtual_memory().used // (1024**2),
        "ram_total_mb": psutil.virtual_memory().total // (1024**2),
        "disk": psutil.disk_usage("/").percent,
        "disk_used_gb": psutil.disk_usage("/").used // (1024**3),
        "disk_total_gb": psutil.disk_usage("/").total // (1024**3),
        "active_streams": len(worker_manager.processes),
    }
    return stats

# ============================================
# Telegram Bot (يتواصل فقط مع API)
# ============================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# كيبورد رئيسي
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📡 قائمة القنوات")],
        [KeyboardButton(text="➕ إضافة قناة"), KeyboardButton(text="❌ حذف قناة")],
        [KeyboardButton(text="📊 مراقبة السيرفر")],
        [KeyboardButton(text="🔄 إعادة تشغيل الكل"), KeyboardButton(text="⏹ إيقاف الكل")]
    ],
    resize_keyboard=True
)

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

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("🚫 غير مصرح لك.")
        return
    await message.reply(
        "🎬 *نظام إدارة البث المتقدم*\n\n"
        "البث يتم عبر FastAPI و FFmpeg مع إمكانية مراقبة كاملة.\n"
        "استخدم الأزرار أدناه لإدارة القنوات.",
        reply_markup=main_kb,
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(lambda msg: msg.text == "📡 قائمة القنوات")
async def list_channels(message: types.Message):
    res = await api_request("GET", "/streams")
    if "error" in res:
        await message.reply("❌ خطأ في الاتصال بالـ API")
        return
    streams = res.get("streams", {})
    if not streams:
        await message.reply("📭 لا توجد قنوات مسجلة.")
        return
    for sid, cfg in streams.items():
        status_res = await api_request("GET", f"/streams/{sid}/status")
        status = status_res.get("status", "unknown")
        icon = "🟢" if status == "running" else "🔴"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=f"{icon} {cfg['name']}", callback_data=f"view_{sid}"),
                InlineKeyboardButton(text="▶️", callback_data=f"start_{sid}"),
                InlineKeyboardButton(text="⏹️", callback_data=f"stop_{sid}")
            ]
        ])
        await message.answer(f"🎬 *{cfg['name']}*\n🆔 `{sid}`\n📡 الحالة: {status}",
                             reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "➕ إضافة قناة")
async def add_channel_start(message: types.Message):
    user_data[message.from_user.id] = {"step": "add_name"}
    await message.reply("📝 أرسل اسم القناة الجديدة:")

@dp.message(lambda msg: msg.text == "❌ حذف قناة")
async def delete_channel_list(message: types.Message):
    res = await api_request("GET", "/streams")
    if "error" in res:
        await message.reply("❌ خطأ في الاتصال")
        return
    streams = res.get("streams", {})
    if not streams:
        await message.reply("لا توجد قنوات")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cfg['name'], callback_data=f"del_{sid}")] for sid, cfg in streams.items()
    ])
    await message.reply("اختر القناة للحذف:", reply_markup=kb)

@dp.message(lambda msg: msg.text == "📊 مراقبة السيرفر")
async def server_stats(message: types.Message):
    res = await api_request("GET", "/system/stats")
    if "error" in res:
        await message.reply("❌ لا يمكن جلب الإحصائيات")
        return
    stats = res
    text = (
        "🖥️ *مراقبة الخادم*\n\n"
        f"📡 البثوث النشطة: `{stats.get('active_streams',0)}`\n"
        f"🖥️ CPU: `{stats.get('cpu',0)}%`\n"
        f"🧠 RAM: `{stats.get('ram',0)}%` ({stats.get('ram_used_mb',0)}/{stats.get('ram_total_mb',0)} MB)\n"
        f"💾 Disk: `{stats.get('disk',0)}%` ({stats.get('disk_used_gb',0)}/{stats.get('disk_total_gb',0)} GB)"
    )
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "🔄 إعادة تشغيل الكل")
async def restart_all(message: types.Message):
    res = await api_request("GET", "/streams")
    streams = res.get("streams", {})
    for sid in streams.keys():
        await api_request("POST", f"/streams/{sid}/restart")
    await message.reply("✅ تم طلب إعادة تشغيل جميع البثوث")

@dp.message(lambda msg: msg.text == "⏹ إيقاف الكل")
async def stop_all(message: types.Message):
    res = await api_request("GET", "/streams")
    streams = res.get("streams", {})
    for sid in streams.keys():
        await api_request("POST", f"/streams/{sid}/stop")
    await message.reply("✅ تم طلب إيقاف جميع البثوث")

# ========== معالجات الـ Callback ==========
@dp.callback_query(lambda c: c.data.startswith("view_"))
async def view_stream(callback: types.CallbackQuery):
    sid = callback.data.split("_")[1]
    status_res = await api_request("GET", f"/streams/{sid}/status")
    status = status_res.get("status", "unknown")
    config_res = await api_request("GET", f"/streams/{sid}")
    config = config_res.get("name", sid)
    text = f"🎬 *{config}*\n🆔 `{sid}`\n📡 الحالة: {status}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"start_{sid}"),
         InlineKeyboardButton(text="⏹️ إيقاف", callback_data=f"stop_{sid}")],
        [InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{sid}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("start_"))
async def start_stream_cb(callback: types.CallbackQuery):
    sid = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{sid}/start")
    await callback.answer("✅ تم طلب التشغيل", show_alert=True)
@dp.callback_query(lambda c: c.data.startswith("stop_"))
async def stop_stream_cb(callback: types.CallbackQuery):
    sid = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{sid}/stop")
    await callback.answer("⏹️ تم طلب الإيقاف", show_alert=True)
@dp.callback_query(lambda c: c.data.startswith("restart_"))
async def restart_stream_cb(callback: types.CallbackQuery):
    sid = callback.data.split("_")[1]
    await api_request("POST", f"/streams/{sid}/restart")
    await callback.answer("🔄 تم طلب إعادة التشغيل", show_alert=True)
@dp.callback_query(lambda c: c.data.startswith("del_"))
async def delete_stream_cb(callback: types.CallbackQuery):
    sid = callback.data.split("_")[1]
    await api_request("DELETE", f"/streams/{sid}")
    await callback.message.edit_text("🗑 تم حذف القناة")
    await callback.answer()

# ========== مراحل إضافة قناة (في البوت) ==========
user_data = {}
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_name")
async def process_add_name(message: types.Message):
    name = message.text
    user_data[message.from_user.id] = {"step": "add_input", "name": name}
    await message.reply("📥 أرسل رابط المصدر (URL):")
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_input")
async def process_add_input(message: types.Message):
    input_url = message.text
    user_data[message.from_user.id]["input"] = input_url
    user_data[message.from_user.id]["step"] = "add_output"
    await message.reply("📤 أرسل رابط الإخراج (RTMP أو HLS):")
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_output")
async def process_add_output(message: types.Message):
    output_url = message.text
    data = user_data[message.from_user.id]
    config = {
        "name": data["name"],
        "input_url": data["input"],
        "output_url": output_url,
        "quality": "medium",
        "enable_logo": False,
        "logo_path": None,
        "fallback_url": None,
        "schedule": None
    }
    res = await api_request("POST", "/streams", config)
    if "stream_id" in res:
        await message.reply(f"✅ تم إنشاء القناة `{data['name']}` بنجاح\n🆔 المعرف: `{res['stream_id']}`", parse_mode=ParseMode.MARKDOWN)
    else:
        await message.reply("❌ فشل في إنشاء القناة")
    del user_data[message.from_user.id]

# ============================================
# تشغيل الخدمات
# ============================================
async def main():
    # الاتصال بـ Redis إذا كان متاحاً
    await state.connect_redis()
    # تحميل نسخة احتياطية إذا كانت بدون Redis
    state.load_backup()
    # بدء FastAPI في مهمة خلفية
    config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    api_task = asyncio.create_task(server.serve())
    # بدء البوت
    bot_task = asyncio.create_task(dp.start_polling(bot))
    print("🚀 النظام يعمل - API على http://{}:{}".format(API_HOST, API_PORT))
    await asyncio.gather(api_task, bot_task)

if __name__ == "__main__":
    # التأكد من وجود FFmpeg
    if os.system("ffmpeg -version > /dev/null 2>&1") != 0:
        print("خطأ: ffmpeg غير مثبت")
        exit(1)
    asyncio.run(main())