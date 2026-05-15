
import asyncio
import json
import os
import re
import time
import logging
import signal
import threading
from typing import Dict, Optional, List
from contextlib import asynccontextmanager

import aiohttp
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode

# ========== الإعدادات الأساسية ==========
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
API_HOST = "0.0.0.0"
API_PORT = 8000

REDIS_HOST = "localhost"
REDIS_PASSWORD = None
REDIS_DB = 0

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== نماذج البيانات ==========
class Channel(BaseModel):
    name: str
    slug: str
    input_url: str
    output_url: str        # RTMP للمخرج (قد يهمل لـ HLS)
    type: str = "rtmp"     # rtmp / hls
    quality: str = "medium"
    logo_url: Optional[str] = None
    enabled: bool = True
    schedule: Optional[str] = None   # cron expression

class ChannelStatus(BaseModel):
    status: str  # stopped, starting, running, error
    pid: Optional[int] = None
    started_at: Optional[float] = None
    fps: float = 0.0
    bitrate: int = 0

# ========== قاعدة بيانات مؤقتة (SQLite) ==========
import sqlite3
from contextlib import contextmanager

DB_PATH = "orchestrator.db"

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                slug TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                input_url TEXT NOT NULL,
                output_url TEXT,
                type TEXT NOT NULL,
                quality TEXT NOT NULL,
                logo_url TEXT,
                enabled INTEGER DEFAULT 1,
                schedule TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_status (
                slug TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                pid INTEGER,
                started_at REAL,
                fps REAL DEFAULT 0,
                bitrate INTEGER DEFAULT 0
            )
        """)
        conn.commit()

init_db()

# ========== Redis الاتصال ==========
redis_client = None

async def get_redis():
    global redis_client
    if redis_client is None:
        redis_client = await redis.from_url(f"redis://{REDIS_HOST}:6379/{REDIS_DB}", decode_responses=True)
    return redis_client

# ========== Worker Manager (يدير عمليات FFmpeg) ==========
class WorkerManager:
    def __init__(self):
        self.processes: Dict[str, asyncio.subprocess.Process] = {}
        self._lock = asyncio.Lock()
        self._running = True

    async def start_worker(self, slug: str, channel: Channel):
        """تشغيل عملية FFmpeg بناءً على إعدادات القناة"""
        if slug in self.processes and self.processes[slug].returncode is None:
            return False

        cmd = self._build_ffmpeg_cmd(slug, channel)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        self.processes[slug] = proc
        # تحديث حالة التشغيل في قاعدة البيانات
        with get_db() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO channel_status (slug, status, pid, started_at)
                VALUES (?, 'running', ?, ?)
            """, (slug, proc.pid, time.time()))
            conn.commit()
        # مراقبة العملية في الخلفية
        asyncio.create_task(self._monitor(slug, proc, channel))
        return True

    async def stop_worker(self, slug: str):
        if slug not in self.processes:
            return False
        proc = self.processes.pop(slug)
        if proc.returncode is None:
            proc.terminate()
            await asyncio.sleep(1)
            if proc.returncode is None:
                proc.kill()
        with get_db() as conn:
            conn.execute("UPDATE channel_status SET status='stopped', pid=NULL, started_at=NULL WHERE slug=?", (slug,))
            conn.commit()
        return True

    async def _monitor(self, slug: str, proc: asyncio.subprocess.Process, channel: Channel):
        await proc.wait()
        # العملية انتهت – إعادة تشغيل تلقائي إذا كانت القناة مفعلة
        with get_db() as conn:
            row = conn.execute("SELECT enabled FROM channels WHERE slug=?", (slug,)).fetchone()
            enabled = row['enabled'] if row else True
        if enabled:
            logger.warning(f"البث {slug} توقف فجأة، إعادة تشغيل تلقائي...")
            await asyncio.sleep(3)
            await self.start_worker(slug, channel)
        else:
            with get_db() as conn:
                conn.execute("UPDATE channel_status SET status='stopped' WHERE slug=?", (slug,))
                conn.commit()

    def _build_ffmpeg_cmd(self, slug: str, channel: Channel) -> list:
        quality_map = {
            "low": ["-b:v", "1000k", "-maxrate", "1000k", "-bufsize", "2000k", "-r", "25"],
            "medium": ["-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", "-r", "30"],
            "high": ["-b:v", "5000k", "-maxrate", "5000k", "-bufsize", "10000k", "-r", "30"]
        }
        q = quality_map.get(channel.quality, quality_map["medium"])
        cmd = [
            "ffmpeg", "-re",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
            "-timeout", "10000000", "-rw_timeout", "10000000",
            "-fflags", "+genpts+discardcorrupt", "-analyzeduration", "5000000", "-probesize", "50000000",
            "-i", channel.input_url
        ]
        if channel.logo_url:
            cmd.extend(["-i", channel.logo_url, "-filter_complex", "[1:v]scale=iw:ih[logo];[0:v][logo]overlay=0:0"])
        cmd.extend([
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency", *q,
            "-c:a", "aac", "-b:a", "128k"
        ])
        if channel.type == "hls":
            hls_dir = f"/tmp/hls/{slug}"
            os.makedirs(hls_dir, exist_ok=True)
            out_file = f"{hls_dir}/index.m3u8"
            cmd.extend(["-f", "hls", "-hls_time", "2", "-hls_list_size", "5",
                        "-hls_flags", "delete_segments+append_list", "-y", out_file])
        else:
            cmd.extend(["-f", "flv", channel.output_url])
        return cmd

worker_manager = WorkerManager()

# ========== FastAPI ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء Redis
    await get_redis()
    # استعادة العمليات الموجودة من قاعدة البيانات
    with get_db() as conn:
        rows = conn.execute("SELECT slug, status, pid FROM channel_status WHERE status='running'").fetchall()
        for row in rows:
            # لا نستطيع استعادة العملية، نعتبرها متوقفة
            conn.execute("UPDATE channel_status SET status='stopped' WHERE slug=?", (row['slug'],))
        conn.commit()
    yield
    # إيقاف جميع العمليات
    for slug in list(worker_manager.processes.keys()):
        await worker_manager.stop_worker(slug)
    await redis_client.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ========== API Endpoints ==========
@app.post("/channels")
async def create_channel(channel: Channel, background_tasks: BackgroundTasks):
    with get_db() as conn:
        existing = conn.execute("SELECT slug FROM channels WHERE slug=?", (channel.slug,)).fetchone()
        if existing:
            raise HTTPException(400, "Slug already exists")
        conn.execute("""
            INSERT INTO channels (slug, name, input_url, output_url, type, quality, logo_url, enabled, schedule)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (channel.slug, channel.name, channel.input_url, channel.output_url, channel.type, channel.quality, channel.logo_url, int(channel.enabled), channel.schedule))
        conn.execute("INSERT OR REPLACE INTO channel_status (slug, status) VALUES (?, 'stopped')", (channel.slug,))
        conn.commit()
    # إرسال أمر إلى Redis (يمكن استخدامه لبدء القناة إذا كانت مفعلة)
    r = await get_redis()
    await r.publish("channel:created", channel.slug)
    return {"slug": channel.slug}

@app.get("/channels")
async def list_channels():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM channels").fetchall()
        return [dict(row) for row in rows]

@app.get("/channels/{slug}")
async def get_channel(slug: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM channels WHERE slug=?", (slug,)).fetchone()
        if not row:
            raise HTTPException(404, "Channel not found")
        return dict(row)

@app.put("/channels/{slug}")
async def update_channel(slug: str, channel: Channel, background_tasks: BackgroundTasks):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM channels WHERE slug=?", (slug,)).fetchone()
        if not row:
            raise HTTPException(404, "Channel not found")
        # تحديث الحقول
        conn.execute("""
            UPDATE channels SET name=?, input_url=?, output_url=?, type=?, quality=?, logo_url=?, enabled=?, schedule=?
            WHERE slug=?
        """, (channel.name, channel.input_url, channel.output_url, channel.type, channel.quality, channel.logo_url, int(channel.enabled), channel.schedule, slug))
        conn.commit()
    # إذا تم تغيير المصدر وكانت القناة تعمل، إعادة تشغيلها
    status = await get_channel_status(slug)
    if status["status"] == "running":
        await restart_channel(slug, background_tasks)
    return {"success": True}

@app.delete("/channels/{slug}")
async def delete_channel(slug: str):
    await worker_manager.stop_worker(slug)
    with get_db() as conn:
        conn.execute("DELETE FROM channels WHERE slug=?", (slug,))
        conn.execute("DELETE FROM channel_status WHERE slug=?", (slug,))
        conn.commit()
    return {"success": True}

@app.post("/channels/{slug}/start")
async def start_channel(slug: str, background_tasks: BackgroundTasks):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM channels WHERE slug=?", (slug,)).fetchone()
        if not row:
            raise HTTPException(404, "Channel not found")
        channel = Channel(**dict(row))
    background_tasks.add_task(worker_manager.start_worker, slug, channel)
    return {"message": "Start requested"}

@app.post("/channels/{slug}/stop")
async def stop_channel(slug: str):
    await worker_manager.stop_worker(slug)
    return {"message": "Stopped"}

@app.post("/channels/{slug}/restart")
async def restart_channel(slug: str, background_tasks: BackgroundTasks):
    await worker_manager.stop_worker(slug)
    with get_db() as conn:
        row = conn.execute("SELECT * FROM channels WHERE slug=?", (slug,)).fetchone()
        if not row:
            raise HTTPException(404, "Channel not found")
        channel = Channel(**dict(row))
    background_tasks.add_task(worker_manager.start_worker, slug, channel)
    return {"message": "Restart requested"}

@app.get("/channels/{slug}/status")
async def get_channel_status(slug: str):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM channel_status WHERE slug=?", (slug,)).fetchone()
        if not row:
            return {"status": "unknown"}
        return dict(row)

@app.get("/system/stats")
async def system_stats():
    try:
        import psutil
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        active = len(worker_manager.processes)
        total = 0
        with get_db() as conn:
            total = conn.execute("SELECT COUNT(*) as c FROM channels").fetchone()["c"]
        return {
            "cpu": cpu,
            "ram_percent": mem.percent,
            "ram_used_mb": mem.used // (1024**2),
            "ram_total_mb": mem.total // (1024**2),
            "disk_percent": disk.percent,
            "disk_used_gb": disk.used // (1024**3),
            "disk_total_gb": disk.total // (1024**3),
            "active_streams": active,
            "total_streams": total
        }
    except ImportError:
        return {"error": "psutil not installed"}

# ========== Telegram Bot ==========
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📡 قائمة القنوات")],
        [KeyboardButton(text="➕ إضافة قناة"), KeyboardButton(text="❌ حذف قناة")],
        [KeyboardButton(text="📊 مراقبة السيرفر")]
    ],
    resize_keyboard=True
)

async def api_request(method: str, endpoint: str, json_data: dict = None):
    url = f"http://{API_HOST}:{API_PORT}{endpoint}"
    try:
        async with aiohttp.ClientSession() as session:
            if method == "GET":
                async with session.get(url) as resp:
                    return await resp.json()
            elif method == "POST":
                async with session.post(url, json=json_data) as resp:
                    return await resp.json()
            elif method == "PUT":
                async with session.put(url, json=json_data) as resp:
                    return await resp.json()
            elif method == "DELETE":
                async with session.delete(url) as resp:
                    return await resp.json()
    except Exception as e:
        return {"error": str(e)}

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("غير مصرح")
    await message.reply("🎬 Media Orchestrator\nاستخدم الأزرار أدناه.", reply_markup=main_kb)

@dp.message(lambda msg: msg.text == "📡 قائمة القنوات")
async def list_channels_bot(message: types.Message):
    res = await api_request("GET", "/channels")
    if "error" in res:
        await message.reply(f"خطأ: {res['error']}")
        return
    channels = res
    if not channels:
        await message.reply("لا توجد قنوات.")
        return
    for ch in channels:
        slug = ch["slug"]
        status_res = await api_request("GET", f"/channels/{slug}/status")
        status = status_res.get("status", "stopped")
        icon = "🟢" if status == "running" else "🔴"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text=f"{icon} {ch['name']}", callback_data=f"view_{slug}"),
                InlineKeyboardButton(text="▶️", callback_data=f"start_{slug}"),
                InlineKeyboardButton(text="⏹️", callback_data=f"stop_{slug}")
            ],
            [
                InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
                InlineKeyboardButton(text="✏️ تعديل", callback_data=f"edit_{slug}"),
                InlineKeyboardButton(text="📊 تحديث", callback_data=f"refresh_{slug}")
            ]
        ])
        await message.answer(f"🎬 *{ch['name']}*\n📛 `{slug}`\n📡 الحالة: {status}", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "➕ إضافة قناة")
async def add_start(message: types.Message):
    user_data[message.from_user.id] = {"step": "name"}
    await message.reply("📝 أرسل اسم القناة (سيُستخدم المعرف في الرابط):")

@dp.message(lambda msg: msg.text == "❌ حذف قناة")
async def delete_list(message: types.Message):
    res = await api_request("GET", "/channels")
    if "error" in res:
        await message.reply("خطأ في جلب البيانات")
        return
    channels = res
    if not channels:
        await message.reply("لا توجد قنوات.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=ch["name"], callback_data=f"del_{ch['slug']}")] for ch in channels
    ])
    await message.reply("اختر القناة لحذفها:", reply_markup=kb)

@dp.message(lambda msg: msg.text == "📊 مراقبة السيرفر")
async def stats_bot(message: types.Message):
    res = await api_request("GET", "/system/stats")
    if "error" in res:
        await message.reply(f"خطأ: {res['error']}")
        return
    s = res
    text = (
        f"🖥️ *مراقبة الخادم*\n"
        f"CPU: {s.get('cpu')}%\n"
        f"RAM: {s.get('ram_percent')}% ({s.get('ram_used_mb')}/{s.get('ram_total_mb')} MB)\n"
        f"Disk: {s.get('disk_percent')}% ({s.get('disk_used_gb')}/{s.get('disk_total_gb')} GB)\n"
        f"البثوث النشطة: {s.get('active_streams')} / {s.get('total_streams')}"
    )
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

# ========== بيانات المستخدم المؤقتة للإضافة ==========
user_data = {}
async def finish_create_channel(message: types.Message, data: dict):
    slug = data["slug"]
    config = {
        "name": data["name"],
        "slug": slug,
        "input_url": data["input"],
        "output_url": data.get("output", ""),
        "type": data["type"],
        "quality": "medium",
        "logo_url": None,
        "enabled": True,
        "schedule": None
    }
    res = await api_request("POST", "/channels", json_data=config)
    if "slug" in res:
        msg = f"✅ تم إنشاء القناة `{data['name']}`\n📛 المعرف: `{slug}`\n"
        if data["type"] == "hls":
            msg += "🔗 رابط HLS: http://164.68.102.28/live/{slug}/index.m3u8 (تأكد من إعداد nginx)\n"
        await message.reply(msg, parse_mode=ParseMode.MARKDOWN)
    else:
        await message.reply("❌ فشل الإنشاء: " + str(res))

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "name")
async def name_step(message: types.Message):
    name = message.text.strip()
    slug = re.sub(r'[^a-z0-9]', '-', name.lower()).strip('-')
    user_data[message.from_user.id] = {"step": "type", "name": name, "slug": slug}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📺 HLS", callback_data="type_hls"),
         InlineKeyboardButton(text="📡 RTMP", callback_data="type_rtmp")]
    ])
    await message.reply("اختر نوع البث:", reply_markup=kb)

@dp.callback_query(lambda c: c.data in ["type_hls", "type_rtmp"])
async def type_callback(callback: types.CallbackQuery):
    uid = callback.from_user.id
    data = user_data.get(uid)
    if not data:
        await callback.answer("انتهت الجلسة")
        return
    data["type"] = "hls" if callback.data == "type_hls" else "rtmp"
    data["step"] = "input"
    await callback.message.reply("📥 أرسل رابط المصدر (URL أو مسار محلي):")
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "input")
async def input_step(message: types.Message):
    uid = message.from_user.id
    data = user_data[uid]
    data["input"] = message.text.strip()
    if data["type"] == "rtmp":
        data["step"] = "output"
        await message.reply("📤 أرسل رابط الإخراج RTMP (rtmp://...):")
    else:
        await finish_create_channel(message, data)
        del user_data[uid]

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "output")
async def output_step(message: types.Message):
    uid = message.from_user.id
    data = user_data[uid]
    data["output"] = message.text.strip()
    await finish_create_channel(message, data)
    del user_data[uid]

# ========== أزرار التحكم ==========
@dp.callback_query(lambda c: c.data.startswith(("start_", "stop_", "restart_", "refresh_", "edit_", "del_")))
async def control_callback(callback: types.CallbackQuery):
    action, slug = callback.data.split("_", 1)
    if action == "start":
        res = await api_request("POST", f"/channels/{slug}/start")
        await callback.answer(res.get("message", "تم التشغيل"))
    elif action == "stop":
        res = await api_request("POST", f"/channels/{slug}/stop")
        await callback.answer(res.get("message", "تم الإيقاف"))
    elif action == "restart":
        res = await api_request("POST", f"/channels/{slug}/restart")
        await callback.answer(res.get("message", "تم إعادة التشغيل"))
    elif action == "refresh":
        status_res = await api_request("GET", f"/channels/{slug}/status")
        status = status_res.get("status", "unknown")
        await callback.answer(f"الحالة: {status}")
        return
    elif action == "edit":
        # يمكن إضافة تعديل لاحقاً
        await callback.answer("سيتم إضافة التعديل قريباً")
        return
    elif action == "del":
        await api_request("DELETE", f"/channels/{slug}")
        await callback.answer("تم الحذف")
        await callback.message.edit_text("🗑 تم حذف القناة.")
        return
    # تحديث عرض القناة بعد التغيير
    ch_res = await api_request("GET", f"/channels/{slug}")
    if "error" not in ch_res:
        status_res = await api_request("GET", f"/channels/{slug}/status")
        status = status_res.get("status", "stopped")
        icon = "🟢" if status == "running" else "🔴"
        text = f"🎬 *{ch_res['name']}*\n📛 `{slug}`\n📡 الحالة: {status}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{icon} {ch_res['name']}", callback_data=f"view_{slug}"),
             InlineKeyboardButton(text="▶️", callback_data=f"start_{slug}"),
             InlineKeyboardButton(text="⏹️", callback_data=f"stop_{slug}")],
            [InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
             InlineKeyboardButton(text="✏️ تعديل", callback_data=f"edit_{slug}"),
             InlineKeyboardButton(text="📊 تحديث", callback_data=f"refresh_{slug}")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(lambda c: c.data.startswith("view_"))
async def view_callback(callback: types.CallbackQuery):
    slug = callback.data.split("_")[1]
    ch = await api_request("GET", f"/channels/{slug}")
    stat = await api_request("GET", f"/channels/{slug}/status")
    if "error" in ch:
        await callback.answer("غير موجود")
        return
    text = (
        f"🎬 *{ch['name']}*\n📛 `{slug}`\n"
        f"📡 الحالة: {stat.get('status')}\n"
        f"🎬 FPS: {stat.get('fps', 0)}\n"
        f"📡 Bitrate: {stat.get('bitrate', 0)} kbps\n"
        f"⏱️ المدة: ...\n"  # يمكن حسابها من started_at
        f"🆔 PID: {stat.get('pid', '—')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"start_{slug}"),
         InlineKeyboardButton(text="⏹️ إيقاف", callback_data=f"stop_{slug}")],
        [InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
         InlineKeyboardButton(text="✏️ تعديل", callback_data=f"edit_{slug}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# ========== تشغيل الخدمات ==========
async def run_api():
    config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

async def shutdown(sig):
    logger.info(f"إشارة {sig.name}, إيقاف نظيف...")
    for slug in list(worker_manager.processes.keys()):
        await worker_manager.stop_worker(slug)
    if redis_client:
        await redis_client.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    # بدء FastAPI و Telegram معاً
    api_task = asyncio.create_task(run_api())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    logger.info("🚀 Media Orchestrator يعمل - API على http://0.0.0.0:8000")
    await asyncio.gather(api_task, bot_task)

if __name__ == "__main__":
    asyncio.run(main())