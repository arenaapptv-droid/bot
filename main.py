
import asyncio
import json
import os
import re
import time
import logging
import subprocess
import threading
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import uvicorn
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode

# ========== الإعدادات ==========
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
HLS_BASE_URL = "http://164.68.102.28"
HLS_DIR = "/tmp/hls"
os.makedirs(HLS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== تخزين البيانات ==========
streams: Dict[str, dict] = {}
processes: Dict[str, subprocess.Popen] = {}
stream_stats: Dict[str, dict] = {}  # fps, bitrate, last_update
user_steps: Dict[int, dict] = {}

DATA_FILE = "streams_data.json"

def save_data():
    data = {}
    for sid, cfg in streams.items():
        data[sid] = {k: v for k, v in cfg.items() if k not in ['proc']}
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def load_data():
    global streams
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            streams = json.load(f)
        for sid, cfg in streams.items():
            cfg.setdefault("status", "stopped")
            cfg.setdefault("started_at", None)
            cfg.setdefault("logo_url", "")
    else:
        streams = {}

load_data()

# ========== FastAPI لخدمة HLS ==========
app = FastAPI()
@app.get("/live/{slug}/{file:path}")
async def serve_hls(slug: str, file: str):
    path = os.path.join(HLS_DIR, slug, file)
    if not os.path.exists(path):
        raise HTTPException(404)
    return FileResponse(path)

# ========== دوال إدارة البث ==========
def slugify(name: str) -> str:
    s = re.sub(r'[^a-zA-Z0-9\u0600-\u06FF\s-]', '', name)
    s = re.sub(r'\s+', '-', s.strip())
    return s.lower()

def build_ffmpeg_cmd(slug: str, cfg: dict) -> list:
    cmd = [
        "ffmpeg", "-re",
        "-i", cfg["input"],
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
        "-c:a", "aac", "-b:a", "128k",
        "-progress", "pipe:1", "-nostats"
    ]
    # إضافة الشعار (تغطية كاملة)
    if cfg.get("logo_url"):
        cmd.extend(["-i", cfg["logo_url"], "-filter_complex", "[1:v]scale=iw:ih[logo];[0:v][logo]overlay=0:0"])
    # تحديد المخرج
    if cfg["type"] == "hls":
        out_dir = os.path.join(HLS_DIR, slug)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, "index.m3u8")
        cmd.extend(["-f", "hls", "-hls_time", "2", "-hls_list_size", "5",
                    "-hls_flags", "delete_segments+append_list", "-y", out_file])
    else:
        cmd.extend(["-f", "flv", cfg["output"]])
    return cmd

def start_stream(slug: str):
    if slug in processes and processes[slug].poll() is None:
        return False
    cfg = streams[slug]
    cmd = build_ffmpeg_cmd(slug, cfg)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    processes[slug] = proc
    streams[slug]["status"] = "running"
    streams[slug]["started_at"] = time.time()
    save_data()
    
    def monitor():
        for line in proc.stdout:
            fps_match = re.search(r'fps=([\d.]+)', line)
            bitrate_match = re.search(r'bitrate=([\d.]+)kbits', line)
            if fps_match:
                stream_stats[slug] = {
                    "fps": float(fps_match.group(1)),
                    "bitrate": int(float(bitrate_match.group(1))) if bitrate_match else 0,
                    "updated": time.time()
                }
        # إذا انتهت العملية وكانت الحالة "running" نعيد التشغيل
        if streams.get(slug, {}).get("status") == "running":
            logger.warning(f"البث {slug} توقف، إعادة تشغيل...")
            time.sleep(3)
            start_stream(slug)
    
    threading.Thread(target=monitor, daemon=True).start()
    return True

def stop_stream(slug: str):
    if slug in processes:
        proc = processes.pop(slug)
        if proc.poll() is None:
            proc.terminate()
            time.sleep(0.5)
            if proc.poll() is None:
                proc.kill()
    streams[slug]["status"] = "stopped"
    streams[slug]["started_at"] = None
    save_data()

def restart_stream(slug: str):
    stop_stream(slug)
    time.sleep(1)
    start_stream(slug)

def get_status_text(slug: str) -> str:
    cfg = streams[slug]
    status = cfg.get("status", "stopped")
    uptime = ""
    if status == "running" and cfg.get("started_at"):
        uptime = time.strftime("%H:%M:%S", time.gmtime(time.time() - cfg["started_at"]))
    stats = stream_stats.get(slug, {})
    fps = stats.get("fps", 0)
    bitrate = stats.get("bitrate", 0)
    status_icon = "🟢 يعمل" if status == "running" else "🔴 متوقف"
    hls_link = f"\n🔗 {HLS_BASE_URL}/live/{slug}/index.m3u8" if cfg["type"] == "hls" else ""
    return (
        f"🎬 *{cfg['name']}*\n"
        f"📛 المعرف: `{slug}`\n"
        f"📡 الحالة: {status_icon}\n"
        f"🎬 FPS: {fps}\n"
        f"📡 Bitrate: {bitrate} kbps\n"
        f"⏱️ مدة التشغيل: {uptime}\n"
        f"🆔 PID: {processes[slug].pid if slug in processes and processes[slug].poll() is None else '—'}{hls_link}"
    )

def get_keyboard(slug: str) -> InlineKeyboardMarkup:
    is_running = streams[slug].get("status") == "running"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ تشغيل" if not is_running else "⏹️ إيقاف", callback_data=f"toggle_{slug}"),
            InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{slug}"),
        ],
        [
            InlineKeyboardButton(text="🖼️ شعار (رابط)", callback_data=f"logo_{slug}"),
            InlineKeyboardButton(text="📊 تحديث الحالة", callback_data=f"refresh_{slug}")
        ]
    ])

# ========== بوت تلجرام ==========
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

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("غير مصرح")
    await message.reply("🎬 أهلاً بك في بوت البث المباشر.\nاستخدم الأزرار أدناه.", reply_markup=main_kb)

@dp.message(lambda msg: msg.text == "📡 قائمة القنوات")
async def list_channels(message: types.Message):
    if not streams:
        await message.reply("لا توجد قنوات. استخدم ➕ إضافة قناة.")
        return
    for slug in streams:
        text = get_status_text(slug)
        kb = get_keyboard(slug)
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "➕ إضافة قناة")
async def add_start(message: types.Message):
    user_steps[message.from_user.id] = {"step": "name"}
    await message.reply("📝 أرسل اسم القناة (سيُستخدم في رابط HLS):")

@dp.message(lambda msg: msg.text == "❌ حذف قناة")
async def delete_list(message: types.Message):
    if not streams:
        await message.reply("لا توجد قنوات.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cfg["name"], callback_data=f"del_{slug}")] for slug, cfg in streams.items()
    ])
    await message.reply("اختر القناة لحذفها:", reply_markup=kb)

@dp.message(lambda msg: msg.text == "📊 مراقبة السيرفر")
async def server_stats(message: types.Message):
    try:
        import psutil
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        active = sum(1 for s in streams.values() if s.get("status") == "running")
        text = (
            f"🖥️ *مراقبة الخادم*\n"
            f"CPU: {cpu}%\n"
            f"RAM: {mem.percent}% ({mem.used//1024**2}/{mem.total//1024**2} MB)\n"
            f"Disk: {disk.percent}% ({disk.used//1024**3}/{disk.total//1024**3} GB)\n"
            f"البثوث النشطة: {active}"
        )
    except ImportError:
        text = "⚠️ ثبّت psutil: pip install psutil"
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

# إضافة قناة - الخطوات
@dp.message(lambda msg: msg.from_user.id in user_steps and user_steps[msg.from_user.id].get("step") == "name")
async def step_name(message: types.Message):
    name = message.text.strip()
    slug = slugify(name)
    if slug in streams:
        await message.reply("⚠️ هذا الاسم موجود. اختر اسماً آخر.")
        return
    user_steps[message.from_user.id] = {"step": "type", "name": name, "slug": slug}
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📺 HLS", callback_data="type_hls"),
         InlineKeyboardButton(text="📡 RTMP", callback_data="type_rtmp")]
    ])
    await message.reply("اختر نوع البث:", reply_markup=kb)

@dp.callback_query(lambda c: c.data in ["type_hls", "type_rtmp"])
async def step_type(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_steps:
        await callback.answer("انتهت الجلسة.")
        return
    data = user_steps[uid]
    data["type"] = "hls" if callback.data == "type_hls" else "rtmp"
    data["step"] = "input"
    await callback.message.reply("📥 أرسل رابط المصدر (URL أو مسار ملف):")
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_steps and user_steps[msg.from_user.id].get("step") == "input")
async def step_input(message: types.Message):
    uid = message.from_user.id
    data = user_steps[uid]
    data["input"] = message.text.strip()
    if data["type"] == "rtmp":
        data["step"] = "output"
        await message.reply("📤 أرسل رابط إخراج RTMP (rtmp://...):")
    else:
        await finish_create(message)

@dp.message(lambda msg: msg.from_user.id in user_steps and user_steps[msg.from_user.id].get("step") == "output")
async def step_output(message: types.Message):
    uid = message.from_user.id
    data = user_steps[uid]
    data["output"] = message.text.strip()
    await finish_create(message)

async def finish_create(message: types.Message):
    uid = message.from_user.id
    data = user_steps.pop(uid)
    slug = data["slug"]
    streams[slug] = {
        "name": data["name"],
        "type": data["type"],
        "input": data["input"],
        "output": data.get("output", ""),
        "logo_url": "",
        "status": "stopped",
        "started_at": None
    }
    save_data()
    msg = f"✅ تم إنشاء القناة `{data['name']}`\n📛 المعرف: `{slug}`\n"
    if data["type"] == "hls":
        msg += f"🔗 رابط HLS: {HLS_BASE_URL}/live/{slug}/index.m3u8\n"
    msg += "يمكنك الآن تشغيلها وإضافة شعار."
    await message.reply(msg, parse_mode=ParseMode.MARKDOWN)

# أزرار التحكم
@dp.callback_query(lambda c: c.data.startswith(("toggle_", "restart_", "refresh_", "logo_", "del_")))
async def control(callback: types.CallbackQuery):
    action, slug = callback.data.split("_", 1)
    if action == "toggle":
        if streams[slug].get("status") == "running":
            stop_stream(slug)
            await callback.answer("⏹️ تم الإيقاف")
        else:
            start_stream(slug)
            await callback.answer("▶️ تم التشغيل")
    elif action == "restart":
        restart_stream(slug)
        await callback.answer("🔄 إعادة تشغيل...")
    elif action == "refresh":
        text = get_status_text(slug)
        kb = get_keyboard(slug)
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await callback.answer("تم تحديث الحالة")
        return
    elif action == "logo":
        user_steps[callback.from_user.id] = {"step": "logo", "slug": slug}
        await callback.message.reply("🖼️ أرسل رابط الصورة (http://...) للتغطية الكاملة (16:9):")
        await callback.answer()
        return
    elif action == "del":
        stop_stream(slug)
        del streams[slug]
        save_data()
        await callback.message.edit_text("🗑 تم حذف القناة.")
        await callback.answer()
        return
    # تحديث الرسالة بعد أي تغيير في الحالة
    text = get_status_text(slug)
    kb = get_keyboard(slug)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.message(lambda msg: msg.from_user.id in user_steps and user_steps[msg.from_user.id].get("step") == "logo")
async def set_logo(message: types.Message):
    uid = message.from_user.id
    data = user_steps.pop(uid)
    slug = data["slug"]
    url = message.text.strip()
    if url.lower() == "none":
        streams[slug]["logo_url"] = ""
    else:
        if not (url.startswith("http://") or url.startswith("https://")):
            await message.reply("❌ الرابط غير صالح. يجب أن يبدأ بـ http:// أو https://")
            return
        streams[slug]["logo_url"] = url
    save_data()
    if streams[slug].get("status") == "running":
        restart_stream(slug)
    await message.reply("✅ تم تحديث الشعار (سيتم إعادة تشغيل البث إن كان يعمل).")

# ========== التشغيل ==========
async def run_api():
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    if os.system("ffmpeg -version > /dev/null 2>&1") != 0:
        logger.error("ffmpeg غير مثبت")
        return
    asyncio.create_task(run_api())
    logger.info("🚀 البوت يعمل...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
