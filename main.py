#!/usr/bin/env python3
"""
نظام بث متكامل - Telegram Bot + FastAPI + Stream Manager + FFmpeg
المتطلبات: pip install fastapi uvicorn aiogram aiohttp python-multipart
"""

import asyncio
import json
import os
import signal
import time
import logging
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Depends, Header
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
import uvicorn
import aiohttp

# ═══════════════════════════════════════════
# الإعدادات (تم تغيير المنفذ إلى 8080)
# ═══════════════════════════════════════════
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
API_HOST = "127.0.0.1"
API_PORT = 8080                     # ← تغيير المنفذ إلى 8080
API_KEY = "CHANGE_THIS_SECRET_KEY"  # يفضل تغييره لاحقاً

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════
# قاعدة البيانات (حفظ في ملف JSON)
# ═══════════════════════════════════════════
DB_FILE = "streams.json"

def normalize_stream(stream_data: dict) -> dict:
    """تأكد من وجود جميع المفاتيح الأساسية لكل بث"""
    defaults = {
        "name": "",
        "input": "",
        "output": "",
        "logo": "",
        "status": "stopped",
        "started_at": None,
        "pid": None
    }
    for key, val in defaults.items():
        if key not in stream_data:
            stream_data[key] = val
    return stream_data

def load_streams() -> dict:
    if not os.path.exists(DB_FILE):
        return {}
    with open(DB_FILE, "r") as f:
        raw = json.load(f)
    for sid, data in raw.items():
        raw[sid] = normalize_stream(data)
    return raw

def save_streams(streams: dict):
    data_to_save = {}
    for sid, s in streams.items():
        data_to_save[sid] = normalize_stream(s.copy())
    with open(DB_FILE, "w") as f:
        json.dump(data_to_save, f, indent=2, ensure_ascii=False)

streams: Dict[int, dict] = load_streams()

# ═══════════════════════════════════════════
# Stream Manager
# ═══════════════════════════════════════════
processes: Dict[int, asyncio.subprocess.Process] = {}
retry_counter: Dict[int, int] = {}

async def _start_ffmpeg(stream_id: int):
    s = streams[stream_id]
    input_url = s["input"]
    output_url = s["output"]
    logo = s.get("logo", "")

    cmd = [
        "ffmpeg",
        "-re",
        "-i", input_url,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-b:v", "2500k",
        "-maxrate", "2500k",
        "-bufsize", "5000k",
        "-c:a", "aac",
        "-f", "flv",
        output_url
    ]

    if logo and os.path.exists(logo):
        cmd = [
            "ffmpeg",
            "-re",
            "-i", input_url,
            "-i", logo,
            "-filter_complex", "overlay=W-w-20:20",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-tune", "zerolatency",
            "-b:v", "2500k",
            "-maxrate", "2500k",
            "-bufsize", "5000k",
            "-c:a", "aac",
            "-f", "flv",
            output_url
        ]

    logger.info(f"بدء تشغيل البث {stream_id} - {s['name']}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    processes[stream_id] = proc
    s["status"] = "running"
    s["started_at"] = time.time()
    s["pid"] = proc.pid
    save_streams(streams)
    return proc

async def start_stream(stream_id: int) -> tuple[bool, str]:
    if stream_id not in streams:
        return False, "Stream not found"
    if stream_id in processes and processes[stream_id].returncode is None:
        return False, "Already running"
    await stop_stream(stream_id, kill=True)
    s = streams[stream_id]
    if not s["input"] or not s["output"]:
        return False, "Missing input or output URL"
    try:
        proc = await _start_ffmpeg(stream_id)
        asyncio.create_task(monitor_single_stream(stream_id, proc))
        return True, "Started successfully"
    except Exception as e:
        logger.exception(f"فشل تشغيل البث {stream_id}")
        return False, f"Error: {str(e)}"

async def stop_stream(stream_id: int, kill: bool = False) -> tuple[bool, str]:
    if stream_id not in processes:
        return False, "No process found"
    proc = processes.pop(stream_id, None)
    if proc and proc.returncode is None:
        try:
            if kill:
                proc.kill()
            else:
                proc.terminate()
            await asyncio.sleep(1)
            if proc.returncode is None:
                proc.kill()
        except Exception as e:
            logger.warning(f"خطأ في إيقاف البث {stream_id}: {e}")
    if stream_id in streams:
        streams[stream_id]["status"] = "stopped"
        streams[stream_id]["started_at"] = None
        streams[stream_id]["pid"] = None
        save_streams(streams)
    retry_counter.pop(stream_id, None)
    return True, "Stopped successfully"

async def restart_stream(stream_id: int) -> tuple[bool, str]:
    await stop_stream(stream_id)
    await asyncio.sleep(2)
    return await start_stream(stream_id)

async def monitor_single_stream(stream_id: int, proc: asyncio.subprocess.Process):
    returncode = await proc.wait()
    logger.warning(f"توقف البث {stream_id} برمز {returncode}")
    if stream_id in streams and streams[stream_id].get("status") == "running":
        count = retry_counter.get(stream_id, 0) + 1
        retry_counter[stream_id] = count
        delay = min(30, count * 3)
        if count <= 10:
            logger.info(f"إعادة تشغيل البث {stream_id} بعد {delay} ثانية (محاولة {count})")
            await asyncio.sleep(delay)
            await start_stream(stream_id)
        else:
            logger.error(f"توقف البث {stream_id} نهائياً بعد {count} محاولات فاشلة")
            streams[stream_id]["status"] = "error"
            save_streams(streams)

# ═══════════════════════════════════════════
# FastAPI
# ═══════════════════════════════════════════
app = FastAPI(title="Stream Manager API")

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return True

@app.get("/")
async def home():
    return {"status": "online", "streams": len(streams)}

@app.post("/start/{stream_id}")
async def api_start(stream_id: int, _=Depends(verify_api_key)):
    ok, msg = await start_stream(stream_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"success": ok, "message": msg}

@app.post("/stop/{stream_id}")
async def api_stop(stream_id: int, _=Depends(verify_api_key)):
    ok, msg = await stop_stream(stream_id)
    return {"success": ok, "message": msg}

@app.post("/restart/{stream_id}")
async def api_restart(stream_id: int, _=Depends(verify_api_key)):
    ok, msg = await restart_stream(stream_id)
    return {"success": ok, "message": msg}

@app.get("/status/{stream_id}")
async def api_status(stream_id: int, _=Depends(verify_api_key)):
    if stream_id not in streams:
        raise HTTPException(status_code=404, detail="Not found")
    s = streams[stream_id]
    return {"status": s.get("status"), "pid": s.get("pid")}

# ═══════════════════════════════════════════
# Telegram Bot
# ═══════════════════════════════════════════
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def get_keyboard(stream_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ تشغيل", callback_data=f"start_{stream_id}"),
            InlineKeyboardButton(text="⏹️ إيقاف", callback_data=f"stop_{stream_id}")
        ],
        [
            InlineKeyboardButton(text="🔄 إعادة تشغيل", callback_data=f"restart_{stream_id}"),
            InlineKeyboardButton(text="📊 حالة", callback_data=f"status_{stream_id}")
        ]
    ])

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("🚫 غير مصرح لك.")
        return
    if not streams:
        await message.reply("لا توجد بثوث مسجلة.\nأضف بثاً عبر تعديل ملف streams.json")
        return
    for sid, s in streams.items():
        status = s.get("status", "stopped")
        started_at = s.get("started_at")
        started_str = time.strftime('%H:%M:%S', time.localtime(started_at)) if started_at else "—"
        kb = get_keyboard(sid)
        await message.answer(
            f"🎬 *{s.get('name', 'بدون اسم')}*\n"
            f"📡 الحالة: `{status}`\n"
            f"⏱️ بدأ: {started_str}",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )

@dp.callback_query()
async def handle_callback(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("غير مصرح", show_alert=True)
        return
    data = callback.data
    try:
        action, stream_id = data.split("_")
        stream_id = int(stream_id)
    except Exception:
        await callback.answer("بيانات غير صالحة")
        return

    async def send_api_request(endpoint: str, method="POST"):
        url = f"http://{API_HOST}:{API_PORT}/{endpoint}/{stream_id}"
        headers = {"X-API-Key": API_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                if method == "POST":
                    async with session.post(url, headers=headers) as resp:
                        return await resp.json()
                else:
                    async with session.get(url, headers=headers) as resp:
                        return await resp.json()
        except Exception as e:
            return {"success": False, "message": f"API error: {str(e)}"}

    if action == "start":
        res = await send_api_request("start")
        await callback.message.reply(res.get("message", "غير معروف"))
    elif action == "stop":
        res = await send_api_request("stop")
        await callback.message.reply(res.get("message", "غير معروف"))
    elif action == "restart":
        res = await send_api_request("restart")
        await callback.message.reply(res.get("message", "غير معروف"))
    elif action == "status":
        res = await send_api_request("status", method="GET")
        status = res.get("status", "unknown")
        pid = res.get("pid")
        await callback.message.reply(f"الحالة: {status}\nPID: {pid if pid else '—'}")
    else:
        await callback.answer("أمر غير معروف")
    await callback.answer()

# ═══════════════════════════════════════════
# تشغيل الخادمين معاً
# ═══════════════════════════════════════════
async def run_api():
    config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

async def shutdown(sig):
    logger.info(f"استقبل إشارة {sig.name}, جاري الإيقاف النظيف...")
    for sid in list(processes.keys()):
        await stop_stream(sid, kill=True)
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("تم الإنهاء.")
    asyncio.get_event_loop().stop()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    api_task = asyncio.create_task(run_api())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    logger.info("🚀 النظام يعمل - API على http://%s:%d , Bot يعمل", API_HOST, API_PORT)
    await asyncio.gather(api_task, bot_task)

if __name__ == "__main__":
    if os.system("ffmpeg -version > /dev/null 2>&1") != 0:
        print("خطأ: ffmpeg غير مثبت. قم بتثبيته أولاً.")
        exit(1)
    if not streams:
        streams = {
            1: normalize_stream({
                "name": "قناة رياضية",
                "input": "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
                "output": "rtmp://a.rtmp.youtube.com/live2/YOUR_STREAM_KEY",
                "logo": "logo.png",
            })
        }
        save_streams(streams)
    asyncio.run(main())