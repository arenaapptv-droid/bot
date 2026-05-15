#!/usr/bin/env python3
"""
نظام بث متكامل - Telegram Bot + FastAPI + Stream Manager + FFmpeg
مع واجهة Reply Keyboard وإدارة كاملة لكل بث
المتطلبات: pip install fastapi uvicorn aiogram aiohttp python-multipart psutil
"""

import asyncio
import json
import os
import signal
import time
import logging
from typing import Dict, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, Header
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.enums import ParseMode
import uvicorn
import aiohttp

# محاولة استيراد psutil للمراقبة
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil غير مثبت - قم بتثبيته: pip install psutil")

# ═══════════════════════════════════════════
# الإعدادات
# ═══════════════════════════════════════════
BOT_TOKEN = "8260979666:AAHkg21xZmD5svkyswqu9ascEz1pJf2P0Kg"
ADMIN_ID = 8266981888
API_HOST = "127.0.0.1"
API_PORT = 8080
API_KEY = "CHANGE_THIS_SECRET_KEY_FOR_PROTECTION"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════
# قاعدة البيانات
# ═══════════════════════════════════════════
DB_FILE = "streams.json"
user_data = {}

def normalize_stream(stream_data: dict) -> dict:
    defaults = {
        "name": "",
        "input": "",
        "output": "",
        "logo": "",
        "mode": "copy",
        "status": "stopped",
        "started_at": None,
        "pid": None,
        "fps": "—",
        "viewers": 0
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
    streams_dict = {}
    for sid, data in raw.items():
        streams_dict[int(sid)] = normalize_stream(data)
    return streams_dict

def save_streams(streams_dict: dict):
    data_to_save = {}
    for sid, s in streams_dict.items():
        data_to_save[str(sid)] = normalize_stream(s.copy())
    with open(DB_FILE, "w") as f:
        json.dump(data_to_save, f, indent=2, ensure_ascii=False)

streams: Dict[int, dict] = load_streams()
processes: Dict[int, asyncio.subprocess.Process] = {}
retry_counter: Dict[int, int] = {}

# ═══════════════════════════════════════════
# مراقبة النظام
# ═══════════════════════════════════════════
def get_system_stats() -> dict:
    """جمع إحصائيات النظام"""
    stats = {
        "cpu": 0,
        "ram": 0,
        "ram_used": 0,
        "ram_total": 0,
        "disk": 0,
        "disk_used": 0,
        "disk_total": 0,
        "active_streams": 0,
        "total_streams": len(streams),
        "psutil_available": PSUTIL_AVAILABLE
    }
    
    if PSUTIL_AVAILABLE:
        stats["cpu"] = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory()
        stats["ram"] = mem.percent
        stats["ram_used"] = mem.used // (1024**2)
        stats["ram_total"] = mem.total // (1024**2)
        disk = psutil.disk_usage("/")
        stats["disk"] = disk.percent
        stats["disk_used"] = disk.used // (1024**3)
        stats["disk_total"] = disk.total // (1024**3)
    
    # حساب عدد البثوث النشطة
    stats["active_streams"] = sum(1 for s in streams.values() if s.get("status") == "running")
    
    return stats

def format_system_stats() -> str:
    """تنسيق إحصائيات النظام للنص"""
    stats = get_system_stats()
    
    if stats["psutil_available"]:
        text = (
            "🖥️ *مراقبة الخادم*\n\n"
            f"📡 *البثوث:* {stats['active_streams']} نشط / {stats['total_streams']} إجمالي\n\n"
            f"🖥️ *CPU:* `{stats['cpu']:.1f}%`\n"
            f"🧠 *RAM:* `{stats['ram']:.1f}%` ({stats['ram_used']} MB / {stats['ram_total']} MB)\n"
            f"💾 *Disk:* `{stats['disk']:.1f}%` ({stats['disk_used']} GB / {stats['disk_total']} GB)\n"
        )
    else:
        text = (
            "🖥️ *مراقبة الخادم*\n\n"
            f"📡 *البثوث:* {stats['active_streams']} نشط / {stats['total_streams']} إجمالي\n\n"
            "⚠️ *معلومات إضافية غير متوفرة*\n"
            "قم بتثبيت psutil: `pip install psutil`"
        )
    
    return text

# ═══════════════════════════════════════════
# لوحات المفاتيح
# ═══════════════════════════════════════════
def get_main_reply_keyboard() -> ReplyKeyboardMarkup:
    """لوحة المفاتيح الرئيسية (Reply Keyboard)"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📡 قائمة البثوث"), KeyboardButton(text="📊 مراقبة الخادم")],
            [KeyboardButton(text="➕ إضافة بث جديد"), KeyboardButton(text="❌ حذف بث")],
            [KeyboardButton(text="🔄 إعادة تشغيل الكل"), KeyboardButton(text="⏹ إيقاف الكل")]
        ],
        resize_keyboard=True
    )

def get_stream_inline_keyboard(stream_id: int) -> InlineKeyboardMarkup:
    """لوحة تحكم متقدمة لكل بث (Inline Keyboard)"""
    s = streams.get(stream_id, {})
    is_running = s.get("status") == "running"
    current_mode = s.get("mode", "copy")
    
    # حالة البث
    status_icon = "🟢" if is_running else "🔴"
    status_text = "إيقاف" if is_running else "تشغيل"
    status_action = "stop" if is_running else "start"
    
    # وضع البث
    mode_text = "🔧 ترميز" if current_mode == "encode" else "📋 نسخ"
    mode_action = "encode" if current_mode == "copy" else "copy"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        # الصف الأول: تشغيل/إيقاف
        [
            InlineKeyboardButton(
                text=f"{status_icon} {status_text}", 
                callback_data=f"stream_{status_action}_{stream_id}"
            )
        ],
        # الصف الثاني: الوضع
        [
            InlineKeyboardButton(
                text=f"⚙️ الوضع: {mode_text}", 
                callback_data=f"stream_mode_{mode_action}_{stream_id}"
            )
        ],
        # الصف الثالث: شعار + حالة
        [
            InlineKeyboardButton(
                text="🖼 إضافة/تغيير شعار", 
                callback_data=f"stream_logo_{stream_id}"
            ),
            InlineKeyboardButton(
                text="📊 حالة البث", 
                callback_data=f"stream_status_{stream_id}"
            )
        ],
        # الصف الرابع: إعادة تشغيل + تعديل
        [
            InlineKeyboardButton(
                text="🔄 إعادة تشغيل", 
                callback_data=f"stream_restart_{stream_id}"
            ),
            InlineKeyboardButton(
                text="✏️ تعديل المصدر", 
                callback_data=f"stream_edit_source_{stream_id}"
            )
        ]
    ])

def get_streams_list_keyboard(page: int = 0, items_per_page: int = 5) -> InlineKeyboardMarkup:
    """قائمة البثوث مع أزرار التنقل"""
    items = [(sid, s["name"]) for sid, s in streams.items()]
    total_pages = max(1, (len(items) + items_per_page - 1) // items_per_page)
    page = max(0, min(page, total_pages - 1))
    
    start = page * items_per_page
    end = start + items_per_page
    
    buttons = []
    for sid, name in items[start:end]:
        s = streams.get(sid, {})
        icon = "🟢" if s.get("status") == "running" else "🔴"
        buttons.append([InlineKeyboardButton(f"{icon} {name}", callback_data=f"view_stream_{sid}")])
    
    # أزرار التنقل
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"list_page_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"list_page_{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ═══════════════════════════════════════════
# FFmpeg وإدارة البث
# ═══════════════════════════════════════════
async def _start_ffmpeg(stream_id: int):
    s = streams[stream_id]
    input_url = s["input"]
    output_url = s["output"]
    logo = s.get("logo", "")
    mode = s.get("mode", "copy")
    
    if mode == "copy":
        cmd = [
            "ffmpeg", "-re", "-i", input_url,
            "-c:v", "copy", "-c:a", "copy",
            "-f", "flv", output_url
        ]
    else:
        cmd = [
            "ffmpeg", "-re", "-i", input_url,
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            "-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "flv", output_url
        ]
    
    if logo and os.path.exists(logo):
        cmd = [
            "ffmpeg", "-re", "-i", input_url, "-i", logo,
            "-filter_complex", "overlay=W-w-20:20",
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            "-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "flv", output_url
        ]
    
    logger.info(f"بدء تشغيل البث {stream_id} - {s['name']} (وضع: {mode})")
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
        return False, "البث غير موجود"
    if stream_id in processes and processes[stream_id].returncode is None:
        return False, "البث يعمل بالفعل"
    await stop_stream(stream_id, kill=True)
    s = streams[stream_id]
    if not s["input"] or not s["output"]:
        return False, "رابط المصدر أو الإخراج غير مكتمل"
    try:
        proc = await _start_ffmpeg(stream_id)
        asyncio.create_task(monitor_single_stream(stream_id, proc))
        return True, "تم تشغيل البث بنجاح"
    except Exception as e:
        logger.exception(f"فشل تشغيل البث {stream_id}")
        return False, f"خطأ: {str(e)}"

async def stop_stream(stream_id: int, kill: bool = False) -> tuple[bool, str]:
    if stream_id not in processes:
        return False, "لا توجد عملية للتشغيل"
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
    return True, "تم إيقاف البث"

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
            logger.info(f"إعادة تشغيل البث {stream_id} بعد {delay} ثانية")
            await asyncio.sleep(delay)
            await start_stream(stream_id)
        else:
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
    return {"status": "online", "streams": len(streams), "active": sum(1 for s in streams.values() if s.get("status") == "running")}

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
    return {"status": s.get("status"), "pid": s.get("pid"), "mode": s.get("mode")}

# ═══════════════════════════════════════════
# Telegram Bot
# ═══════════════════════════════════════════
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def send_api_request(stream_id: int, action: str, method: str = "POST"):
    """إرسال طلب إلى API"""
    url = f"http://{API_HOST}:{API_PORT}/{action}/{stream_id}"
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

def format_stream_status(stream_id: int) -> str:
    """تنسيق حالة البث للنص"""
    s = streams.get(stream_id, {})
    is_running = s.get("status") == "running"
    uptime = "—"
    if is_running and s.get("started_at"):
        elapsed = time.time() - s["started_at"]
        uptime = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    status_icon = "🟢 يعمل" if is_running else "🔴 متوقف"
    mode_text = "🔧 ترميز" if s.get("mode") == "encode" else "📋 نسخ مباشر"
    
    text = (
        f"🎬 *{s.get('name', 'بدون اسم')}*\n"
        f"🆔 المعرف: `{stream_id}`\n"
        f"📡 الحالة: {status_icon}\n"
        f"⚙️ الوضع: {mode_text}\n"
        f"⏱️ مدة التشغيل: {uptime}\n"
        f"🎬 FPS: {s.get('fps', '—')}\n"
        f"👥 المشاهدون: {s.get('viewers', 0)}\n"
        f"🖼️ الشعار: {'✅ موجود' if s.get('logo') else '❌ لا يوجد'}\n\n"
        f"📥 المصدر:\n`{s.get('input', '—')[:80]}`\n\n"
        f"📤 الإخراج:\n`{s.get('output', '—')[:80]}`"
    )
    return text

# ═══════════════════════════════════════════
# معالجات الرسائل (Reply Keyboard)
# ═══════════════════════════════════════════
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("🚫 غير مصرح لك.")
        return
    await message.reply(
        "🎬 *مرحباً بك في نظام إدارة البث المباشر*\n\n"
        "استخدم الأزرار أدناه للتحكم:\n\n"
        "📡 *قائمة البثوث* - عرض وإدارة جميع البثوث\n"
        "📊 *مراقبة الخادم* - إحصائيات CPU, RAM, Disk\n"
        "➕ *إضافة بث جديد* - إنشاء قناة جديدة\n"
        "❌ *حذف بث* - إزالة قناة\n"
        "🔄 *إعادة تشغيل الكل* - تشغيل جميع البثوث المتوقفة\n"
        "⏹ *إيقاف الكل* - إيقاف جميع البثوث العاملة",
        reply_markup=get_main_reply_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(lambda msg: msg.text == "📡 قائمة البثوث")
async def show_streams_list(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    if not streams:
        await message.reply("📭 لا توجد بثوث مسجلة حالياً.\nاستخدم ➕ إضافة بث جديد")
        return
    await message.reply("📡 *قائمة البثوث:*\nاختر بثاً للتحكم فيه:", 
                        reply_markup=get_streams_list_keyboard(0),
                        parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "📊 مراقبة الخادم")
async def show_server_monitor(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    stats_text = format_system_stats()
    await message.reply(stats_text, parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "➕ إضافة بث جديد")
async def add_stream_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    user_data[message.from_user.id] = {"step": "add_name"}
    await message.reply("📝 أرسل *اسم القناة* الجديدة:", parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.text == "❌ حذف بث")
async def delete_stream_list(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    if not streams:
        await message.reply("📭 لا توجد بثوث للحذف.")
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(s['name'], callback_data=f"delete_stream_{sid}")]
        for sid, s in streams.items()
    ])
    await message.reply("❌ اختر القناة للحذف:", reply_markup=kb)

@dp.message(lambda msg: msg.text == "🔄 إعادة تشغيل الكل")
async def restart_all_streams(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.reply("🔄 جاري إعادة تشغيل جميع البثوث...")
    for sid in streams.keys():
        await restart_stream(sid)
    await message.reply("✅ تم إعادة تشغيل جميع البثوث")

@dp.message(lambda msg: msg.text == "⏹ إيقاف الكل")
async def stop_all_streams(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.reply("⏹ جاري إيقاف جميع البثوث...")
    for sid in streams.keys():
        await stop_stream(sid)
    await message.reply("✅ تم إيقاف جميع البثوث")

# ═══════════════════════════════════════════
# معالجات إضافة بث جديد
# ═══════════════════════════════════════════
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_name")
async def process_add_name(message: types.Message):
    name = message.text
    new_id = max(streams.keys(), default=0) + 1
    streams[new_id] = normalize_stream({
        "name": name,
        "input": "",
        "output": "",
        "logo": "",
        "mode": "copy"
    })
    save_streams(streams)
    user_data[message.from_user.id] = {"step": "add_input", "sid": new_id}
    await message.reply("📥 أرسل *رابط المصدر* (input):\nمثال: `https://example.com/stream.m3u8`", 
                        parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_input")
async def process_add_input(message: types.Message):
    sid = user_data[message.from_user.id]["sid"]
    streams[sid]["input"] = message.text
    user_data[message.from_user.id]["step"] = "add_output"
    await message.reply("📤 أرسل *رابط الإخراج RTMP*:\nمثال: `rtmp://live.example.com/app/streamkey`", 
                        parse_mode=ParseMode.MARKDOWN)

@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "add_output")
async def process_add_output(message: types.Message):
    sid = user_data[message.from_user.id]["sid"]
    streams[sid]["output"] = message.text
    save_streams(streams)
    del user_data[message.from_user.id]
    await message.reply(
        f"✅ *تمت إضافة القناة بنجاح!*\n\n"
        f"🎬 الاسم: `{streams[sid]['name']}`\n"
        f"🆔 المعرف: `{sid}`\n\n"
        f"استخدم 📡 قائمة البثوث لعرض وإدارة القناة الجديدة.",
        parse_mode=ParseMode.MARKDOWN
    )

# ═══════════════════════════════════════════
# معالجات الـ Callback
# ═══════════════════════════════════════════
@dp.callback_query(lambda c: c.data.startswith("view_stream_"))
async def view_stream_details(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    if stream_id not in streams:
        await callback.message.reply("❌ البث غير موجود")
        await callback.answer()
        return
    
    text = format_stream_status(stream_id)
    await callback.message.edit_text(
        text, 
        reply_markup=get_stream_inline_keyboard(stream_id),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("stream_start_"))
async def stream_start_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    res = await send_api_request(stream_id, "start")
    await callback.answer(res.get("message", "تم"), show_alert=True)
    # تحديث العرض
    text = format_stream_status(stream_id)
    await callback.message.edit_text(
        text, 
        reply_markup=get_stream_inline_keyboard(stream_id),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(lambda c: c.data.startswith("stream_stop_"))
async def stream_stop_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    res = await send_api_request(stream_id, "stop")
    await callback.answer(res.get("message", "تم"), show_alert=True)
    text = format_stream_status(stream_id)
    await callback.message.edit_text(
        text, 
        reply_markup=get_stream_inline_keyboard(stream_id),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(lambda c: c.data.startswith("stream_restart_"))
async def stream_restart_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    res = await send_api_request(stream_id, "restart")
    await callback.answer(res.get("message", "تم"), show_alert=True)
    text = format_stream_status(stream_id)
    await callback.message.edit_text(
        text, 
        reply_markup=get_stream_inline_keyboard(stream_id),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(lambda c: c.data.startswith("stream_mode_"))
async def stream_mode_cmd(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    mode = parts[2]
    stream_id = int(parts[3])
    
    if mode == "copy":
        streams[stream_id]["mode"] = "copy"
    else:
        streams[stream_id]["mode"] = "encode"
    save_streams(streams)
    
    # إعادة تشغيل البث إذا كان يعمل
    if streams[stream_id].get("status") == "running":
        await restart_stream(stream_id)
    
    await callback.answer(f"✅ تم تغيير الوضع إلى {'ترميز' if mode=='encode' else 'نسخ'}")
    text = format_stream_status(stream_id)
    await callback.message.edit_text(
        text, 
        reply_markup=get_stream_inline_keyboard(stream_id),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(lambda c: c.data.startswith("stream_logo_"))
async def stream_logo_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    user_data[callback.from_user.id] = {"step": "set_logo", "sid": stream_id}
    await callback.message.reply("🖼️ أرسل المسار الكامل لملف الشعار:\nمثال: `/root/logo.png`\nأو أرسل `skip` للتخطي")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("stream_status_"))
async def stream_status_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    res = await send_api_request(stream_id, "status", method="GET")
    status = res.get("status", "unknown")
    mode = streams.get(stream_id, {}).get("mode", "copy")
    mode_text = "ترميز" if mode == "encode" else "نسخ"
    await callback.answer(f"الحالة: {status} | الوضع: {mode_text}", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("stream_edit_source_"))
async def stream_edit_source(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[3])
    user_data[callback.from_user.id] = {"step": "edit_source", "sid": stream_id}
    await callback.message.reply("📥 أرسل رابط المصدر الجديد:")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("delete_stream_"))
async def delete_stream_cmd(callback: types.CallbackQuery):
    stream_id = int(callback.data.split("_")[2])
    if stream_id in streams:
        await stop_stream(stream_id, kill=True)
        name = streams[stream_id]["name"]
        del streams[stream_id]
        save_streams(streams)
        await callback.message.reply(f"✅ تم حذف القناة *{name}*", parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("list_page_"))
async def list_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    await callback.message.edit_reply_markup(reply_markup=get_streams_list_keyboard(page))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        "🎬 *القائمة الرئيسية*",
        reply_markup=get_main_reply_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()

# معالجة تعديل المصدر
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "edit_source")
async def process_edit_source(message: types.Message):
    sid = user_data[message.from_user.id]["sid"]
    streams[sid]["input"] = message.text
    save_streams(streams)
    if streams[sid].get("status") == "running":
        await restart_stream(sid)
    del user_data[message.from_user.id]
    await message.reply(f"✅ تم تحديث مصدر القناة `{streams[sid]['name']}`", parse_mode=ParseMode.MARKDOWN)

# معالجة إضافة شعار
@dp.message(lambda msg: msg.from_user.id in user_data and user_data[msg.from_user.id].get("step") == "set_logo")
async def process_set_logo(message: types.Message):
    sid = user_data[message.from_user.id]["sid"]
    if message.text.lower() != "skip":
        if os.path.exists(message.text):
            streams[sid]["logo"] = message.text
            save_streams(streams)
            if streams[sid].get("status") == "running":
                await restart_stream(sid)
            await message.reply(f"✅ تم تحديث شعار القناة `{streams[sid]['name']}`", parse_mode=ParseMode.MARKDOWN)
        else:
            await message.reply("❌ الملف غير موجود! تأكد من المسار.")
    del user_data[message.from_user.id]

# ═══════════════════════════════════════════
# تشغيل الخادمين
# ═══════════════════════════════════════════
async def run_api():
    config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

async def shutdown(sig):
    logger.info(f"استقبل إشارة {sig.name}, جاري الإيقاف...")
    for sid in list(processes.keys()):
        await stop_stream(sid, kill=True)
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.get_event_loop().stop()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    
    api_task = asyncio.create_task(run_api())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    
    logger.info("🚀 النظام يعمل - API على http://%s:%d", API_HOST, API_PORT)
    await asyncio.gather(api_task, bot_task)

if __name__ == "__main__":
    if os.system("ffmpeg -version > /dev/null 2>&1") != 0:
        print("خطأ: ffmpeg غير مثبت. قم بتثبيته أولاً.")
        exit(1)
    
    if not streams:
        streams = {
            1: normalize_stream({
                "name": "قناة تجريبية",
                "input": "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8",
                "output": "rtmp://a.rtmp.youtube.com/live2/YOUR_STREAM_KEY",
                "logo": "",
                "mode": "copy"
            })
        }
        save_streams(streams)
    
    asyncio.run(main())