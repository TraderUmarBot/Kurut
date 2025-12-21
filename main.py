import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
from datetime import datetime

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.methods import DeleteWebhook, SetWebhook

# ================= CONFIG =================

TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR: Check your environment variables!")
    sys.exit(1)

# ================= BOT INITIALIZATION =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================= CONSTANTS =================

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = [1, 5, 15]

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    15: "15m"
}

# ================= LANG TEXT (BEAUTIFIED) =================

TEXT = {
    "ru": {
        "choose_lang": "🌍 Выберите язык",
        "menu": "🏠 Главное меню",
        "pairs": "📈 Валютные пары",
        "signal": "📊 СИГНАЛ KURUT TRADE",
        "up": "ВВЕРХ 🚀",
        "down": "ВНИЗ 📉",
        "strong": "🔥 СИЛЬНЫЙ",
        "medium": "⚡ СРЕДНИЙ",
        "weak": "⚠️ СЛАБЫЙ",
        "no_access": "❌ Нет доступа",
        "instr": (
            "🚀 **ДОБРО ПОЖАЛОВАТЬ В KURUT TRADE**\n\n"
            "**1. КАК ПОЛУЧИТЬ ДОСТУП?**\n"
            f"• Регистрация: [ССЫЛКА ТУТ]({REF_LINK})\n"
            "• Депозит от **20$** (ваш капитал)\n"
            "• После пополнения бот откроется автоматически\n\n"
            "**2. КАК ТОРГОВАТЬ?**\n"
            "• Выберите пару и время\n"
            "• Входите в сделку сразу после сигнала!\n"
        )
    },
    "uz": {
        "choose_lang": "🌍 Tilni tanlang",
        "menu": "🏠 Asosiy menyu",
        "pairs": "📈 Valyuta juftliklari",
        "signal": "📊 KURUT TRADE SIGNALI",
        "up": "YUQORIGA 🚀",
        "down": "PASTGA 📉",
        "strong": "🔥 KUCHLI",
        "medium": "⚡ O'RTACHA",
        "weak": "⚠️ ZAIF",
        "no_access": "❌ Ruxsat yo'q",
        "instr": "🔐 Kirish uchun ro'yxatdan o'ting va 20$ depozit qiling."
    },
    "kz": {
        "choose_lang": "🌍 Тілді таңдаңыз",
        "menu": "🏠 Басты мәзір",
        "pairs": "📈 Валюта жұптары",
        "signal": "📊 KURUT TRADE СИГНАЛЫ",
        "up": "ЖОҒАРЫ 🚀",
        "down": "ТӨМЕН 📉",
        "strong": "🔥 КҮШТІ",
        "medium": "⚡ ОРТАША",
        "weak": "⚠️ ӘЛСІЗ",
        "no_access": "❌ Қол жетімсіз",
        "instr": "🔐 Кіру үшін тіркеліп, 20$ депозит салыңыз."
    },
    "kg": {
        "choose_lang": "🌍 Тилди тандаңыз",
        "menu": "🏠 Башкы меню",
        "pairs": "📈 Валюта жуптары",
        "signal": "📊 KURUT TRADE СИГНАЛ",
        "up": "ЖОГОРУ 🚀",
        "down": "ТӨМӨН 📉",
        "strong": "🔥 КҮЧТҮҮ",
        "medium": "⚡ ОРТОЧО",
        "weak": "⚠️ АЛСЫЗ",
        "no_access": "❌ Кирүү жок",
        "instr": "🔐 Кирүү үчүн каттоодон өтүп, 20$ депозит салыңыз."
    },
    "tj": {
        "choose_lang": "🌍 Забонро интихоб кунед",
        "menu": "🏠 Меню",
        "pairs": "📈 Ҷуфтҳо",
        "signal": "📊 СИГНАЛИ KURUT",
        "up": "БОЛО 🚀",
        "down": "ПОЁН 📉",
        "strong": "🔥 ҚАВӢ",
        "medium": "⚡ МИЁНА",
        "weak": "⚠️ ЗАИФ",
        "no_access": "❌ Дастрас нест",
        "instr": "🔐 Барои дастрасӣ сабти ном кунед ва 20$ пасандоз кунед."
    },
    "en": {
        "choose_lang": "🌍 Choose language",
        "menu": "🏠 Main menu",
        "pairs": "📈 Currency pairs",
        "signal": "📊 KURUT TRADE SIGNAL",
        "up": "UP 🚀",
        "down": "DOWN 📉",
        "strong": "🔥 STRONG",
        "medium": "⚡ MEDIUM",
        "weak": "⚠️ WEAK",
        "no_access": "❌ No access",
        "instr": "🔐 Register and deposit $20 to get access."
    }
}

# ================= DATABASE =================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0,
            lang TEXT DEFAULT 'ru'
        );
        """)

async def upsert_user(uid):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", uid)

async def set_lang(uid, lang):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET lang=$1 WHERE user_id=$2", lang, uid)

async def get_user(uid):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)

async def has_access(uid):
    if uid in AUTHORS: return True
    u = await get_user(uid)
    return bool(u and u["balance"] >= MIN_DEPOSIT)

# ================= POWERFUL SIGNAL ENGINE (10 INDICATORS) =================

async def get_signal(pair, exp):
    df = yf.download(pair, period="1d", interval=INTERVAL_MAP[exp], progress=False)
    if df.empty or len(df) < 30: return "down", "weak"

    # Индикаторы
    df['RSI'] = ta.rsi(df['Close'], length=14)
    macd = ta.macd(df['Close'])
    df['EMA_10'] = ta.ema(df['Close'], length=10)
    df['EMA_30'] = ta.ema(df['Close'], length=30)
    stoch = ta.stoch(df['High'], df['Low'], df['Close'])
    df['ADX'] = ta.adx(df['High'], df['Low'], df['Close'])['ADX_14']
    df['CCI'] = ta.cci(df['High'], df['Low'], df['Close'], length=20)
    df['WPR'] = ta.willr(df['High'], df['Low'], df['Close'], length=14)
    bbands = ta.bbands(df['Close'], length=20)
    df['MFI'] = ta.mfi(df['High'], df['Low'], df['Close'], df['Volume'], length=14)

    score = 0
    l = -1 # Последняя свеча

    # Математика сигналов
    if df['RSI'].iloc[l] > 50: score += 1
    elif df['RSI'].iloc[l] < 50: score -= 1
    
    if macd['MACD_12_26_9'].iloc[l] > macd['MACDs_12_26_9'].iloc[l]: score += 1
    else: score -= 1

    if df['EMA_10'].iloc[l] > df['EMA_30'].iloc[l]: score += 2
    else: score -= 2

    if stoch['STOCHk_14_3_3'].iloc[l] > stoch['STOCHd_14_3_3'].iloc[l]: score += 1
    
    if df['CCI'].iloc[l] > 100: score += 1
    elif df['CCI'].iloc[l] < -100: score -= 1

    if df['WPR'].iloc[l] > -20: score += 1
    elif df['WPR'].iloc[l] < -80: score -= 1

    if df['Close'].iloc[l] > bbands['BBM_20_2.0'].iloc[l]: score += 1
    
    if df['MFI'].iloc[l] > 50: score += 1
    
    # Сила тренда через ADX
    if df['ADX'].iloc[l] > 25: score *= 1.2

    direction = "up" if score > 0 else "down"
    abs_s = abs(score)
    strength = "strong" if abs_s >= 5 else "medium" if abs_s >= 2 else "weak"
    
    return direction, strength

# ================= KEYBOARDS =================

def lang_kb():
    kb = InlineKeyboardBuilder()
    for l in ["ru","uz","kz","kg","tj","en"]:
        kb.button(text=l.upper(), callback_data=f"lang:{l}")
    kb.adjust(3)
    return kb.as_markup()

def main_menu(lang):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT[lang]["pairs"], callback_data="pairs")
    kb.adjust(1)
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 **AUTHOR ACCESS GRANTED**", reply_markup=main_menu("ru"))
    else:
        await msg.answer("🌍 **Choose your language / Выберите язык**", reply_markup=lang_kb())

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def set_language(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    await set_lang(cb.from_user.id, lang)
    await cb.message.edit_text(TEXT[lang]["instr"], reply_markup=main_menu(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    lang = user["lang"]
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    for p in PAIRS:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.adjust(2)
    await cb.message.edit_text(f"📉 **{TEXT[lang]['pairs']}**", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair_name = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"⏱ {e}m", callback_data=f"exp:{pair_name}:{e}")
    kb.adjust(3)
    await cb.message.edit_text(f"💎 **{pair_name.replace('=X','')}**\nВыберите время сделки:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, p_name, e_time = cb.data.split(":")
    user = await get_user(cb.from_user.id)
    lang = user["lang"]

    await cb.message.edit_text("🔍 **Анализирую рынок...**")
    
    direction, strength = await get_signal(p_name, int(e_time))
    
    icon = "🟢" if direction == "up" else "🔴"
    stars = "⭐️⭐️⭐️" if strength == "strong" else "⭐️⭐️" if strength == "medium" else "⭐️"
    
    res_text = (
        f"💎 **{TEXT[lang]['signal']}** 💎\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 **ПАРА:** `{p_name.replace('=X','')}`\n"
        f"⏳ **ВРЕМЯ:** `{e_time} МИНУТ`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{icon} **ПРОГНОЗ: {TEXT[lang][direction]}**\n"
        f"🔥 **СИЛА: {TEXT[lang][strength]} {stars}**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {datetime.now().strftime('%H:%M:%S')} UTC\n"
        f"📍 *Входите в сделку сейчас!*"
    )

    await cb.message.edit_text(res_text, reply_markup=main_menu(lang), parse_mode="Markdown")

# ================= POSTBACK (AUTOMATIC DEPOSIT CHECK) =================

async def postback(request):
    try:
        cid = request.query.get("click_id","")
        amount = float(request.query.get("amount","0"))
        if cid.isdigit():
            user_id = int(cid)
            await upsert_user(user_id)
            async with DB_POOL.acquire() as conn:
                await conn.execute("UPDATE users SET balance = $1 WHERE user_id = $2", amount, user_id)
            logging.info(f"Postback success: User {user_id} deposit {amount}")
    except Exception as e:
        logging.error(f"Postback error: {e}")
        
    return web.Response(text="OK")

# ================= STARTUP =================

async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    logging.info("--- BOT IS ONLINE ---")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())

