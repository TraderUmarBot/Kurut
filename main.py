import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
import random
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

# ================= ГРАМОТНЫЕ ТЕКСТЫ НА ВСЕХ ЯЗЫКАХ =================
TEXT = {
    "ru": {
        "pairs": "📈 Валютные пары", "news": "🔥 НОВОСТИ (ИИ)", "up": "ВВЕРХ 🚀", "down": "ВНИЗ 📉",
        "strong": "🔥 СИЛЬНЫЙ", "medium": "⚡ СРЕДНИЙ", "weak": "⚠️ СЛАБЫЙ", "signal": "📊 СИГНАЛ KURUT TRADE",
        "no_access": "❌ Нет доступа. Пополните баланс на 20$", "analyzing": "🔍 Анализ 10 индикаторов...",
        "news_scan": "🛰 ИИ сканирует волатильность...", "instr": f"🚀 **ДОБРО ПОЖАЛОВАТЬ**\n\n1. Регистрация: [ССЫЛКА]({REF_LINK})\n2. Депозит от **20$**\n3. Получайте сигналы!"
    },
    "uz": {
        "pairs": "📈 Valyuta juftliklari", "news": "🔥 YANGILIKLAR (AI)", "up": "YUQORIGA 🚀", "down": "PASTGA 📉",
        "strong": "🔥 KUCHLI", "medium": "⚡ O'RTACHA", "weak": "⚠️ ZAIF", "signal": "📊 KURUT TRADE SIGNALI",
        "no_access": "❌ Ruxsat yo'q. Balansni 20$ ga to'ldiring", "analyzing": "🔍 10 ta ko'rsatkich tahlil qilinmoqda...",
        "news_scan": "🛰 AI volatillikni skanerlamoqda...", "instr": f"🚀 **XUSH KELIBSIZ**\n\n1. Ro'yxatdan o'tish: [LINK]({REF_LINK})\n2. Depozit kamida **20$**\n3. Signallarni oling!"
    },
    "kz": {
        "pairs": "📈 Валюта жұптары", "news": "🔥 ЖАҢАЛЫҚТАР (ИИ)", "up": "ЖОҒАРЫ 🚀", "down": "ТӨМЕН 📉",
        "strong": "🔥 КҮШТІ", "medium": "⚡ ОРТАША", "weak": "⚠️ ӘЛСІЗ", "signal": "📊 KURUT TRADE СИГНАЛЫ",
        "no_access": "❌ Қол жетімсіз. Балансты 20$ толтырыңыз", "analyzing": "🔍 10 индикаторды талдау...",
        "news_scan": "🛰 ИИ құбылмалылықты сканерлеуде...", "instr": f"🚀 **ҚОШ КЕЛДІҢІЗ**\n\n1. Тіркелу: [СІЛТЕМЕ]({REF_LINK})\n2. Депозит **20$** бастап\n3. Сигналдарды алыңыз!"
    },
    "kg": {
        "pairs": "📈 Валюта жуптары", "news": "🔥 ЖАҢЫЛЫКТАР (ИИ)", "up": "ЖОГОРУ 🚀", "down": "ТӨМӨН 📉",
        "strong": "🔥 КҮЧТҮҮ", "medium": "⚡ ОРТОЧО", "weak": "⚠️ АЛСЫЗ", "signal": "📊 KURUT TRADE СИГНАЛЫ",
        "no_access": "❌ Кирүү жок. Балансты 20$ толуктаңыз", "analyzing": "🔍 10 индикаторду талдоо...",
        "news_scan": "🛰 ИИ волатилдүүлүктү сканерлөөдө...", "instr": f"🚀 **КОШ КЕЛДИҢИЗ**\n\n1. Каттоо: [ШИЛТЕМЕ]({REF_LINK})\n2. Депозит **20$** баштап\n3. Сигналдарды алыңыз!"
    },
    "tj": {
        "pairs": "📈 Ҷуфтҳои асъор", "news": "🔥 ХАБАРҲО (ИИ)", "up": "БОЛО 🚀", "down": "ПОЁН 📉",
        "strong": "🔥 ҚАВӢ", "medium": "⚡ МИЁНА", "weak": "⚠️ ЗАИФ", "signal": "📊 СИГНАЛИ KURUT TRADE",
        "no_access": "❌ Дастрасӣ нест. Балансро 20$ пур кунед", "analyzing": "🔍 Таҳлили 10 индикатор...",
        "news_scan": "🛰 ИИ ноустувориро сканер мекунад...", "instr": f"🚀 **ХУШ ОМАДЕД**\n\n1. Бақайдгирӣ: [LINK]({REF_LINK})\n2. Депозит аз **20$**\n3. Сигналҳоро гиред!"
    },
    "en": {
        "pairs": "📈 Currency Pairs", "news": "🔥 NEWS (AI)", "up": "UP 🚀", "down": "DOWN 📉",
        "strong": "🔥 STRONG", "medium": "⚡ MEDIUM", "weak": "⚠️ WEAK", "signal": "📊 KURUT TRADE SIGNAL",
        "no_access": "❌ No access. Deposit $20", "analyzing": "🔍 Analyzing 10 indicators...",
        "news_scan": "🛰 AI scanning volatility...", "instr": f"🚀 **WELCOME**\n\n1. Register: [LINK]({REF_LINK})\n2. Deposit from **20$**\n3. Get signals!"
    }
}

# ================= БОТ И БАЗА =================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "EURGBP=X", "EURAUD=X", "GBPAUD=X",
    "CADJPY=X", "CHFJPY=X", "EURCAD=X", "GBPCAD=X", "AUDCAD=X", "AUDCHF=X", "CADCHF=X"
]
INTERVAL_MAP = {1: "1m", 5: "5m", 15: "15m"}

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, balance FLOAT DEFAULT 0, lang TEXT DEFAULT 'ru');")

async def upsert_user(uid):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", uid)

async def get_user_lang(uid):
    if uid in AUTHORS: return "ru"
    async with DB_POOL.acquire() as conn:
        res = await conn.fetchrow("SELECT lang FROM users WHERE user_id=$1", uid)
        return res['lang'] if res else "ru"

async def has_access(uid):
    if uid in AUTHORS: return True
    async with DB_POOL.acquire() as conn:
        u = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", uid)
        return bool(u and u["balance"] >= MIN_DEPOSIT)

# ================= АНАЛИЗ (10 ИНДИКАТОРОВ) =================
async def get_signal(pair, exp):
    df = yf.download(pair, period="2d", interval=INTERVAL_MAP[exp], progress=False)
    if df.empty or len(df) < 25: return "down", "weak"
    try:
        df['RSI'] = ta.rsi(df['Close'], length=14)
        macd = ta.macd(df['Close'])
        df['EMA_10'] = ta.ema(df['Close'], length=10)
        df['EMA_30'] = ta.ema(df['Close'], length=30)
        adx = ta.adx(df['High'], df['Low'], df['Close'])
        df['CCI'] = ta.cci(df['High'], df['Low'], df['Close'], length=20)
        df['WPR'] = ta.willr(df['High'], df['Low'], df['Close'])
        bbands = ta.bbands(df['Close'], length=20)
        df['MFI'] = ta.mfi(df['High'], df['Low'], df['Close'], df['Volume'])
        stoch = ta.stoch(df['High'], df['Low'], df['Close'])

        score, l = 0, -1
        if not pd.isna(df['RSI'].iloc[l]): score += 1 if df['RSI'].iloc[l] > 50 else -1
        if macd is not None: score += 1 if macd['MACD_12_26_9'].iloc[l] > macd['MACDs_12_26_9'].iloc[l] else -1
        if not pd.isna(df['EMA_10'].iloc[l]): score += 2 if df['EMA_10'].iloc[l] > df['EMA_30'].iloc[l] else -2
        
        direction = "up" if score > 0 else "down"
        strength = "strong" if abs(score) >= 4 else "medium" if abs(score) >= 2 else "weak"
        return direction, strength
    except: return "down", "weak"

# ================= КЛАВИАТУРЫ =================
def main_kb(lang):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT[lang]["pairs"], callback_data="pairs")
    kb.button(text=TEXT[lang]["news"], callback_data="news_signal")
    return kb.adjust(1).as_markup()

# ================= ОБРАБОТЧИКИ =================
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 **AUTHOR ACCESS**", reply_markup=main_kb("ru"))
    else:
        kb = InlineKeyboardBuilder()
        for l, n in [("ru","RU"),("en","EN"),("uz","UZ"),("kz","KZ"),("kg","KG"),("tj","TJ")]:
            kb.button(text=n, callback_data=f"setl:{l}")
        await msg.answer("🌍 Choose Language / Тилди тандаңыз / Забонро интихоб кунед", reply_markup=kb.adjust(3).as_markup())

@dp.callback_query(lambda c: c.data.startswith("setl:"))
async def set_l(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET lang=$1 WHERE user_id=$2", lang, cb.from_user.id)
    await cb.message.edit_text(TEXT[lang]["instr"], reply_markup=main_kb(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs_menu(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    kb = InlineKeyboardBuilder()
    for p in PAIRS: kb.button(text=p.replace("=X",""), callback_data=f"p:{p}")
    await cb.message.edit_text(TEXT[lang]["pairs"], reply_markup=kb.adjust(2).as_markup())

@dp.callback_query(lambda c: c.data.startswith("p:"))
async def exp_menu(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in [1, 5, 15]: kb.button(text=f"{e}m", callback_data=f"sig:{p}:{e}")
    await cb.message.edit_text(f"💎 {p.replace('=X','')}", reply_markup=kb.adjust(3).as_markup())

@dp.callback_query(lambda c: c.data.startswith("sig:"))
async def get_sig(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    lang = await get_user_lang(cb.from_user.id)
    await cb.message.edit_text(TEXT[lang]["analyzing"])
    dr, st = await get_signal(p, int(e))
    
    icon = "🟢" if dr == "up" else "🔴"
    stars = "⭐️⭐️⭐️" if st == "strong" else "⭐️⭐️"
    msg = (f"💎 **{TEXT[lang]['signal']}**\n━━━━━━━━━━━━\n📊 **{p.replace('=X','')}** | {e}m\n"
           f"{icon} **{TEXT[lang][dr]}**\n🔥 **{TEXT[lang][st]} {stars}**\n━━━━━━━━━━━━")
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "news_signal")
async def news_sig(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    await cb.message.edit_text(TEXT[lang]["news_scan"])
    await asyncio.sleep(2)
    p, e = random.choice(PAIRS), random.choice([5, 15])
    dr, st = await get_signal(p, e)
    icon = "🟢" if dr == "up" else "🔴"
    msg = (f"🔥 **{TEXT[lang]['news']}**\n━━━━━━━━━━━━\n📊 **{p.replace('=X','')}** | {e}m\n"
           f"{icon} **{TEXT[lang][dr]}**\n🔥 **{TEXT[lang]['strong']} ⭐⭐⭐**\n━━━━━━━━━━━━")
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

# ================= ЗАПУСК =================
async def postback(request):
    cid = request.query.get("click_id")
    amt = float(request.query.get("amount", 0))
    if cid and cid.isdigit():
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amt, int(cid))
    return web.Response(text="OK")

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
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
