import os
import sys
import asyncio
import logging
import asyncpg
import yfinance as yf
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
    sys.exit("ENV ERROR")

# ================= BOT =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================= LANG TEXT =================

TEXT = {
    "ru": {
        "menu": "🏠 Главное меню",
        "pairs": "📈 Валютные пары",
        "news": "📰 Новости",
        "lang": "🌍 Сменить язык",
        "choose_pair": "Выберите валютную пару:",
        "choose_exp": "Выберите экспирацию:",
        "signal": "📊 *СИГНАЛ KURUT TRADE*",
        "pair": "Пара",
        "exp": "Экспирация",
        "dir": "Направление",
        "level": "Качество",
        "buy": "ВВЕРХ 📈",
        "sell": "ВНИЗ 📉",
        "strong": "🔥 СИЛЬНЫЙ сигнал",
        "mid": "⚡ СРЕДНИЙ сигнал",
        "weak": "⚠️ СЛАБЫЙ рынок",
        "no_access": "❌ Нет доступа",
        "back": "⬅️ Назад",
        "welcome": "📘 ИНСТРУКЦИЯ KURUT TRADE",
        "next": "➡️ Далее",
        "how": "1️⃣ Регистрация\n2️⃣ Пополнение от 20$\n3️⃣ Проверка ID",
        "get_access": "🔗 Получить доступ",
        "register": "🔗 Регистрация",
        "check": "✅ Проверить ID",
        "wait": "⏳ Ожидаем пополнение от 20$",
    }
}

# зеркалим RU → остальные языки (чтобы код был компактный)
for lang in ["en", "tj", "uz", "kg", "kz"]:
    TEXT[lang] = TEXT["ru"]

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
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS postback_logs (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount FLOAT,
            ip TEXT,
            created_at TIMESTAMP
        );
        """)

async def get_user(uid):
    async with DB_POOL.acquire() as c:
        return await c.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)

async def upsert_user(uid):
    async with DB_POOL.acquire() as c:
        await c.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", uid)

async def set_lang(uid, lang):
    async with DB_POOL.acquire() as c:
        await c.execute("""
        INSERT INTO users (user_id, lang) VALUES ($1,$2)
        ON CONFLICT (user_id) DO UPDATE SET lang=$2
        """, uid, lang)

async def update_balance(uid, amount):
    async with DB_POOL.acquire() as c:
        await c.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, uid)

async def has_access(uid):
    if uid in AUTHORS:
        return True
    u = await get_user(uid)
    return u and u["balance"] >= MIN_DEPOSIT

async def t(uid, key):
    if uid in AUTHORS:
        return TEXT["ru"][key]
    u = await get_user(uid)
    lang = u["lang"] if u else "ru"
    return TEXT[lang][key]

# ================= SIGNAL CORE =================

PAIRS = ["EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X"]
EXPIRATIONS = [1, 5, 10]
INTERVAL = {1:"1m",5:"5m",10:"15m"}

def last(v): return float(v.iloc[-1])

async def get_signal(pair, exp):
    df = yf.download(pair, period="2d", interval=INTERVAL[exp], progress=False)
    if df.empty:
        return "sell", "weak"

    close = df["Close"]
    ema20 = close.ewm(span=20).mean()
    ema50 = close.ewm(span=50).mean()

    if last(ema20) > last(ema50):
        return "buy", "strong"
    return "sell", "mid"

# ================= KEYBOARDS =================

def main_menu(uid):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT["ru"]["pairs"], callback_data="pairs")
    kb.button(text=TEXT["ru"]["news"], callback_data="news")
    kb.button(text=TEXT["ru"]["lang"], callback_data="change_lang")
    kb.adjust(1)
    return kb.as_markup()

def lang_kb():
    kb = InlineKeyboardBuilder()
    for l in ["ru","en","tj","uz","kg","kz"]:
        kb.button(text=l.upper(), callback_data=f"lang:{l}")
    kb.adjust(3)
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu(msg.from_user.id))
        return

    await msg.answer("🌍 Выберите язык", reply_markup=lang_kb())

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def set_language(cb):
    await set_lang(cb.from_user.id, cb.data.split(":")[1])
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT["ru"]["next"], callback_data="instr2")
    await cb.message.edit_text(TEXT["ru"]["welcome"], reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT["ru"]["get_access"], callback_data="get_access")
    await cb.message.edit_text(TEXT["ru"]["how"], reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT["ru"]["register"], url=REF_LINK)
    kb.button(text=TEXT["ru"]["check"], callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("🔓", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb):
    await upsert_user(cb.from_user.id)
    if await has_access(cb.from_user.id):
        await cb.message.edit_text(TEXT["ru"]["menu"], reply_markup=main_menu(cb.from_user.id))
    else:
        await cb.answer(TEXT["ru"]["wait"], show_alert=True)

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id","")
    amount = request.query.get("amount","0")
    ip = request.remote

    if not click_id.isdigit():
        return web.Response(text="BAD CLICK_ID")

    amount = float(amount)
    if amount <= 0:
        return web.Response(text="BAD AMOUNT")

    uid = int(click_id)
    await upsert_user(uid)
    await update_balance(uid, amount)

    async with DB_POOL.acquire() as c:
        await c.execute("""
        INSERT INTO postback_logs (user_id, amount, ip, created_at)
        VALUES ($1,$2,$3,$4)
        """, uid, amount, ip, datetime.utcnow())

    logging.info(f"POSTBACK OK | {uid} | {amount}$ | {ip}")
    return web.Response(text="OK")

# ================= START =================

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

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
