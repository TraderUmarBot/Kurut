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

# ================= BOT =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================= TEXT =================

TEXT = {
    "ru": {
        "menu": "🏠 Главное меню",
        "pairs": "📈 Валютные пары",
        "news": "📰 Новости",
        "lang": "🌍 Сменить язык",
        "choose_pair": "Выберите валютную пару:",
        "choose_exp": "Выберите экспирацию:",
        "back": "⬅️ Назад",
        "buy": "ВВЕРХ 📈",
        "sell": "ВНИЗ 📉",
        "strong": "🔥 СИЛЬНЫЙ сигнал",
        "weak": "⚠️ СЛАБЫЙ рынок",
        "no_access": "❌ Нет доступа",
    }
}

for l in ["en","tj","uz","kg","kz"]:
    TEXT[l] = TEXT["ru"]

# ================= DATABASE =================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0,
            lang TEXT DEFAULT 'ru'
        );
        """)
        await c.execute("""
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

# ================= SIGNALS =================

PAIRS = ["EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X"]
EXP = [1,5,10]
INTERVAL = {1:"1m",5:"5m",10:"15m"}

def last(v): return float(v.iloc[-1])

async def get_signal(pair, exp):
    df = yf.download(pair, period="2d", interval=INTERVAL[exp], progress=False)
    if df.empty:
        return "sell", "weak"
    close = df["Close"]
    ema20 = close.ewm(span=20).mean()
    ema50 = close.ewm(span=50).mean()
    return ("buy","strong") if last(ema20) > last(ema50) else ("sell","weak")

# ================= KEYBOARDS =================

def main_menu(uid):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT["ru"]["pairs"], callback_data="pairs")
    kb.button(text=TEXT["ru"]["news"], callback_data="news")
    kb.button(text=TEXT["ru"]["lang"], callback_data="change_lang")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb():
    kb = InlineKeyboardBuilder()
    for p in PAIRS:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.button(text=TEXT["ru"]["back"], callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXP:
        kb.button(text=f"{e}m", callback_data=f"exp:{pair}:{e}")
    kb.button(text=TEXT["ru"]["back"], callback_data="pairs")
    kb.adjust(2)
    return kb.as_markup()

def lang_kb():
    kb = InlineKeyboardBuilder()
    for l in ["ru","en","tj","uz","kg","kz"]:
        kb.button(text=l.upper(), callback_data=f"lang:{l}")
    kb.adjust(3)
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg):
    await msg.answer("🌍 Choose language", reply_markup=lang_kb())

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def set_language(cb):
    await set_lang(cb.from_user.id, cb.data.split(":")[1])
    await cb.message.edit_text("✅ OK", reply_markup=main_menu(cb.from_user.id))

@dp.callback_query(lambda c: c.data=="menu")
async def menu(cb):
    await cb.message.edit_text(TEXT["ru"]["menu"], reply_markup=main_menu(cb.from_user.id))

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb):
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT["ru"]["no_access"], show_alert=True)
        return
    await cb.message.edit_text(TEXT["ru"]["choose_pair"], reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(TEXT["ru"]["choose_exp"], reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb):
    _, pair, e = cb.data.split(":")
    d,l = await get_signal(pair,int(e))
    await cb.message.edit_text(
        f"📊 {pair.replace('=X','')}\n"
        f"{TEXT['ru'][d]}\n"
        f"{TEXT['ru'][l]}",
        reply_markup=main_menu(cb.from_user.id)
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb):
    pair = PAIRS[0]
    d,l = await get_signal(pair,5)
    await cb.message.edit_text(
        f"📰 {pair.replace('=X','')}\n{TEXT['ru'][d]}\n{TEXT['ru'][l]}",
        reply_markup=main_menu(cb.from_user.id)
    )

# ================= POSTBACK =================

async def postback(request):
    click_id = request.query.get("click_id","")
    amount = request.query.get("amount","0")
    ip = request.remote

    if not click_id.isdigit():
        return web.Response(text="BAD")

    amount = float(amount)
    if amount <= 0:
        return web.Response(text="BAD")

    uid = int(click_id)
    await upsert_user(uid)
    await update_balance(uid, amount)

    async with DB_POOL.acquire() as c:
        await c.execute("""
        INSERT INTO postback_logs (user_id,amount,ip,created_at)
        VALUES ($1,$2,$3,$4)
        """, uid, amount, ip, datetime.utcnow())

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
