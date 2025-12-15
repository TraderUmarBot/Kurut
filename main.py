import os
import sys
import asyncio
import logging
from collections import Counter

import pandas as pd
import pandas_ta as ta
import yfinance as yf
import asyncpg

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [7079260196, 6117198446]

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL = None

# ===================== CONSTANTS =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
TIMEFRAMES = [1, 2, 5, 15]
PAIRS_PER_PAGE = 6
MIN_DEPOSIT = 20.0

# ===================== DB =====================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT,
            balance FLOAT DEFAULT 0
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pair TEXT,
            timeframe INT,
            direction TEXT,
            confidence FLOAT,
            explanation TEXT,
            result TEXT
        );
        """)
    logging.info("✅ БД готова")

async def get_balance(uid):
    async with DB_POOL.acquire() as c:
        return await c.fetchval("SELECT balance FROM users WHERE user_id=$1", uid) or 0

async def add_user(uid):
    async with DB_POOL.acquire() as c:
        await c.execute("INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid)

async def update_balance(uid, amount):
    async with DB_POOL.acquire() as c:
        await c.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)

async def save_trade(uid, pair, tf, direction, conf, expl):
    async with DB_POOL.acquire() as c:
        return await c.fetchval(
            "INSERT INTO trades(user_id,pair,timeframe,direction,confidence,explanation)"
            "VALUES($1,$2,$3,$4,$5,$6) RETURNING id",
            uid, pair, tf, direction, conf, expl
        )

async def get_history(uid):
    async with DB_POOL.acquire() as c:
        return await c.fetch("SELECT * FROM trades WHERE user_id=$1 ORDER BY timestamp DESC LIMIT 20", uid)

# ===================== ANALYSIS =====================
def get_signal(df: pd.DataFrame):
    df = df.dropna()
    if len(df) < 30:
        return "WAIT", 50.0, "Недостаточно данных"

    signals = []

    try:
        signals.append("BUY" if ta.sma(df.Close, 5).iloc[-1] > ta.sma(df.Close, 20).iloc[-1] else "SELL")
        signals.append("BUY" if ta.ema(df.Close, 5).iloc[-1] > ta.ema(df.Close, 20).iloc[-1] else "SELL")

        rsi = ta.rsi(df.Close).iloc[-1]
        signals.append("BUY" if rsi < 30 else "SELL" if rsi > 70 else "BUY")

        macd = ta.macd(df.Close)
        signals.append("BUY" if macd["MACD_12_26_9"].iloc[-1] > macd["MACDs_12_26_9"].iloc[-1] else "SELL")

        bb = ta.bbands(df.Close)
        signals.append("BUY" if df.Close.iloc[-1] < bb["BBL_20_2.0"].iloc[-1] else "SELL")

        adx = ta.adx(df.High, df.Low, df.Close)
        signals.append("BUY" if adx["ADX_14"].iloc[-1] > 25 else "SELL")

    except Exception:
        return "WAIT", 50.0, "Ошибка индикаторов"

    direction, count = Counter(signals).most_common(1)[0]
    confidence = round(count / len(signals) * 100, 1)

    return direction, confidence, "SMA EMA RSI MACD BB ADX"

# ===================== KEYBOARDS =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Пары", callback_data="pairs")
    kb.button(text="📜 История", callback_data="history")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.adjust(2)
    return kb.as_markup()

def tf_kb(pair):
    kb = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        kb.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    kb.adjust(2)
    return kb.as_markup()

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    await add_user(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return

    if await get_balance(msg.from_user.id) < MIN_DEPOSIT:
        await msg.answer(f"Пополните ${MIN_DEPOSIT}\n{REF_LINK}")
        return

    await msg.answer("🏠 Главное меню", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb):
    await cb.message.edit_text("Выбери пару", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выбери TF", reply_markup=tf_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb):
    _, pair, tf = cb.data.split(":")
    df = yf.download(pair, interval=f"{tf}m", period="5d", progress=False)
    direction, conf, expl = get_signal(df)

    tid = await save_trade(cb.from_user.id, pair.replace("=X",""), int(tf), direction, conf, expl)

    await cb.message.edit_text(
        f"📊 {pair.replace('=X','')}\nTF {tf}m\n\n{direction}\nУверенность {conf}%",
    )

@dp.callback_query(lambda c: c.data == "history")
async def history(cb):
    trades = await get_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("История пуста")
        return
    txt = ""
    for t in trades:
        txt += f"{t['pair']} {t['direction']} {t['confidence']}%\n"
    await cb.message.answer(txt)

# ===================== POSTBACK =====================
async def handle_postback(req):
    try:
        amount = float(req.query.get("amount", 0))
    except:
        amount = 0
    click_id = req.query.get("click_id")
    if click_id and amount > 0:
        await add_user(int(click_id))
        await update_balance(int(click_id), amount)
    return web.Response(text="OK")

# ===================== MAIN =====================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", handle_postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, HOST, PORT).start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
