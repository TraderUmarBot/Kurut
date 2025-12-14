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

AUTHORS = [
    7079260196,
    6117198446
]

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV variables not set")
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

# ===================== DATABASE =====================
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

async def get_balance(user_id):
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval(
            "SELECT balance FROM users WHERE user_id=$1", user_id
        )
        return val or 0.0

async def add_user(user_id, pocket_id):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, pocket_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
            user_id, pocket_id
        )

async def update_balance(user_id, amount):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id=$2",
            amount, user_id
        )

async def save_trade(user_id, pair, tf, direction, confidence, explanation):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO trades (user_id,pair,timeframe,direction,confidence,explanation)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING id
        """, user_id, pair, tf, direction, confidence, explanation)

async def update_trade(trade_id, result):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE trades SET result=$1 WHERE id=$2",
            result, trade_id
        )

async def get_history(user_id):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch("""
            SELECT * FROM trades
            WHERE user_id=$1
            ORDER BY timestamp DESC LIMIT 20
        """, user_id)

# ===================== KEYBOARDS =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📜 История", callback_data="history")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"pairs_page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Далее", callback_data=f"pairs_page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def tf_kb(pair):
    kb = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        kb.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    kb.adjust(2)
    return kb.as_markup()

def result_kb(trade_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПЛЮС", callback_data=f"res:{trade_id}:PLUS")
    kb.button(text="❌ МИНУС", callback_data=f"res:{trade_id}:MINUS")
    kb.adjust(2)
    return kb.as_markup()

# ===================== ANALYSIS =====================
def get_signal(df):
    if df.empty:
        return "SELL", 50.0, "Нет данных"

    signals = []
    expl = []

    signals.append("BUY" if ta.sma(df.Close, 5).iloc[-1] > ta.sma(df.Close, 20).iloc[-1] else "SELL")
    expl.append("SMA")

    signals.append("BUY" if ta.ema(df.Close, 5).iloc[-1] > ta.ema(df.Close, 20).iloc[-1] else "SELL")
    expl.append("EMA")

    rsi = ta.rsi(df.Close).iloc[-1]
    signals.append("BUY" if rsi < 30 else "SELL" if rsi > 70 else "BUY")
    expl.append("RSI")

    macd = ta.macd(df.Close)
    signals.append("BUY" if macd.iloc[-1,0] > macd.iloc[-1,1] else "SELL")
    expl.append("MACD")

    counter = Counter(signals)
    direction, count = counter.most_common(1)[0]
    confidence = round(count / len(signals) * 100, 1)

    return direction, confidence, " | ".join(expl)

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id

    if uid in AUTHORS:
        await msg.answer("🏠 Главное меню (Автор)", reply_markup=main_menu())
        return

    balance = await get_balance(uid)
    if balance < MIN_DEPOSIT:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Я пополнил", callback_data="check_deposit")
        await msg.answer(
            f"Минимальный депозит ${MIN_DEPOSIT}\nРегистрация:\n{REF_LINK}",
            reply_markup=kb.as_markup()
        )
    else:
        await msg.answer("🏠 Главное меню", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("Выбери пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(f"{pair.replace('=X','')} — выбери TF", reply_markup=tf_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb: types.CallbackQuery):
    _, pair, tf = cb.data.split(":")
    df = yf.download(pair, period="2d", interval=f"{tf}m", progress=False)

    direction, confidence, expl = get_signal(df)
    trade_id = await save_trade(cb.from_user.id, pair.replace("=X",""), int(tf), direction, confidence, expl)

    await cb.message.edit_text(
        f"Пара: {pair.replace('=X','')}\nTF: {tf} мин\nСигнал: {direction}\nУверенность: {confidence}%\n{expl}",
        reply_markup=result_kb(trade_id)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    _, tid, r = cb.data.split(":")
    await update_trade(int(tid), r)
    await cb.message.edit_text("Результат сохранён", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "history")
async def history(cb: types.CallbackQuery):
    trades = await get_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("История пуста")
        return
    text = "История:\n"
    for t in trades:
        text += f"{t['timestamp']} | {t['pair']} | {t['direction']} | {t['result']}\n"
    await cb.message.answer(text)

# ===================== POSTBACK =====================
async def handle_postback(request):
    event = request.query.get("event")
    click_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not click_id:
        return web.Response(text="NO CLICK_ID", status=400)

    user_id = int(click_id)
    await add_user(user_id, click_id)

    if event == "deposit" and amount > 0:
        await update_balance(user_id, amount)

    return web.Response(text="OK")

# ===================== WEBHOOK =====================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(WEBHOOK_URL))

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", handle_postback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
