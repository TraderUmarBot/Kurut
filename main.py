import os
import sys
import asyncio
import logging
from datetime import datetime

import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ================= CONFIG =================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV variables not set")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= BOT =================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher()
DB_POOL: asyncpg.Pool | None = None

# ================= DATA =================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 2, 3, 5, 10]

# ================= DATABASE =================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT,
            balance FLOAT DEFAULT 0,
            registered BOOLEAN DEFAULT FALSE
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            confidence FLOAT,
            expiration INT,
            created_at TIMESTAMP DEFAULT now()
        );
        """)

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM users WHERE user_id=$1", user_id
        )
        return row and row["balance"] >= MIN_DEPOSIT

# ================= KEYBOARDS =================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Получить сигнал", callback_data="pairs:0")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page: int):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE

    for pair in PAIRS[start:end]:
        kb.button(text=pair.replace("=X", ""), callback_data=f"pair:{pair}")

    if page > 0:
        kb.button(text="⬅ Назад", callback_data=f"pairs:{page-1}")
    if end < len(PAIRS):
        kb.button(text="➡ Вперёд", callback_data=f"pairs:{page+1}")

    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair: str):
    kb = InlineKeyboardBuilder()
    for exp in EXPIRATIONS:
        kb.button(text=f"{exp} мин", callback_data=f"exp:{pair}:{exp}")
    kb.adjust(2)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПЛЮС", callback_data="menu")
    kb.button(text="❌ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================= INDICATORS =================
def calculate_indicators(df: pd.DataFrame):
    votes = []

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    votes.append("BUY" if close.iloc[-1] > close.rolling(10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.rolling(20).mean().iloc[-1] else "SELL")

    votes.append("BUY" if close.iloc[-1] > close.ewm(span=10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.ewm(span=20).mean().iloc[-1] else "SELL")

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    votes.append("BUY" if rsi.iloc[-1] > 50 else "SELL")

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    votes.append("BUY" if macd.iloc[-1] > signal.iloc[-1] else "SELL")

    sma = close.rolling(20).mean()
    votes.append("BUY" if close.iloc[-1] > sma.iloc[-1] else "SELL")

    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    stoch = 100 * (close - low14) / (high14 - low14)
    votes.append("BUY" if stoch.iloc[-1] > 50 else "SELL")

    votes.append("BUY" if close.iloc[-1] > close.iloc[-2] else "SELL")

    tp = (high + low + close) / 3
    cci = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())
    votes.append("BUY" if cci.iloc[-1] > 0 else "SELL")

    momentum = close.diff(4)
    votes.append("BUY" if momentum.iloc[-1] > 0 else "SELL")

    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    votes.append("BUY" if obv.iloc[-1] > obv.iloc[-2] else "SELL")

    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
    votes.append("BUY" if tenkan.iloc[-1] > kijun.iloc[-1] else "SELL")

    return votes

# ================= SIGNAL =================
async def get_signal(pair: str):
    data = yf.download(pair, interval="15m", period="7d", progress=False)

    if data.empty or len(data) < 30:
        return None, 0.0

    data = data.tail(30)
    votes = calculate_indicators(data)

    buy = votes.count("BUY")
    sell = votes.count("SELL")

    if buy == sell:
        return None, 0.0

    direction = "ПОКУПКА" if buy > sell else "ПРОДАЖА"
    confidence = max(buy, sell) / len(votes) * 100

    return direction, round(confidence, 2)

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    await msg.answer(
        "🤖 KURUT TRADE BOT\n\n"
        "• Анализ рынка по 15 индикаторам\n"
        "• Таймфрейм: 15 минут\n"
        "• Без рандома и нейтрала\n\n"
        "Для доступа:\n"
        "1️⃣ Зарегистрируйтесь по ссылке\n"
        "2️⃣ Пополните баланс от 20$\n"
        "3️⃣ Доступ откроется автоматически\n\n"
        f"🔗 {REF_LINK}\n\n"
        "После доступа — получите сигналы 👇",
        reply_markup=main_menu()
    )

@dp.callback_query(lambda c: c.data.startswith("pairs"))
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return

    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите валютную пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}\nВыберите экспирацию",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, confidence = await get_signal(pair)

    if not direction:
        await cb.answer("❌ Нет сигнала", show_alert=True)
        return

    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO signals(user_id,pair,direction,confidence,expiration) VALUES($1,$2,$3,$4,$5)",
            cb.from_user.id, pair, direction, confidence, int(exp)
        )

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence}%",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= POSTBACK =================
async def postback(request: web.Request):
    click_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not click_id:
        return web.Response(text="NO CLICK_ID")

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users(user_id,balance,registered)
        VALUES($1,$2,TRUE)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = users.balance + $2
        """, int(click_id), amount)

    return web.Response(text="OK")

# ================= WEBHOOK =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, HOST, PORT).start()

    logging.info("🚀 BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
