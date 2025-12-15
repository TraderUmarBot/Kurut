import os
import sys
import asyncio
import logging
import random

import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [7079260196, 6117198446]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ Не заданы ENV переменные")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL = None

# ===================== PAIRS =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 3, 5, 10]

# ===================== DATABASE =====================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0
        )
        """)
    logging.info("✅ БД подключена")

async def get_balance(user_id: int) -> float:
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval(
            "SELECT balance FROM users WHERE user_id=$1",
            user_id
        )
        return val or 0.0

async def add_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = users.balance + $2
        """, user_id, amount)

# ===================== KEYBOARDS =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE

    for pair in PAIRS[start:end]:
        kb.button(
            text=pair.replace("=X", ""),
            callback_data=f"pair:{pair}"
        )

    nav = []
    if page > 0:
        nav.append(
            types.InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"pairs_page:{page-1}"
            )
        )
    if end < len(PAIRS):
        nav.append(
            types.InlineKeyboardButton(
                text="➡️ Вперёд",
                callback_data=f"pairs_page:{page+1}"
            )
        )
    if nav:
        kb.row(*nav)

    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

# ===================== SIGNAL ENGINE (15 INDICATORS) =====================
def get_signal(pair: str):
    data = yf.download(pair, period="60d", interval="1h", progress=False)
    if data.empty:
        return "NEUTRAL", 50.0

    close = data["Close"]
    high = data["High"]
    low = data["Low"]

    votes = []

    # SMA
    for p in [5, 10, 20]:
        votes.append("BUY" if close.iloc[-1] > close.rolling(p).mean().iloc[-1] else "SELL")

    # EMA
    for p in [5, 10, 20]:
        votes.append("BUY" if close.iloc[-1] > close.ewm(span=p).mean().iloc[-1] else "SELL")

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    if rsi.iloc[-1] < 30:
        votes.append("BUY")
    elif rsi.iloc[-1] > 70:
        votes.append("SELL")

    # MACD
    macd = close.ewm(12).mean() - close.ewm(26).mean()
    signal = macd.ewm(9).mean()
    votes.append("BUY" if macd.iloc[-1] > signal.iloc[-1] else "SELL")

    # Bollinger
    sma = close.rolling(20).mean()
    std = close.rolling(20).std()
    if close.iloc[-1] < sma.iloc[-1] - 2 * std.iloc[-1]:
        votes.append("BUY")
    elif close.iloc[-1] > sma.iloc[-1] + 2 * std.iloc[-1]:
        votes.append("SELL")

    buy = votes.count("BUY")
    sell = votes.count("SELL")

    if buy > sell:
        return "BUY", buy / (buy + sell) * 100
    elif sell > buy:
        return "SELL", sell / (buy + sell) * 100
    else:
        return "NEUTRAL", 50.0

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id

    if uid in AUTHORS:
        await msg.answer(
            "👑 Авторский доступ\n\nВсе функции открыты",
            reply_markup=main_menu()
        )
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Начать", callback_data="begin")
    await msg.answer(
        "👋 Привет!\n\n"
        "Я бот с сильными торговыми сигналами 📊\n\n"
        "Чтобы получить доступ:\n"
        "1️⃣ Зарегистрируйся\n"
        "2️⃣ Пополни баланс\n"
        "3️⃣ Получай сигналы\n\n"
        "Нажми кнопку ниже 👇",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin")
async def begin(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить доступ", callback_data="check")
    kb.adjust(1)
    await cb.message.answer(
        "📌 Инструкция:\n\n"
        "1️⃣ Зарегистрируйся по ссылке\n"
        f"2️⃣ Пополни минимум {MIN_DEPOSIT}$\n"
        "3️⃣ Нажми Проверить доступ\n\n"
        "После этого сигналы откроются 🔥",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check")
async def check(cb: types.CallbackQuery):
    bal = await get_balance(cb.from_user.id)
    if bal >= MIN_DEPOSIT:
        await cb.message.answer("✅ Доступ открыт!", reply_markup=main_menu())
    else:
        await cb.message.answer("❌ Баланс не найден")
    await cb.answer()

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📈 Выбери валютную пару",
        reply_markup=pairs_kb(0)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text(
        "📈 Выбери валютную пару",
        reply_markup=pairs_kb(page)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ {pair.replace('=X','')} — выбери экспирацию",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, mins = cb.data.split(":")
    direction, conf = get_signal(pair)

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Экспирация: {mins} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%"
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    direction, conf = get_signal(pair)
    await cb.message.answer(
        f"📰 Новостной сигнал\n\n"
        f"{pair.replace('=X','')}\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%"
    )
    await cb.answer()

# ===================== POSTBACK =====================
async def postback(request):
    user_id = int(request.query.get("click_id", 0))
    amount = float(request.query.get("amount", 0))
    if user_id and amount > 0:
        await add_balance(user_id, amount)
    return web.Response(text="OK")

# ===================== START =====================
async def main():
    await init_db()

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
