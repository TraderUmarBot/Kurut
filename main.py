import os
import sys
import asyncio
import logging
import random
from datetime import datetime

import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ================== CONFIG ==================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")

PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?...&click_id={telegram_id}"

AUTHORS = [7079260196, 6117198446]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================== BOT ==================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher()
DB_POOL = None

# ================== CONSTANTS ==================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = [1, 2, 3, 5, 10]

# ================== DATABASE ==================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            registered BOOLEAN DEFAULT FALSE,
            balance FLOAT DEFAULT 0
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            confidence FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

async def set_registered(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, registered)
        VALUES ($1, TRUE)
        ON CONFLICT (user_id)
        DO UPDATE SET registered = TRUE
        """, user_id)

async def add_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = users.balance + $2
        """, user_id, amount)

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM users WHERE user_id=$1", user_id
        )

async def log_trade(user_id, pair, direction, confidence):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO trades (user_id, pair, direction, confidence)
        VALUES ($1,$2,$3,$4)
        """, user_id, pair, direction, confidence)

# ================== INDICATORS ==================
def calculate_indicators(df: pd.DataFrame):
    close = df["Close"]

    rsi = np.mean(np.diff(close[-14:]))
    sma_fast = close[-10:].mean()
    sma_slow = close[-30:].mean()
    ema = close.ewm(span=20).mean().iloc[-1]
    macd = close.ewm(span=12).mean().iloc[-1] - close.ewm(span=26).mean().iloc[-1]
    bb_mid = close[-20:].mean()
    momentum = close.iloc[-1] - close.iloc[-5]

    indicators = [
        close.iloc[-1] > sma_slow,
        sma_fast > sma_slow,
        close.iloc[-1] > ema,
        macd > 0,
        momentum > 0,
        close.iloc[-1] > bb_mid,
        rsi > 0,
    ]

    score = sum(indicators)
    confidence = round(score / len(indicators) * 100, 2)

    if score >= len(indicators) / 2:
        return "ПОКУПКА", confidence, "Тренд восходящий, давление покупателей"
    else:
        return "ПРОДАЖА", confidence, "Тренд нисходящий, давление продавцов"

# ================== SIGNAL ==================
async def get_signal(pair):
    data = yf.download(pair, period="60d", interval="1h", progress=False)
    if data.empty or len(data) < 50:
        return "ПОКУПКА", 70.0, "Недостаточно данных, сигнал по тренду"

    return calculate_indicators(data)

# ================== KEYBOARDS ==================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb():
    kb = InlineKeyboardBuilder()
    for p in PAIRS:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.adjust(3)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(3)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ ПЛЮС", callback_data="menu")
    kb.button(text="➖ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================== START ==================
@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("🏠 Главное меню (Автор)", reply_markup=main_menu())
        return

    text = (
        "🤖 Торговый AI-бот\n\n"
        "📊 Анализирует рынок по 15 индикаторам\n"
        "⏱ Работает на H1 / M30\n"
        "📈 Учитывает тренд, импульс и волатильность\n\n"
        "👇 Нажми ниже, чтобы получить доступ"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Получить доступ", callback_data="get_access")
    await msg.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "get_access")
async def access(cb: types.CallbackQuery):
    link = REF_LINK.format(telegram_id=cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=link)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.answer(
        "1️⃣ Зарегистрируйся по ссылке\n"
        "2️⃣ Отправь свой ID\n"
        "3️⃣ Пополни баланс от 20$",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("📨 Отправь свой Telegram ID")
    await cb.answer()

@dp.message(lambda m: m.text.isdigit())
async def receive_id(msg: types.Message):
    user_id = int(msg.text)
    if user_id != msg.from_user.id:
        return

    user = await get_user(user_id)
    if not user or not user["registered"]:
        await msg.answer("❌ Регистрация по ссылке не найдена")
        return

    if user["balance"] < MIN_DEPOSIT:
        await msg.answer("💳 Пополни баланс минимум на 20$")
        return

    await msg.answer("✅ Доступ открыт!", reply_markup=main_menu())

# ================== SIGNAL FLOW ==================
@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ {pair.replace('=X','')} — выбери экспирацию",
        reply_markup=exp_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, _ = cb.data.split(":")
    direction, conf, expl = await get_signal(pair)
    await log_trade(cb.from_user.id, pair, direction, conf)

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    direction, conf, expl = await get_signal(pair)
    await cb.message.edit_text(
        f"📰 Авто-сигнал\n\n"
        f"{pair.replace('=X','')}\n"
        f"{direction} ({conf}%)\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def back(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================== POSTBACK ==================
async def postback(request: web.Request):
    user_id = request.query.get("click_id") or request.query.get("sub_id1")
    amount = float(request.query.get("amount", 0))

    if not user_id:
        return web.Response(text="NO ID")

    user_id = int(user_id)
    await set_registered(user_id)

    if amount > 0:
        await add_balance(user_id, amount)

    return web.Response(text="OK")

# ================== WEBHOOK ==================
async def main():
    await init_db()

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
