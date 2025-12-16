import os
import sys
import asyncio
import logging
from typing import Tuple

import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf

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

HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"

AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= BOT =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: asyncpg.Pool | None = None

# ================= CONSTANTS =================

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 3, 5, 10]

# ================= DATABASE =================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            expiration INT,
            created TIMESTAMP DEFAULT now()
        );
        """)

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id) VALUES ($1)
        ON CONFLICT (user_id) DO NOTHING
        """, user_id)

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, user_id)

async def log_signal(user_id: int, pair: str, direction: str, expiration: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO signals (user_id, pair, direction, expiration) VALUES ($1,$2,$3,$4)",
            user_id, pair, direction, expiration
        )

# ================= ACCESS =================

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= KEYBOARDS =================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="🗞️ Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

def back_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ В меню", callback_data="menu")
    return kb.as_markup()

# ================= STRATEGY (MULTI TF) =================

async def get_signal(pair: str) -> str:
    intervals = ["1m", "5m", "15m"]
    results = []

    for interval in intervals:
        df = yf.download(pair, period="2d", interval=interval, progress=False)
        if df.empty:
            continue

        close = df["Close"]

        ma = close.rolling(20).mean()
        ema = close.ewm(span=20).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rsi = 100 - (100 / (gain.iloc[-1] / (loss.iloc[-1] + 1)))

        score = 0
        if close.iloc[-1] > ma.iloc[-1]: score += 1
        if close.iloc[-1] > ema.iloc[-1]: score += 1
        if rsi > 50: score += 1

        results.append("BUY" if score >= 2 else "SELL")

    if results.count("BUY") > results.count("SELL"):
        return "ВВЕРХ 📈"
    return "ВНИЗ 📉"

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr_2")
    await msg.answer("📘 ИНСТРУКЦИЯ KURUT TRADE\n\n"
                     "Бот анализирует рынок по 15 индикаторам\n"
                     "Подходит новичкам и профи\n\n"
                     "Нажмите «Далее»", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "instr_2")
async def instr_2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Получить доступ", callback_data="get_access")
    await cb.message.edit_text(
        "🔥 ЧТО УМЕЕТ БОТ\n\n"
        "✔ Анализ рынка\n"
        "✔ Направление тренда\n"
        "✔ Реальные сигналы\n"
        "✔ 24/7\n\n"
        "Нажмите «Получить доступ»",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text(
        "🔐 ДОСТУП К БОТУ\n\n"
        "1️⃣ Регистрация по ссылке\n"
        "2️⃣ Депозит от 20$\n"
        "3️⃣ Проверить ID",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить баланс", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text("⏳ Ожидаем депозит от 20$", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "check_balance")
async def check_balance(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию", reply_markup=expiration_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction = await get_signal(pair)
    await log_signal(cb.from_user.id, pair, direction, int(exp))
    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    direction = await get_signal(pair)
    await cb.message.edit_text(
        f"🗞️ НОВОСТНОЙ СИГНАЛ\n\n{pair}\nНаправление: {direction}",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id", "")
    amount = float(request.query.get("amount", "0"))
    if click_id.isdigit():
        await upsert_user(int(click_id))
        await update_balance(int(click_id), amount)
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
    await web.TCPSite(runner, HOST, PORT).start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
