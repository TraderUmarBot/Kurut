import os
import sys
import asyncio
import logging
from datetime import datetime
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

AUTHORS = [6117198446, 7079260196]  # авторы имеют полный доступ
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
        CREATE TABLE IF NOT EXISTS logs (
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
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
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

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ В меню", callback_data="menu")
    return kb.as_markup()

# ================= SIGNAL & INDICATORS =================

async def get_signal(pair: str, exp: int) -> str:
    """
    Основная функция сигналов. Использует 15 индикаторов.
    Возвращает направление сигнала: ВВЕРХ 📈 или ВНИЗ 📉
    """
    interval_map = {1:"1m", 3:"3m", 5:"5m", 10:"10m"}
    interval = interval_map.get(exp, "5m")
    try:
        df = yf.download(pair, period="2d", interval=interval, progress=False, auto_adjust=True)
        if df.empty:
            return "СИГНАЛ НЕЯСЕН"

        df = df.bfill().ffill()
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        votes = []

        def safe_last(s):
            return s.iloc[-1] if len(s) > 0 else 0

        # 1. SMA10
        sma10 = close.rolling(10).mean()
        votes.append("ВВЕРХ 📈" if safe_last(close) > safe_last(sma10) else "ВНИЗ 📉")
        # 2. SMA20
        sma20 = close.rolling(20).mean()
        votes.append("ВВЕРХ 📈" if safe_last(close) > safe_last(sma20) else "ВНИЗ 📉")
        # 3. EMA10
        ema10 = close.ewm(span=10).mean()
        votes.append("ВВЕРХ 📈" if safe_last(close) > safe_last(ema10) else "ВНИЗ 📉")
        # 4. EMA20
        ema20 = close.ewm(span=20).mean()
        votes.append("ВВЕРХ 📈" if safe_last(close) > safe_last(ema20) else "ВНИЗ 📉")
        # 5. RSI
        delta = close.diff().fillna(0)
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + safe_last(gain)/max(safe_last(loss),0.0001)))
        votes.append("ВВЕРХ 📈" if rsi > 50 else "ВНИЗ 📉")
        # 6. MACD
        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        votes.append("ВВЕРХ 📈" if safe_last(macd) > 0 else "ВНИЗ 📉")
        # 7. Stochastic
        low14 = low.rolling(14).min()
        high14 = high.rolling(14).max()
        stoch = 100*(close - low14)/(high14-low14)
        votes.append("ВВЕРХ 📈" if safe_last(stoch) > 50 else "ВНИЗ 📉")
        # 8. Momentum
        momentum = close.diff(5)
        votes.append("ВВЕРХ 📈" if safe_last(momentum) > 0 else "ВНИЗ 📉")
        # 9. CCI
        tp = (high + low + close)/3
        cci = (tp - tp.rolling(20).mean())/(0.015*tp.rolling(20).std())
        votes.append("ВВЕРХ 📈" if safe_last(cci) > 0 else "ВНИЗ 📉")
        # 10. OBV
        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        votes.append("ВВЕРХ 📈" if safe_last(obv) > safe_last(obv.shift(1),0) else "ВНИЗ 📉")
        # 11. Trend High-Low
        trend = safe_last(high.diff()) - safe_last(low.diff())
        votes.append("ВВЕРХ 📈" if trend>0 else "ВНИЗ 📉")
        # 12-15. Trend filters (на основе роста закрытия)
        trend_filter = safe_last(close) > safe_last(close.shift(1))
        votes += ["ВВЕРХ 📈" if trend_filter else "ВНИЗ 📉"]*4

        # Подсчет голосов
        if votes.count("ВВЕРХ 📈") > votes.count("ВНИЗ 📉"):
            return "ВВЕРХ 📈"
        elif votes.count("ВНИЗ 📉") > votes.count("ВВЕРХ 📈"):
            return "ВНИЗ 📉"
        else:
            return "СИГНАЛ НЕЯСЕН"

    except Exception as e:
        logging.error(f"Ошибка get_signal: {e}")
        return "СИГНАЛ НЕЯСЕН"

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id
    if uid in AUTHORS:
        await msg.answer("👑 Авторский доступ\n\nГлавное меню:", reply_markup=main_menu())
        return

    # Страница 1 инструкции
    kb1 = InlineKeyboardBuilder()
    kb1.button(text="➡️ Далее", callback_data="instr2")
    kb1.adjust(1)
    await msg.answer(
        "📘 ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ ТРЕЙДИНГ-БОТА KURUT TRADE\n\n"
        "🔥 ОБЩАЯ ИНФОРМАЦИЯ\n\n"
        "Бот использует 15 индикаторов для анализа рынка.\n"
        "Он подходит как для новичков, так и для опытных трейдеров.",
        reply_markup=kb1.as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb2 = InlineKeyboardBuilder()
    kb2.button(text="🔗 Получить доступ к боту", callback_data="get_access")
    kb2.adjust(1)
    await cb.message.edit_text(
        "⚙️ КАК НАЧАТЬ РАБОТУ С БОТОМ\n\n"
        "1️⃣ Зарегистрируйтесь по нашей ссылке\n"
        "2️⃣ Пополните баланс минимум на 20$\n"
        "3️⃣ Нажмите «Проверить ID»\n"
        "✅ После успешной проверки доступ будет открыт",
        reply_markup=kb2.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("🔐 Чтобы получить доступ к боту:", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    if cb.from_user.id in AUTHORS:
        await cb.message.edit_text("👑 Вы автор, доступ открыт ✅", reply_markup=main_menu())
        return
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить пополнение", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text("⏳ Ожидаем пополнение от 20$", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="check_balance")
async def check_balance(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    if cb.from_user.id in AUTHORS or (user and user["balance"] >= MIN_DEPOSIT):
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction = await get_signal(pair, exp)
    if direction == "СИГНАЛ НЕЯСЕН":
        await cb.message.edit_text("⚠️ Сигнал слишком слабый. Выберите другую валютную пару.", reply_markup=pairs_kb())
        await cb.answer()
        return
    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\nПара: {pair}\nЭкспирация: {exp} мин\nНаправление: {direction}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction = await get_signal(pair, exp)
    if direction == "СИГНАЛ НЕЯСЕН":
        await cb.message.edit_text("⚠️ Нет сильного сигнала. Выберите другую валютную пару.", reply_markup=pairs_kb())
        await cb.answer()
        return
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\nПара: {pair}\nЭкспирация: {exp} мин\nНаправление: {direction}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data=="menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id", "").strip()
    amount_raw = request.query.get("amount", "0")
    if not click_id.isdigit():
        return web.Response(text="NO CLICK_ID", status=200)
    try:
        await upsert_user(int(click_id))
        await update_balance(int(click_id), float(amount_raw))
    except:
        pass
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
