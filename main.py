# ================= IMPORTS =================
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
        await conn.execute(
            "UPDATE users SET balance=$1 WHERE user_id=$2",
            amount, user_id
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

# ================= SIGNAL CORE =================

def last(series, default=0.0):
    if series is None or len(series) == 0:
        return default
    val = series.iloc[-1]
    return float(val) if pd.notna(val) else default

async def get_signal(pair: str, exp: int) -> Tuple[str, str, int]:
    try:
        interval_map = {1:"1m", 3:"3m", 5:"5m", 10:"15m"}
        interval = interval_map.get(exp, "5m")

        df = yf.download(
            pair,
            period="2d",
            interval=interval,
            progress=False,
            auto_adjust=True
        )

        if df.empty or len(df) < 50:
            return "СИГНАЛ НЕДОСТУПЕН", "❌ Недостаточно данных", 0

        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        score = 0
        total = 15

        # ===== TREND =====
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        ema200 = close.ewm(span=200).mean()

        if last(ema50) > last(ema200): score += 1
        if last(ema20) > last(ema50): score += 1

        # ADX
        tr = (high - low).abs()
        adx = tr.rolling(14).mean()
        if last(adx) > 20: score += 1

        # ===== MOMENTUM =====
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = last(gain) / max(last(loss), 1e-6)
        rsi = 100 - (100 / (1 + rs))
        if rsi > 55: score += 1

        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        if last(macd) > 0: score += 1

        momentum = close.diff(5)
        if last(momentum) > 0: score += 1

        # ===== VOLATILITY =====
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_low = bb_mid - 2 * bb_std
        if last(close) > last(bb_low): score += 1

        atr = tr.rolling(14).mean()
        if last(atr) > 0: score += 1

        # ===== VOLUME =====
        vol_ma = volume.rolling(20).mean()
        if last(volume) > last(vol_ma): score += 1

        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        if last(obv) > last(obv.shift(1)): score += 1

        # ===== EXTRA FILTERS =====
        if last(close) > last(close.shift(1)): score += 1
        if last(high.diff()) > 0: score += 1
        if last(low.diff()) > 0: score += 1
        if rsi < 70: score += 1

        # ===== RESULT =====
        direction = "ВВЕРХ 📈" if score >= total/2 else "ВНИЗ 📉"

        if score >= 13:
            strength = "🔥🔥🔥🔥🔥 ОЧЕНЬ СИЛЬНЫЙ"
        elif score >= 10:
            strength = "🔥🔥🔥🔥 СИЛЬНЫЙ"
        elif score >= 7:
            strength = "🔥🔥🔥 СРЕДНИЙ"
        else:
            strength = "🔥🔥 СЛАБЫЙ"

        return direction, strength, score

    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "СИГНАЛ НЕДОСТУПЕН", "❌ Ошибка анализа", 0

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id
    if uid in AUTHORS:
        await msg.answer("👑 Авторский доступ\n\nГлавное меню:", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr2")
    kb.adjust(1)
    await msg.answer(
        "📘 ИНСТРУКЦИЯ KURUT TRADE\n\n"
        "Бот анализирует рынок по 15 индикаторам\n"
        "и использует 3 стратегии.\n\n"
        "Нажмите далее 👇",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    kb.adjust(1)
    await cb.message.edit_text(
        "1️⃣ Зарегистрируйтесь по ссылке\n"
        "2️⃣ Пополните баланс от 20$\n"
        "3️⃣ Проверьте ID\n\n"
        "Авторы — без ограничений",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("🔐 Доступ к боту:", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    if cb.from_user.id in AUTHORS or (user and user["balance"] >= MIN_DEPOSIT):
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Экспирация:", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, strength, score = await get_signal(pair, int(exp))

    await cb.message.edit_text(
        f"📊 СИГНАЛ KURUT TRADE\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n\n"
        f"Сила: {strength}\n"
        f"Индикаторы: {score}/15",
        reply_markup=main_menu()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, strength, score = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\n"
        f"{pair.replace('=X','')} | {exp} мин\n"
        f"{direction}\n{strength}",
        reply_markup=main_menu()
    )
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
    except Exception as e:
        logging.error(f"Postback error: {e}")

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
