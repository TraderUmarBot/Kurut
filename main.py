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
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ================= CONFIG =================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")

PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

MIN_DEPOSIT = 20.0

AUTHORS = [
    7079260196,
    6117198446
]

REF_LINK = (
    "https://po-ru4.click/register?"
    "utm_campaign=XXXX&utm_source=affiliate&utm_medium=sr&sub_id1={tg_id}"
)

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = [1, 3, 5, 10]

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    sys.exit("ENV ERROR")

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

bot = Bot(TG_TOKEN)
dp = Dispatcher()
DB: asyncpg.Pool | None = None

# ================= DB =================
async def init_db():
    global DB
    DB = await asyncpg.create_pool(DATABASE_URL)
    async with DB.acquire() as conn:
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
            timeframe TEXT,
            direction TEXT,
            confidence INT,
            created_at TIMESTAMP
        );
        """)

async def add_balance(user_id: int, amount: float):
    async with DB.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1,$2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = users.balance + $2
        """, user_id, amount)

async def get_balance(user_id: int) -> float:
    async with DB.acquire() as conn:
        val = await conn.fetchval(
            "SELECT balance FROM users WHERE user_id=$1",
            user_id
        )
        return val or 0.0

async def log_signal(user_id, pair, tf, direction, confidence):
    async with DB.acquire() as conn:
        await conn.execute("""
        INSERT INTO signals (user_id,pair,timeframe,direction,confidence,created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        """, user_id, pair, tf, direction, confidence, datetime.utcnow())

# ================= KEYBOARDS =================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Авто-сигнал", callback_data="news")
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
    kb.adjust(2)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ ПЛЮС", callback_data="menu")
    kb.button(text="➖ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================= ANALYSIS =================
def choose_tf(exp):
    if exp <= 3:
        return "5m"
    if exp == 5:
        return "15m"
    return "30m"

def analyze(pair: str, exp: int):
    tf = choose_tf(exp)

    data = yf.download(pair, period="90d", interval=tf, progress=False)
    if data.empty or len(data) < 200:
        return "ПОКУПКА", 65, tf, "Недостаточно данных, приоритет по тренду"

    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    volume = data["Volume"]

    score = 0
    reasons = []

    ema20 = close.ewm(span=20).mean()
    ema50 = close.ewm(span=50).mean()
    ema100 = close.ewm(span=100).mean()
    ema200 = close.ewm(span=200).mean()

    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    if ema50.iloc[-1] > ema200.iloc[-1]: score += 1; reasons.append("EMA 50 выше EMA 200")
    else: score -= 1; reasons.append("EMA 50 ниже EMA 200")

    if sma50.iloc[-1] > sma200.iloc[-1]: score += 1
    else: score -= 1

    rsi = 100 - (100 / (1 + close.diff().clip(lower=0).rolling(14).mean() /
                       close.diff().clip(upper=0).abs().rolling(14).mean()))
    if rsi.iloc[-1] > 55: score += 1
    else: score -= 1

    macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
    signal = macd.ewm(span=9).mean()
    if macd.iloc[-1] > signal.iloc[-1]: score += 1
    else: score -= 1

    atr = (high - low).rolling(14).mean()
    if atr.iloc[-1] > atr.mean(): score += 1
    else: score -= 1

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    if close.iloc[-1] < bb_upper.iloc[-1]: score += 1
    else: score -= 1

    momentum = close.diff(10)
    if momentum.iloc[-1] > 0: score += 1
    else: score -= 1

    vol_trend = volume.diff().rolling(10).mean()
    if vol_trend.iloc[-1] > 0: score += 1
    else: score -= 1

    direction = "ПОКУПКА" if score > 0 else "ПРОДАЖА"
    confidence = min(95, int(abs(score) / 15 * 100))

    explanation = "\n".join(reasons[:5])

    return direction, confidence, tf, explanation

# ================= START =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id

    if uid in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Получить доступ", callback_data="access")

    await msg.answer(
        "🤖 Торговый бот\n\n"
        "📊 Анализ по 15 индикаторам\n"
        "⏱ Авто-таймфрейм\n"
        "📈 Только ПОКУПКА / ПРОДАЖА\n\n"
        "Нажмите ниже",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "access")
async def access(cb: types.CallbackQuery):
    link = REF_LINK.format(tg_id=cb.from_user.id)

    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=link)
    kb.button(text="✅ Проверить доступ", callback_data="check")
    kb.adjust(1)

    await cb.message.answer(
        "1️⃣ Регистрация по ссылке\n"
        "2️⃣ Пополнение от 20$\n"
        "3️⃣ Проверка доступа",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check")
async def check(cb: types.CallbackQuery):
    bal = await get_balance(cb.from_user.id)
    if bal >= MIN_DEPOSIT:
        await cb.message.answer("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.message.answer("❌ Баланс меньше 20$")
    await cb.answer()

# ================= SIGNAL FLOW =================
@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выберите пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ {pair.replace('=X','')}",
        reply_markup=exp_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, e = cb.data.split(":")
    e = int(e)

    direction, conf, tf, expl = analyze(pair, e)

    await log_signal(
        cb.from_user.id,
        pair.replace("=X",""),
        tf,
        direction,
        conf
    )

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Экспирация: {e} мин\n"
        f"Таймфрейм: {tf}\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)

    direction, conf, tf, expl = analyze(pair, exp)

    await cb.message.edit_text(
        f"📰 АВТО-СИГНАЛ\n\n"
        f"{pair.replace('=X','')} | {exp} мин\n"
        f"{direction} | {conf}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= POSTBACK =================
async def postback(request: web.Request):
    try:
        tg_id = int(request.query.get("sub_id1"))
        amount = float(request.query.get("amount", 0))
        event = request.query.get("event")
    except:
        return web.Response(text="ERROR")

    if event == "deposit" and amount > 0:
        await add_balance(tg_id, amount)

    return web.Response(text="OK")

# ================= WEB =================
async def main():
    await init_db()

    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, HOST, PORT).start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
