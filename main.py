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
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ================== CONFIG ==================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"
AUTHORS = [7079260196, 6117198446]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================== BOT ==================
bot = Bot(TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL = None

# ================== PAIRS ==================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = [1, 2, 3, 5, 10]

# ================== DB ==================
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

async def get_balance(user_id: int) -> float:
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return float(val or 0)

async def add_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = users.balance + $2
        """, user_id, amount)

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

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(3)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Плюс", callback_data="menu")
    kb.button(text="➖ Минус", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================== SIGNAL ENGINE (15 INDICATORS) ==================
def calculate_signal(pair: str):
    data = yf.download(pair, period="60d", interval="1h", progress=False)
    if data.empty or len(data) < 50:
        return "NEUTRAL", 50.0, "Недостаточно данных"

    close = data["Close"]
    high = data["High"]
    low = data["Low"]

    votes = []

    # 1-3 SMA
    for p in (10, 20, 50):
        votes.append("BUY" if close.iloc[-1] > close.rolling(p).mean().iloc[-1] else "SELL")

    # 4-6 EMA
    for p in (10, 20, 50):
        votes.append("BUY" if close.iloc[-1] > close.ewm(p).mean().iloc[-1] else "SELL")

    # 7 RSI
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    if rsi.iloc[-1] < 30:
        votes.append("BUY")
    elif rsi.iloc[-1] > 70:
        votes.append("SELL")

    # 8 MACD
    macd = close.ewm(12).mean() - close.ewm(26).mean()
    signal = macd.ewm(9).mean()
    votes.append("BUY" if macd.iloc[-1] > signal.iloc[-1] else "SELL")

    # 9 Bollinger
    sma = close.rolling(20).mean()
    std = close.rolling(20).std()
    if close.iloc[-1] < (sma - 2 * std).iloc[-1]:
        votes.append("BUY")
    elif close.iloc[-1] > (sma + 2 * std).iloc[-1]:
        votes.append("SELL")

    # 10 Momentum
    votes.append("BUY" if close.iloc[-1] > close.iloc[-10] else "SELL")

    # 11-12 Stochastic
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    stoch = (close - low14) / (high14 - low14) * 100
    if stoch.iloc[-1] < 20:
        votes.append("BUY")
    elif stoch.iloc[-1] > 80:
        votes.append("SELL")

    # 13 ATR trend
    atr = (high - low).rolling(14).mean()
    votes.append("BUY" if atr.iloc[-1] > atr.mean() else "SELL")

    # 14-15 Price Action
    votes.append("BUY" if close.iloc[-1] > close.iloc[-2] else "SELL")
    votes.append("BUY" if close.iloc[-2] > close.iloc[-3] else "SELL")

    buy = votes.count("BUY")
    sell = votes.count("SELL")
    total = buy + sell

    if total == 0:
        return "NEUTRAL", 50.0, "Нет сильного тренда"

    direction = "BUY" if buy > sell else "SELL"
    confidence = round(max(buy, sell) / total * 100, 2)

    if confidence < 60:
        return "NEUTRAL", confidence, "Слабый сигнал"

    return direction, confidence, f"Индикаторы: BUY={buy} SELL={sell}"

# ================== HANDLERS ==================
@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id

    if uid in AUTHORS:
        await msg.answer("🏠 Главное меню (Автор)", reply_markup=main_menu())
        return

    text = (
        "🤖 Торговый бот технического анализа\n\n"
        "📊 Использует 15 индикаторов\n"
        "⏱ Анализирует свежие свечи рынка\n"
        "⚡ Даёт сигналы по валютным парам\n\n"
        "Для доступа необходимо:\n"
        "1️⃣ Зарегистрироваться\n"
        "2️⃣ Пополнить баланс от 20$\n\n"
        "Нажми продолжить ⬇️"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="Продолжить", callback_data="reg")
    await msg.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "reg")
async def reg(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Я пополнил баланс", callback_data="check")
    kb.adjust(1)
    await cb.message.edit_text("Перейди по ссылке и пополни баланс ⬇️", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check")
async def check(cb: types.CallbackQuery):
    bal = await get_balance(cb.from_user.id)
    if bal >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс не найден", show_alert=True)

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("⏱ Выбери экспирацию", reply_markup=expiration_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, e = cb.data.split(":")
    direction, conf, info = calculate_signal(pair)

    await cb.message.edit_text(
        f"📊 Сигнал\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Экспирация: {e} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%\n\n"
        f"{info}",
        reply_markup=result_kb()
    )

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, info = calculate_signal(pair)

    await cb.message.edit_text(
        f"📰 Авто-сигнал\n\n"
        f"{pair.replace('=X','')}\n"
        f"{exp} мин\n"
        f"{direction}\n"
        f"{conf}%\n\n"
        f"{info}",
        reply_markup=result_kb()
    )

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())

# ================== POSTBACK ==================
async def postback(request: web.Request):
    amount_raw = request.query.get("amount", "0")
    try:
        amount = float(amount_raw)
    except:
        amount = 0

    click_id = request.query.get("click_id")
    if click_id and amount > 0:
        try:
            await add_balance(int(click_id), amount)
        except:
            pass

    return web.Response(text="OK")

# ================== MAIN ==================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
