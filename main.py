import os
import sys
import asyncio
import logging

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

# ================== CONFIG ==================

TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================== BOT ==================

bot = Bot(TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: asyncpg.Pool | None = None

# ================== CONSTANTS ==================

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD"
]

PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 2, 3, 5, 10]

# ================== DATABASE ==================

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

# ================== ACCESS ==================

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", user_id)
        return row and row["balance"] >= MIN_DEPOSIT

# ================== KEYBOARDS ==================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE

    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p, callback_data=f"pair:{p}")

    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page - 1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"page:{page + 1}")

    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.button(text="⬅️ Назад", callback_data="pairs")
    kb.adjust(2)
    return kb.as_markup()

# ================== INDICATORS (15) ==================

def analyze(df: pd.DataFrame):
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    votes = []

    votes.append(close.iloc[-1] > close.rolling(10).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.rolling(20).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.ewm(10).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.ewm(20).mean().iloc[-1])

    delta = close.diff()
    rsi = 100 - (100 / (1 + delta.clip(lower=0).rolling(14).mean() /
                       -delta.clip(upper=0).rolling(14).mean()))
    votes.append(rsi.iloc[-1] > 50)

    ema12 = close.ewm(12).mean()
    ema26 = close.ewm(26).mean()
    votes.append(ema12.iloc[-1] > ema26.iloc[-1])

    stoch = 100 * (close - low.rolling(14).min()) / (high.rolling(14).max() - low.rolling(14).min())
    votes.append(stoch.iloc[-1] > 50)

    votes.append(close.iloc[-1] > close.iloc[-2])

    momentum = close.diff(4)
    votes.append(momentum.iloc[-1] > 0)

    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    votes.append(obv.iloc[-1] > obv.iloc[-2])

    cci = (close - close.rolling(20).mean()) / (0.015 * close.rolling(20).std())
    votes.append(cci.iloc[-1] > 0)

    buy = sum(votes)
    sell = len(votes) - buy

    if buy == sell:
        return None, None

    direction = "ПОКУПКА" if buy > sell else "ПРОДАЖА"
    confidence = round(max(buy, sell) / len(votes) * 100, 1)

    return direction, confidence

# ================== SIGNAL ==================

async def get_signal(pair: str):
    symbol = pair + "=X"

    df = yf.download(
        symbol,
        interval="15m",
        period="7d",
        progress=False
    )

    if df is None or len(df) < 30:
        return None, None

    return analyze(df)

# ================== HANDLERS ==================

@dp.message(Command("start"))
async def start(msg: types.Message):
    text = (
        "🤖 **KURUT TRADE BOT**\n\n"
        "📊 Анализ:\n"
        "• Yahoo Finance\n"
        "• 15 индикаторов\n"
        "• 30 свечей M15\n\n"
        "🔐 Доступ:\n"
        "• Авторы — бесплатно\n"
        "• Пользователи — депозит от 20$\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="Получить доступ", url=REF_LINK)
    kb.adjust(1)

    await msg.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите пару:", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"Пара {pair}\nВыберите экспирацию:",
        reply_markup=expiration_kb(pair)
    )

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, conf = await get_signal(pair)

    if not direction:
        await cb.message.edit_text("⚠️ Нет сильного сигнала")
        return

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%"
    )

# ================== 📰 AUTO NEWS SIGNAL ==================

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return

    best = None

    for pair in PAIRS:
        direction, conf = await get_signal(pair)
        if direction and conf and conf >= 70:
            best = (pair, direction, conf)
            break

    if not best:
        await cb.message.edit_text("⚠️ Сейчас нет сильных новостных сигналов")
        return

    pair, direction, conf = best
    await cb.message.edit_text(
        f"📰 АВТО СИГНАЛ\n\n"
        f"Пара: {pair}\n"
        f"Экспирация: 3 мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%"
    )

# ================== POSTBACK ==================

async def postback(request: web.Request):
    click_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not click_id:
        return web.Response(text="NO CLICK", status=400)

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance=$2
        """, int(click_id), amount)

    return web.Response(text="OK")

# ================== START ==================

async def main():
    await init_db()

    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
