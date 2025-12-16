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
    sys.exit("ENV ERROR")

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: asyncpg.Pool | None = None


# ================= PAIRS =================

PAIRS = {
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "USDJPY=X",
    "AUDUSD": "AUDUSD=X",
    "USDCAD": "USDCAD=X",
    "USDCHF": "USDCHF=X",
    "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X",
    "EURGBP": "EURGBP=X",
    "GBPAUD": "GBPAUD=X",
    "EURAUD": "EURAUD=X",
}

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
            balance FLOAT DEFAULT 0
        );
        """)


async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1,$2)
        ON CONFLICT (user_id) DO UPDATE SET balance=$2
        """, user_id, amount)


async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", user_id)
    return bool(row and row["balance"] >= MIN_DEPOSIT)


# ================= KEYBOARDS =================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()


def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    items = list(PAIRS.keys())
    start = page * PAIRS_PER_PAGE
    for p in items[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p, callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(items):
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
    kb.button(text="✅ ПЛЮС", callback_data="menu")
    kb.button(text="❌ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()


# ================= INDICATORS =================

def analyze(df: pd.DataFrame):
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    votes = []

    # 1–3 SMA
    votes.append(close.iloc[-1] > close.rolling(10).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.rolling(20).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.rolling(50).mean().iloc[-1])

    # 4–6 EMA
    votes.append(close.iloc[-1] > close.ewm(span=10).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.ewm(span=20).mean().iloc[-1])
    votes.append(close.iloc[-1] > close.ewm(span=50).mean().iloc[-1])

    # 7 RSI
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    votes.append(rsi.iloc[-1] > 50)

    # 8 MACD
    macd = close.ewm(12).mean() - close.ewm(26).mean()
    signal = macd.ewm(9).mean()
    votes.append(macd.iloc[-1] > signal.iloc[-1])

    # 9 Momentum
    votes.append(close.diff(4).iloc[-1] > 0)

    # 10 CCI
    tp = (high + low + close) / 3
    cci = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())
    votes.append(cci.iloc[-1] > 0)

    # 11 Stochastic
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    stoch = 100 * (close - low14) / (high14 - low14)
    votes.append(stoch.iloc[-1] > 50)

    # 12 Williams %R
    willr = -100 * (high14 - close) / (high14 - low14)
    votes.append(willr.iloc[-1] > -50)

    # 13 Bollinger Bands
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper = sma20 + 2 * std20
    lower = sma20 - 2 * std20
    votes.append(close.iloc[-1] > sma20.iloc[-1])

    # 14 ATR (фильтр силы)
    tr = pd.concat([
        high - low,
        abs(high - close.shift()),
        abs(low - close.shift())
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    votes.append(atr.iloc[-1] > atr.mean())

    # 15 OBV
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    votes.append(obv.iloc[-1] > obv.iloc[-2])

    buy = votes.count(True)
    sell = votes.count(False)

    if buy == sell:
        return None, None

    direction = "ПОКУПКА" if buy > sell else "ПРОДАЖА"
    confidence = round(max(buy, sell) / 15 * 100, 1)

    return direction, confidence


# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return

    await msg.answer(
        "🤖 KURUT TRADE\n\n"
        "🔹 Анализ рынка по реальным данным\n"
        "🔹 Только сильные сигналы\n\n"
        "🔐 Доступ после депозита от 20$",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[[types.InlineKeyboardButton(text="Регистрация", url=REF_LINK)]]
        )
    )


@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.answer()
    if not await has_access(cb.from_user.id):
        await cb.message.answer("❌ Нет доступа")
        return
    await cb.message.edit_text("📈 Выберите валютную пару", reply_markup=pairs_kb())


@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    await cb.answer()
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите валютную пару", reply_markup=pairs_kb(page))


@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    await cb.answer()
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара: {pair}\nВыберите экспирацию",
        reply_markup=expiration_kb(pair)
    )


@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    await cb.answer()
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
        f"Уверенность: {conf}%",
        reply_markup=result_kb()
    )


@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    await cb.answer()
    best = await get_best_signal()
    if not best:
        await cb.message.answer("⚠️ Сейчас нет сильных новостных сигналов")
        return

    pair, direction, conf = best
    exp = 5

    await cb.message.answer(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\n"
        f"Пара: {pair}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf}%",
        reply_markup=result_kb()
    )


@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.answer()
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())


# ================= POSTBACK =================

async def postback(request: web.Request):
    user_id = int(request.query.get("click_id", 0))
    amount = float(request.query.get("amount", 0))
    if user_id:
        await update_balance(user_id, amount)
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

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
