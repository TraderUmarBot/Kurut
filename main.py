import os
import sys
import asyncio
import logging
from datetime import datetime

import asyncpg
import pandas as pd
import numpy as np

from tvDatafeed import TvDatafeed, Interval

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
    print("ENV ERROR")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= BOT =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: asyncpg.Pool | None = None
tv = TvDatafeed()


# ================= CONSTANTS =================

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
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
            balance FLOAT DEFAULT 0
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            confidence FLOAT,
            expiration INT,
            created TIMESTAMP DEFAULT now()
        );
        """)


async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM users WHERE user_id=$1", user_id
        )


async def upsert_user(user_id: int, pocket_id: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, pocket_id)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE
        SET pocket_id = EXCLUDED.pocket_id
        """, user_id, pocket_id)


async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        UPDATE users SET balance=$1 WHERE user_id=$2
        """, amount, user_id)


# ================= ACCESS =================

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True

    user = await get_user(user_id)
    if not user:
        return False

    return user["balance"] >= MIN_DEPOSIT


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
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    votes = []

    votes.append("BUY" if close.iloc[-1] > close.rolling(10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.rolling(20).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.ewm(span=10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.ewm(span=20).mean().iloc[-1] else "SELL")

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    votes.append("BUY" if rsi.iloc[-1] > 50 else "SELL")

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    votes.append("BUY" if ema12.iloc[-1] > ema26.iloc[-1] else "SELL")

    sma20 = close.rolling(20).mean()
    votes.append("BUY" if close.iloc[-1] > sma20.iloc[-1] else "SELL")

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
    df = tv.get_hist(
        symbol=pair,
        exchange="FX_IDC",
        interval=Interval.in_15_minute,
        n_bars=30
    )

    if df is None or len(df) < 30:
        return None, None

    votes = calculate_indicators(df)

    buy = votes.count("BUY")
    sell = votes.count("SELL")

    if buy == sell:
        return None, None

    direction = "ПОКУПКА" if buy > sell else "ПРОДАЖА"
    confidence = round(max(buy, sell) / len(votes) * 100, 1)

    return direction, confidence


# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    text = (
        "🤖 Бот KURUT TRADE\n\n"
        "📊 Анализ рынка:\n"
        "• TradingView данные\n"
        "• 15 индикаторов\n"
        "• 30 свечей M15\n\n"
        "📈 Сигналы:\n"
        "• ПОКУПКА / ПРОДАЖА\n"
        "• Реальная уверенность\n\n"
        "🔐 Доступ:\n"
        "• Регистрация по ссылке\n"
        "• Депозит от 20$\n"
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

    await cb.message.edit_text("Выберите валютную пару", reply_markup=pairs_kb())
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару", reply_markup=pairs_kb(page))
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def choose_pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"Пара {pair}\nВыберите экспирацию",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def signal(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)

    direction, confidence = await get_signal(pair)

    if not direction:
        await cb.message.edit_text("⚠️ Сейчас нет сильного сигнала")
        await cb.answer()
        return

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO logs (user_id, pair, direction, confidence, expiration)
        VALUES ($1,$2,$3,$4,$5)
        """, cb.from_user.id, pair, direction, confidence, exp)

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"Пара: {pair}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence}%",
        reply_markup=result_kb()
    )
    await cb.answer()


@dp.callback_query(lambda c: c.data == "menu")
async def back_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())
    await cb.answer()


# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not click_id:
        return web.Response(text="NO CLICK_ID", status=400)

    user_id = int(click_id)

    await upsert_user(user_id, pocket_id=click_id)
    await update_balance(user_id, amount)

    return web.Response(text="OK")


# ================= START =================

async def main():
    await init_db()

    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)

    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
