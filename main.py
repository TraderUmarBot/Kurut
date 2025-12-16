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

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: asyncpg.Pool | None = None


# ================= CONSTANTS =================

PAIRS = {
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "USDJPY=X",
    "AUDUSD": "AUDUSD=X",
    "USDCAD": "USDCAD=X",
    "USDCHF": "USDCHF=X"
}

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


async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE
        SET balance = EXCLUDED.balance
        """, user_id, amount)


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
    kb.adjust(1)
    return kb.as_markup()


def pairs_kb():
    kb = InlineKeyboardBuilder()
    for p in PAIRS.keys():
        kb.button(text=p, callback_data=f"pair:{p}")
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
    close = df["Close"]

    votes = []

    sma10 = close.rolling(10).mean().iloc[-1]
    sma20 = close.rolling(20).mean().iloc[-1]
    ema10 = close.ewm(span=10).mean().iloc[-1]
    ema20 = close.ewm(span=20).mean().iloc[-1]

    votes.append("BUY" if close.iloc[-1] > sma10 else "SELL")
    votes.append("BUY" if close.iloc[-1] > sma20 else "SELL")
    votes.append("BUY" if close.iloc[-1] > ema10 else "SELL")
    votes.append("BUY" if close.iloc[-1] > ema20 else "SELL")

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    votes.append("BUY" if rsi.iloc[-1] > 50 else "SELL")

    votes.append("BUY" if close.iloc[-1] > close.iloc[-2] else "SELL")

    return votes


# ================= SIGNAL =================

async def get_signal(pair: str):
    symbol = PAIRS[pair]

    df = yf.download(
        symbol,
        interval="15m",
        period="7d",
        progress=False
    )

    if df.empty or len(df) < 30:
        return None, None

    df = df.tail(30)

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
    if msg.from_user.id in AUTHORS:
        await msg.answer(
            "👑 Авторский доступ\n\n"
            "Полный доступ открыт.",
            reply_markup=main_menu()
        )
        return

    text = (
        "🤖 KURUT TRADE BOT\n\n"
        "📊 Реальные сигналы\n"
        "⏱ 15 минут / 30 свечей\n\n"
        "🔐 Доступ:\n"
        "• Регистрация по ссылке\n"
        "• Депозит от 20$\n"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Получить доступ", url=REF_LINK)
    kb.adjust(1)

    await msg.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return

    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb())
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def choose_pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"{pair}\nВыберите экспирацию",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def signal(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)

    direction, confidence = await get_signal(pair)

    if not direction:
        await cb.message.edit_text("⚠️ Нет сильного сигнала")
        await cb.answer()
        return

    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO logs (user_id, pair, direction, confidence, expiration)
        VALUES ($1,$2,$3,$4,$5)
        """, cb.from_user.id, pair, direction, confidence, exp)

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"{pair}\n"
        f"Экспирация: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence}%",
        reply_markup=result_kb()
    )
    await cb.answer()


@dp.callback_query(lambda c: c.data == "menu")
async def back(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())
    await cb.answer()


# ================= POSTBACK =================

async def postback(request: web.Request):
    user_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not user_id:
        return web.Response(text="NO CLICK", status=400)

    await update_balance(int(user_id), amount)
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
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
