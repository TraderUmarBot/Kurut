import os
import sys
import asyncio
import logging
from datetime import datetime
import random

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

EXPIRATIONS = [1, 2, 3, 5, 10]
PAIRS_PER_PAGE = 6

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

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (user_id)
        VALUES ($1)
        ON CONFLICT DO NOTHING
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
    kb.button(text="🔙 В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ================= INSTRUCTION =================

INSTR_1 = """📘 ИНСТРУКЦИЯ KURUT TRADE

🔥 Автоматизированный трейдинг-бот
🔥 15 профессиональных индикаторов
🔥 Реальные стратегии команды KURUT TRADE
🔥 Подходит новичкам и профи
"""

INSTR_2 = """📊 ИНДИКАТОРЫ И СТРАТЕГИИ

• SMA / EMA
• RSI / MACD
• Stochastic
• Momentum / CCI
• OBV / ADX
• Тренд + объём + фильтры

Бот отсекает слабые сигналы.
"""

INSTR_3 = """🔐 КАК ПОЛУЧИТЬ ДОСТУП

1️⃣ Регистрация по нашей ссылке
2️⃣ Пополнение ≥ 20$
3️⃣ Проверка ID

❌ Не по ссылке — нет доступа
❌ Баланс <20$ — нет доступа
"""

def instruction_kb(page):
    kb = InlineKeyboardBuilder()
    if page > 1:
        kb.button(text="⬅️ Назад", callback_data=f"instr:{page-1}")
    if page < 3:
        kb.button(text="➡️ Далее", callback_data=f"instr:{page+1}")
    if page == 3:
        kb.button(text="🔓 Получить доступ", callback_data="get_access")
    kb.adjust(2)
    return kb.as_markup()

# ================= INDICATORS =================

def calculate_indicators(df: pd.DataFrame):
    votes = []
    close, high, low, volume = df["Close"], df["High"], df["Low"], df["Volume"]

    votes += ["BUY" if close.iloc[-1] > close.rolling(10).mean().iloc[-1] else "SELL"]
    votes += ["BUY" if close.iloc[-1] > close.rolling(20).mean().iloc[-1] else "SELL"]

    votes += ["BUY" if close.iloc[-1] > close.ewm(span=10).mean().iloc[-1] else "SELL"]
    votes += ["BUY" if close.iloc[-1] > close.ewm(span=20).mean().iloc[-1] else "SELL"]

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    votes += ["BUY" if rsi.iloc[-1] > 50 else "SELL"]

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    votes += ["BUY" if ema12.iloc[-1] > ema26.iloc[-1] else "SELL"]

    stoch = 100 * (close - low.rolling(14).min()) / (high.rolling(14).max() - low.rolling(14).min())
    votes += ["BUY" if stoch.iloc[-1] > 50 else "SELL"]

    votes += ["BUY" if close.diff(4).iloc[-1] > 0 else "SELL"]

    tp = (high + low + close) / 3
    cci = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())
    votes += ["BUY" if cci.iloc[-1] > 0 else "SELL"]

    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    votes += ["BUY" if obv.iloc[-1] > obv.iloc[-2] else "SELL"]

    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)

    tr = pd.concat([(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(14).sum() / atr
    minus_di = 100 * pd.Series(minus_dm).rolling(14).sum() / atr

    votes += ["BUY" if plus_di.iloc[-1] > minus_di.iloc[-1] else "SELL"]

    return votes

# ================= SIGNAL =================

async def get_signal(pair: str):
    df = yf.download(pair, period="2d", interval="15m", progress=False)
    if df.empty or len(df) < 30:
        return None, None

    votes = calculate_indicators(df)
    buy, sell = votes.count("BUY"), votes.count("SELL")

    if abs(buy - sell) < 3:
        return None, None

    direction = "Вверх 📈" if buy > sell else "Вниз 📉"
    confidence = round(max(buy, sell) / len(votes) * 100, 1)
    return direction, confidence

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)

    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Автор — полный доступ", reply_markup=main_menu())
        return

    await msg.answer(INSTR_1, reply_markup=instruction_kb(1))

@dp.callback_query(lambda c: c.data.startswith("instr:"))
async def instr(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    texts = {1: INSTR_1, 2: INSTR_2, 3: INSTR_3}
    await cb.message.edit_text(texts[page], reply_markup=instruction_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data == "get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="🆔 Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("Выполните шаги ниже ⬇️", reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Нет депозита ≥20$", show_alert=True)

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, confidence = await get_signal(pair)
    if not direction:
        await cb.message.edit_text("⚠️ Нет сильного сигнала")
        return
    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n{pair}\nЭкспирация: {exp} мин\n{direction}\nУверенность: {confidence}%",
        reply_markup=result_kb()
    )

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, confidence = await get_signal(pair)
    if not direction:
        await cb.message.edit_text("⚠️ Нет сигнала")
        return
    await cb.message.edit_text(
        f"📰 НОВОСТЬ / СИГНАЛ\n\n{pair}\n{direction}\nЭкспирация: {exp} мин\nУверенность: {confidence}%",
        reply_markup=result_kb()
    )

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())

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
    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
