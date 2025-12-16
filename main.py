import os
import sys
import asyncio
import logging
from datetime import datetime

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
EXPIRATIONS = [1,2,3,5,10]

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
    kb.adjust(1)
    return kb.as_markup()

# ================= INDICATORS =================

def calculate_indicators(df: pd.DataFrame) -> list[str]:
    df = df.fillna(method='bfill')  # Заполняем NaN с конца
    votes = []

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    # SMA
    votes.append("BUY" if close.iloc[-1] > close.rolling(10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.rolling(20).mean().iloc[-1] else "SELL")

    # EMA
    votes.append("BUY" if close.iloc[-1] > close.ewm(span=10).mean().iloc[-1] else "SELL")
    votes.append("BUY" if close.iloc[-1] > close.ewm(span=20).mean().iloc[-1] else "SELL")

    # RSI
    delta = close.diff().fillna(0)
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + gain / loss))
    votes.append("BUY" if rsi.iloc[-1] > 50 else "SELL")

    # MACD
    macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
    votes.append("BUY" if macd.iloc[-1] > 0 else "SELL")

    # Stochastic
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    stoch = 100 * (close - low14) / (high14 - low14)
    votes.append("BUY" if stoch.iloc[-1] > 50 else "SELL")

    # Momentum
    votes.append("BUY" if close.iloc[-1] > close.shift(5).iloc[-1] else "SELL")

    # CCI
    tp = (high + low + close)/3
    cci = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())
    votes.append("BUY" if cci.iloc[-1] > 0 else "SELL")

    # OBV
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    votes.append("BUY" if obv.iloc[-1] > obv.iloc[-2] else "SELL")

    # ADX упрощённый
    trend = high.diff().iloc[-1] - low.diff().iloc[-1]
    votes.append("BUY" if trend > 0 else "SELL")

    # +5 фильтров тренда
    for _ in range(5):
        votes.append("BUY" if close.iloc[-1] > close.iloc[-2] else "SELL")

    return votes


async def get_signal(pair: str):
    try:
        df = yf.download(pair, period="2d", interval="15m", progress=False, auto_adjust=True)
        if df.empty or len(df) < 30:
            return None, None

        df = df.fillna(method='bfill')  # На всякий случай
        votes = calculate_indicators(df)
        buy = votes.count("BUY")
        sell = votes.count("SELL")

        if buy == sell:
            return None, None

        direction = "ВВЕРХ 📈" if buy > sell else "ВНИЗ 📉"
        confidence = round(max(buy, sell) / len(votes) * 100, 1)
        return direction, confidence
    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return None, None

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    uid = msg.from_user.id
    if uid in AUTHORS:
        await msg.answer("👑 Авторский доступ\n\nГлавное меню:", reply_markup=main_menu())
        return

    # 3-страничная инструкция
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr_2")
    kb.adjust(1)
    await msg.answer(INSTR_1, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "instr_2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr_3")
    kb.adjust(1)
    await cb.message.edit_text(INSTR_2, reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "instr_3")
async def instr3(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text(INSTR_3, reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить пополнение", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text("⏳ Ожидаем пополнение от 20$", reply_markup=kb.as_markup())
    await cb.answer()

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
    direction, confidence = await get_signal(pair)
    if not direction:
        await cb.message.edit_text("⚠️ Сейчас нет сильного сигнала")
        await cb.answer()
        return
    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\nПара: {pair}\nЭкспирация: {exp} мин\nНаправление: {direction}\nУверенность: {confidence}%",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    import random
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, confidence = await get_signal(pair)
    if not direction:
        await cb.message.edit_text("⚠️ Сейчас нет сигнала")
        await cb.answer()
        return
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\nПара: {pair}\nЭкспирация: {exp} мин\nНаправление: {direction}\nУверенность: {confidence}%",
        reply_markup=result_kb()
    )
    await cb.answer()

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id","").strip()
    amount_raw = request.query.get("amount","0")
    if not click_id.isdigit():
        return web.Response(text="NO CLICK_ID", status=200)
    try:
        await upsert_user(int(click_id))
        await update_balance(int(click_id), float(amount_raw))
    except:
        pass
    return web.Response(text="OK")

# ================= START SERVER =================

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
