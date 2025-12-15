import os
import sys
import asyncio
import logging
from datetime import datetime

import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"
AUTHORS = [7079260196, 6117198446]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.pool.Pool | None = None

# ===================== CONSTANTS =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 2, 3, 5, 10]  # минуты

# ===================== DB =====================
async def init_db():
    global DB_POOL
    if DB_POOL is None:
        try:
            DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            logging.info("✅ Подключение к БД успешно")
        except Exception as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            sys.exit(1)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT,
            balance FLOAT DEFAULT 0
        );
        """)

async def add_user(user_id: int, pocket_id: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, pocket_id) VALUES ($1,$2) ON CONFLICT (user_id) DO NOTHING",
            user_id, pocket_id
        )

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id=$2",
            amount, user_id
        )

async def get_balance(user_id: int) -> float:
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return val or 0.0

# ===================== FSM =====================
class TradeState(StatesGroup):
    choosing_pair = State()
    choosing_exp = State()

# ===================== KEYBOARDS =====================
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
        kb.button(text="⬅️ Назад", callback_data=f"pairs_page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"pairs_page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for exp in EXPIRATIONS:
        kb.button(text=f"{exp} мин", callback_data=f"exp:{pair}:{exp}")
    kb.adjust(2)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПЛЮС", callback_data="res:return")
    kb.button(text="❌ МИНУС", callback_data="res:return")
    kb.button(text="🏠 Меню", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ===================== SIGNALS =====================
async def get_signal_advanced(pair: str, expiration: int):
    data = yf.download(pair, period="5d", interval="1m")
    close = data['Close']
    high = data['High']
    low = data['Low']

    if len(close) < 20:
        return "NEUTRAL", 50.0, "Недостаточно данных для анализа"

    # SMA/EMA
    sma5 = close.rolling(5).mean().iloc[-1]
    sma10 = close.rolling(10).mean().iloc[-1]
    sma20 = close.rolling(20).mean().iloc[-1]
    ema5 = close.ewm(span=5, adjust=False).mean().iloc[-1]
    ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]

    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean().iloc[-1]
    avg_loss = loss.rolling(14).mean().iloc[-1]
    rsi = 100 - (100 / (1 + avg_gain / (avg_loss+1e-6)))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    macd_diff = macd.iloc[-1] - macd_signal.iloc[-1]

    # Bollinger
    sma20_full = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper_bb = sma20_full.iloc[-1] + 2*std20.iloc[-1]
    lower_bb = sma20_full.iloc[-1] - 2*std20.iloc[-1]

    # Stochastic
    low14 = low.rolling(14).min().iloc[-1]
    high14 = high.rolling(14).max().iloc[-1]
    k = 100 * (close.iloc[-1] - low14) / (high14 - low14 + 1e-6)

    # ATR
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1)
    atr = tr.max(axis=1).rolling(14).mean().iloc[-1]

    # Momentum
    momentum = close.iloc[-1] - close.iloc[-10]

    # CCI
    typical_price = (high + low + close) / 3
    cci = (typical_price.iloc[-1] - typical_price.rolling(20).mean().iloc[-1]) / (0.015 * typical_price.rolling(20).std().iloc[-1] + 1e-6)

    # Логика
    score = 0
    if close.iloc[-1] > sma5: score += 1
    if close.iloc[-1] > sma10: score += 1
    if close.iloc[-1] > sma20: score += 1
    if close.iloc[-1] > ema5: score += 1
    if close.iloc[-1] > ema10: score += 1
    if close.iloc[-1] > ema20: score += 1
    if rsi < 30: score += 1
    elif rsi > 70: score -= 1
    if macd_diff > 0: score += 1
    else: score -= 1
    if close.iloc[-1] > upper_bb: score -= 1
    elif close.iloc[-1] < lower_bb: score += 1
    if k < 20: score +=1
    elif k > 80: score -=1
    if momentum > 0: score +=1
    else: score -=1
    if cci > 100: score -=1
    elif cci < -100: score +=1

    if score > 3: direction = "BUY"
    elif score < -3: direction = "SELL"
    else: direction = "NEUTRAL"

    confidence = min(max(abs(score)/12*100, 10), 100)
    explanation = f"SMA5:{sma5:.5f}, EMA5:{ema5:.5f}, RSI:{rsi:.2f}, MACD_diff:{macd_diff:.5f}, K:{k:.2f}, Momentum:{momentum:.5f}, CCI:{cci:.2f}"

    return direction, confidence, explanation

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="Начать", callback_data="begin_instruction")
    kb.adjust(1)
    await msg.answer(
        "Привет! Я бот для анализа валютных пар.\n\n"
        "Я использую рыночные данные для генерации сигналов.\n\n"
        "Нажмите кнопку Начать для инструкции.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin_instruction")
async def begin_instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти к регистрации", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer(f"1️⃣ Зарегистрируйтесь по ссылке\n2️⃣ Пополните баланс минимум на ${MIN_DEPOSIT}", reply_markup=kb.as_markup())
    await cb.answer()
    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить пополнение", callback_data="check_deposit")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки:", reply_markup=kb_check.as_markup())

@dp.callback_query(lambda c: c.data == "check_deposit")
async def check_deposit(cb: types.CallbackQuery):
    balance = await get_balance(cb.from_user.id)
    if balance >= MIN_DEPOSIT:
        await cb.message.answer("✅ Доступ к сигналам открыт!", reply_markup=main_menu())
    else:
        await cb.message.answer(f"❌ Пополните баланс минимум на ${MIN_DEPOSIT}")
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(f"⏱ Пара {pair.replace('=X','')}, выбери время экспирации", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    try:
        direction, conf, expl = await get_signal_advanced(pair.replace("=X",""), exp)
    except Exception as e:
        await cb.message.answer(f"Ошибка получения сигнала: {e}")
        await cb.answer()
        return
    await cb.message.edit_text(
        f"📊 Сигнал\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    # Автоматически выбираем случайную пару и время
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, expl = await get_signal_advanced(pair.replace("=X",""), exp)
    await cb.message.edit_text(
        f"📰 Новость - сигнал\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

# ===================== POSTBACK =====================
async def handle_postback(request: web.Request):
    event = request.query.get("event")
    click_id = request.query.get("click_id")
    amount = float(request.query.get("amount", 0))

    if not click_id:
        return web.Response(text="No click_id", status=400)

    try:
        user_id = int(click_id)
    except ValueError:
        user_id = click_id

    await add_user(user_id, pocket_id=str(click_id))
    if event in ["deposit","reg"] and amount > 0:
        await update_balance(user_id, amount)

    return web.Response(text="OK")

# ===================== WEBHOOK =====================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", handle_postback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info(f"🚀 BOT LIVE на {HOST}:{PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(bot.session.close())
