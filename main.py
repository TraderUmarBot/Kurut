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
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
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

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"
AUTHORS = [7079260196, 6117198446]
MIN_DEPOSIT = 20.0

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= BOT ===================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.pool.Pool | None = None

# ================= CONSTANTS ==============
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
PAIRS_PER_PAGE = 6
EXPIRATIONS = [1, 2, 3, 5, 10]

# ================= DB =====================
async def init_db():
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
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

# ================= FSM =====================
class TradeState(StatesGroup):
    choosing_pair = State()
    choosing_exp = State()

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
    kb.button(text="✅ ПЛЮС", callback_data="menu")
    kb.button(text="❌ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================= SIGNALS =================
async def analyze_tf(pair: str, interval: str):
    """
    Анализ на одном таймфрейме с индикаторами
    """
    data = yf.download(pair, period="90d", interval=interval, progress=False)
    if data.empty or len(data) < 30:
        return "ПОКУПКА", "📈 Восходящий", "Данных недостаточно, сигнал по умолчанию ПОКУПКА"

    close = data['Close']

    # SMA и EMA
    sma20 = close.rolling(20).mean()
    ema10 = close.ewm(span=10, adjust=False).mean()

    # RSI
    delta = close.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / roll_down
    rsi = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal_line = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - signal_line

    last_close = close.iloc[-1]
    last_sma20 = sma20.iloc[-1]
    last_ema10 = ema10.iloc[-1]
    last_rsi = rsi.iloc[-1]
    last_macd_hist = macd_hist.iloc[-1]

    indicators = []
    if last_close > last_sma20: indicators.append("BUY") 
    else: indicators.append("SELL")
    if last_close > last_ema10: indicators.append("BUY")
    else: indicators.append("SELL")
    if last_rsi > 50: indicators.append("BUY")
    else: indicators.append("SELL")
    if last_macd_hist > 0: indicators.append("BUY")
    else: indicators.append("SELL")

    buy_count = indicators.count("BUY")
    sell_count = indicators.count("SELL")
    if buy_count > sell_count:
        direction = "ПОКУПКА"
        trend = "📈 Восходящий"
    else:
        direction = "ПРОДАЖА"
        trend = "📉 Нисходящий"

    explanation = (
        f"Цена: {last_close:.5f}, SMA20: {last_sma20:.5f}, EMA10: {last_ema10:.5f}\n"
        f"RSI: {last_rsi:.1f}, MACD: {last_macd_hist:.5f}"
    )

    confidence = 50 + abs(buy_count - sell_count) * 12.5
    return direction, trend, explanation, confidence

async def get_signal(pair: str, expiration: int = 1):
    """
    Сигнал с анализом двух таймфреймов: 1ч и 4ч
    """
    try:
        dir1, trend1, expl1, conf1 = await analyze_tf(pair, "1h")
        dir2, trend2, expl2, conf2 = await analyze_tf(pair, "4h")

        if dir1 == dir2:
            direction = dir1
            trend = trend2
        else:
            direction = dir1
            trend = f"{trend1}/{trend2}"

        explanation = f"1ч: {expl1}\n4ч: {expl2}"
        confidence = (conf1 + conf2) / 2
        return direction, confidence, explanation, trend

    except Exception as e:
        return "ПОКУПКА", 70.0, f"Ошибка анализа: {e}", "📈 Восходящий"

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    balance = await get_balance(user_id)

    if user_id in AUTHORS:
        await msg.answer(
            "🏠 Главное меню (Авторский доступ)",
            reply_markup=main_menu()
        )
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="📖 Получить инструкцию", callback_data="begin_instruction")
    kb.adjust(1)
    await msg.answer(
        "👋 Привет! Добро пожаловать!\nНажмите кнопку ниже, чтобы узнать, как работает бот.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin_instruction")
async def begin_instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Получить доступ к боту", callback_data="get_access")
    kb.adjust(1)
    await cb.message.answer(
        "📝 Инструкция по боту:\n\n"
        "Бот анализирует валютные пары через YFinance 📊.\n"
        "Использует SMA20, EMA10, RSI14 и MACD для анализа.\n"
        "Сигналы всегда ПОКУПКА или ПРОДАЖА.\n"
        "Таймфреймы: 1 час и 4 часа ⏱️.\n"
        "Выводит тренд и пояснение для каждого сигнала.",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти по ссылке", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer("Перейдите по ссылке для регистрации и пополнения баланса", reply_markup=kb.as_markup())

    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить ID", callback_data="check_id")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки ID:", reply_markup=kb_check.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("Отправь свой ID для проверки:")
    await cb.answer()

@dp.message()
async def id_response(msg: types.Message):
    try:
        user_id = int(msg.text)
    except ValueError:
        await msg.answer("❌ Неверный ID")
        return

    balance = await get_balance(user_id)
    if balance >= MIN_DEPOSIT or user_id in AUTHORS:
        await msg.answer("✅ Проверка успешна! Доступ к сигналам открыт.", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="Я пополнил свой баланс", callback_data="check_deposit")
        kb.adjust(1)
        await msg.answer(f"💰 Пополните баланс минимум на ${MIN_DEPOSIT}", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "check_deposit")
async def check_deposit(cb: types.CallbackQuery):
    balance = await get_balance(cb.from_user.id)
    if balance >= MIN_DEPOSIT or cb.from_user.id in AUTHORS:
        await cb.message.answer("✅ Доступ к сигналам открыт!", reply_markup=main_menu())
    else:
        await cb.message.answer(f"❌ Пополните баланс минимум на ${MIN_DEPOSIT}")
    await cb.answer()

# ================= CALLBACKS =================
@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выберите пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}, выберите время экспирации",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, expl, trend = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 Новости - Авто-сигнал\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n"
        f"Тренд: {trend}\n"
        f"Пояснение: {expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, conf, expl, trend = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📊 Сигнал\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n"
        f"Тренд: {trend}\n"
        f"Пояснение: {expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def result_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= POSTBACK =================
async def handle_postback(request: web.Request):
    event = request.query.get("event")
    click_id = request.query.get("click_id")
    try:
        amount = float(request.query.get("amount", 0))
    except ValueError:
        amount = 0
    if not click_id:
        return web.Response(text="No click_id", status=400)
    user_id = int(click_id)
    await add_user(user_id, pocket_id=str(click_id))
    if event in ["deposit","reg"] and amount > 0:
        await update_balance(user_id, amount)
    return web.Response(text="OK")

# ================= WEBHOOK =================
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
