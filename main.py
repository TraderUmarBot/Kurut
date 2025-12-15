import os
import sys
import asyncio
import logging
import random
from datetime import datetime

import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
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
EXPIRATIONS = [1, 2, 3, 5, 10]  # минуты

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
            balance FLOAT DEFAULT 0,
            registered BOOLEAN DEFAULT FALSE
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

async def set_registered(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET registered=TRUE WHERE user_id=$1",
            user_id
        )

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow(
            "SELECT user_id, pocket_id, balance, registered FROM users WHERE user_id=$1",
            user_id
        )

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
    kb.button(text="✅ Я пополнил свой баланс", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ================= INDICATORS =================
def calculate_indicators(data: pd.DataFrame):
    # Простая демонстрация 15 индикаторов
    close = data['Close']
    high = data['High']
    low = data['Low']

    # 1-5: SMA и EMA
    sma5 = close.rolling(5).mean()
    sma10 = close.rolling(10).mean()
    sma20 = close.rolling(20).mean()
    ema10 = close.ewm(span=10, adjust=False).mean()
    ema20 = close.ewm(span=20, adjust=False).mean()

    # 6-10: RSI, MACD
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    macd = ema10 - ema20
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    # 11-15: Bollinger Bands, ATR, Stochastic
    std20 = close.rolling(20).std()
    upper_bb = sma20 + 2 * std20
    lower_bb = sma20 - 2 * std20
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    k = ((close - low.rolling(14).min()) / (high.rolling(14).max() - low.rolling(14).min())) * 100
    d = k.rolling(3).mean()

    return {
        "sma5": sma5, "sma10": sma10, "sma20": sma20,
        "ema10": ema10, "ema20": ema20,
        "rsi": rsi, "macd": macd, "macd_signal": macd_signal,
        "upper_bb": upper_bb, "lower_bb": lower_bb,
        "atr": atr, "k": k, "d": d
    }

# ================= SIGNALS =================
async def get_signal(pair: str, expiration: int = 1):
    """
    Сигнал на основе 15 индикаторов, всегда BUY или SELL
    """
    try:
        data = yf.download(pair, period="90d", interval="1h", progress=False)
        if data.empty or len(data) < 20:
            return "ПОКУПКА", 50, "Недостаточно данных, сигнал по умолчанию ПОКУПКА"

        ind = calculate_indicators(data)

        # Простая стратегия:
        buy_score = 0
        sell_score = 0

        # SMA/EMA
        if ind['sma5'].iloc[-1] > ind['sma20'].iloc[-1]: buy_score += 1
        else: sell_score += 1
        if ind['ema10'].iloc[-1] > ind['ema20'].iloc[-1]: buy_score += 1
        else: sell_score += 1

        # RSI
        if ind['rsi'].iloc[-1] < 30: buy_score += 1
        elif ind['rsi'].iloc[-1] > 70: sell_score += 1

        # MACD
        if ind['macd'].iloc[-1] > ind['macd_signal'].iloc[-1]: buy_score += 1
        else: sell_score += 1

        # Bollinger
        if data['Close'].iloc[-1] < ind['lower_bb'].iloc[-1]: buy_score += 1
        elif data['Close'].iloc[-1] > ind['upper_bb'].iloc[-1]: sell_score += 1

        # ATR trend (рост волатильности = вверх)
        if ind['atr'].iloc[-1] > ind['atr'].iloc[-2]: buy_score += 1
        else: sell_score += 1

        # K/D Stochastic
        if ind['k'].iloc[-1] > ind['d'].iloc[-1]: buy_score += 1
        else: sell_score += 1

        direction = "ПОКУПКА" if buy_score >= sell_score else "ПРОДАЖА"
        confidence = (max(buy_score, sell_score) / 15) * 100
        explanation = f"Тренд: {'восходящий' if direction=='ПОКУПКА' else 'нисходящий'} | Счёт BUY:{buy_score} SELL:{sell_score}"

        return direction, confidence, explanation
    except Exception as e:
        return "ПОКУПКА", 50, f"Ошибка анализа: {e}"

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    user = await get_user(user_id)

    # Авторский доступ
    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    # Новый пользователь или уже зарегистрирован
    kb = InlineKeyboardBuilder()
    kb.button(text="📖 Инструкция по боту", callback_data="instruction")
    kb.adjust(1)
    await msg.answer("👋 Привет! Добро пожаловать!", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "instruction")
async def instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Получить доступ к боту", url=REF_LINK)
    kb.button(text="Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.answer(
        "📝 Инструкция:\nБот анализирует валютные пары с помощью 15 индикаторов.\nТаймфрейм: 1 час.\nСигналы: ПОКУПКА / ПРОДАЖА\nПояснение тренда выводится вместе с сигналом.",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("Отправь свой ID:")
    await cb.answer()
    await TradeState.choosing_pair.set()  # Переписываем, можно использовать отдельное состояние для ID

@dp.message()
async def receive_id(msg: types.Message, state: TradeState):
    try:
        user_id = int(msg.text)
    except ValueError:
        await msg.answer("ID должен быть числом.")
        return

    # Проверка регистрации и баланса
    user = await get_user(user_id)
    if not user:
        await add_user(user_id, str(user_id))
        await msg.answer("❌ Пользователь не найден через Postback.")
        return

    if user['balance'] < MIN_DEPOSIT:
        await msg.answer(f"❌ Пополните баланс минимум на ${MIN_DEPOSIT}")
        return

    # Всё ок, даём доступ
    await set_registered(user_id)
    await msg.answer("✅ Доступ к сигналам открыт!", reply_markup=main_menu())
    await state.clear()

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
    await cb.message.edit_text(f"⏱ Пара {pair.replace('=X','')}, выберите время экспирации", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, conf, expl = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📊 Сигнал\nПара: {pair.replace('=X','')}\nВремя экспирации: {exp} мин\nНаправление: {direction}\nУверенность: {conf:.2f}%\nПояснение: {expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, expl = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 Новости - Авто-сигнал\nПара: {pair.replace('=X','')}\nВремя экспирации: {exp} мин\nНаправление: {direction}\nУверенность: {conf:.2f}%\nПояснение: {expl}",
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
