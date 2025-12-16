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
AUTHORS = [6117198446, 7079260196]
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
EXPIRATIONS = [1,2,3,5,10]  # минуты

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
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            confidence FLOAT,
            exp_minutes INT,
            timestamp TIMESTAMP DEFAULT now()
        );
        """)

async def add_user(user_id: int, pocket_id: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, pocket_id) VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET pocket_id = $2",
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

async def check_user_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    balance = await get_balance(user_id)
    return balance >= MIN_DEPOSIT

# ================= FSM =====================
class TradeState(StatesGroup):
    waiting_id = State()
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

def access_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Получить доступ", url=REF_LINK)
    kb.button(text="Проверить ID", callback_data="check_id")
    kb.adjust(1)
    return kb.as_markup()

def after_access_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Я пополнил баланс", callback_data="check_balance")
    kb.adjust(1)
    return kb.as_markup()

# ================= INDICATORS =================
def calculate_indicators(data: pd.DataFrame):
    indicators = []

    close = data['Close']
    high = data['High']
    low = data['Low']
    volume = data['Volume']

    indicators.append('BUY' if close.iloc[-1] > close.rolling(10).mean().iloc[-1] else 'SELL')
    indicators.append('BUY' if close.iloc[-1] > close.rolling(20).mean().iloc[-1] else 'SELL')
    indicators.append('BUY' if close.iloc[-1] > close.ewm(span=10).mean().iloc[-1] else 'SELL')
    indicators.append('BUY' if close.iloc[-1] > close.ewm(span=20).mean().iloc[-1] else 'SELL')

    delta = close.diff()
    gain = delta.where(delta>0,0).rolling(14).mean()
    loss = -delta.where(delta<0,0).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100/(1+rs))
    indicators.append('BUY' if rsi.iloc[-1] > 50 else 'SELL')

    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    indicators.append('BUY' if macd.iloc[-1] > signal.iloc[-1] else 'SELL')

    sma20 = close.rolling(20).mean()
    indicators.append('BUY' if close.iloc[-1] > sma20.iloc[-1] else 'SELL')

    low14 = close.rolling(14).min()
    high14 = close.rolling(14).max()
    k = 100*(close - low14)/(high14 - low14)
    indicators.append('BUY' if k.iloc[-1] > 50 else 'SELL')

    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    indicators.append('BUY' if close.iloc[-1] > close.iloc[-2] else 'SELL')

    tp = (high + low + close)/3
    cci = (tp - tp.rolling(20).mean())/(0.015*tp.rolling(20).std())
    indicators.append('BUY' if cci.iloc[-1] > 0 else 'SELL')

    plus_dm = high.diff()
    minus_dm = low.diff() * -1
    tr14 = tr.rolling(14).sum()
    plus_di = 100 * plus_dm.rolling(14).sum() / tr14
    minus_di = 100 * minus_dm.rolling(14).sum() / tr14
    adx = (abs(plus_di - minus_di)/(plus_di + minus_di))*100
    indicators.append('BUY' if plus_di.iloc[-1] > minus_di.iloc[-1] else 'SELL')

    indicators.append('BUY' if k.iloc[-1] < -50 else 'SELL')

    momentum = close.diff(4)
    indicators.append('BUY' if momentum.iloc[-1] > 0 else 'SELL')

    obv = (np.sign(close.diff())*volume).cumsum()
    indicators.append('BUY' if obv.iloc[-1] > obv.iloc[-2] else 'SELL')

    tenkan = (high.rolling(9).max() + low.rolling(9).min())/2
    kijun = (high.rolling(26).max() + low.rolling(26).min())/2
    indicators.append('BUY' if tenkan.iloc[-1] > kijun.iloc[-1] else 'SELL')

    return indicators

# ================= SIGNALS =================
async def get_signal(pair: str, interval="8h"):
    try:
        data = yf.download(pair, period="15d", interval=interval, progress=False)
        if data.empty or len(data) < 15:
            return "ПОКУПКА", 50.0
        indicators = calculate_indicators(data)
        buy_count = indicators.count('BUY')
        sell_count = indicators.count('SELL')
        if buy_count > sell_count:
            direction = 'ПОКУПКА'
            confidence = buy_count / len(indicators) * 100
        else:
            direction = 'ПРОДАЖА'
            confidence = sell_count / len(indicators) * 100
        return direction, confidence
    except Exception as e:
        print("Ошибка get_signal:", e)
        return "НЕОПРЕДЕЛЕНО", 50.0

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    async with DB_POOL.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    text = (
        "👋 Привет! Добро пожаловать!\n\n"
        "📌 Бот создан командой KURUT TRADE.\n"
        "💡 Что умеет бот:\n"
        "- Анализ рынка по 15 индикаторам\n"
        "- Сигналы: ПОКУПКА / ПРОДАЖА\n"
        "- Уверенность сигнала в процентах\n"
        "- Время экспирации выбирает пользователь\n\n"
        "📊 Как использовать:\n"
        "1. Зарегистрируйтесь по нашей реферальной ссылке\n"
        "2. Если есть аккаунт, удалите старый\n"
        "3. Нажмите 'Проверить ID'\n"
        "4. Пополните баланс на ≥20$\n"
        "5. После пополнения получите доступ к сигналам"
    )
    await msg.answer(text, reply_markup=access_kb())

# ================= HANDLERS ДЛЯ ВАЛЮТНЫХ ПАР И НОВОСТЕЙ =================

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выберите валютную пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите валютную пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Вы выбрали пару {pair.replace('=X','')}. Выберите время экспирации:",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, confidence = await get_signal(pair, interval="8h")
    user_id = cb.from_user.id
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO logs(user_id,pair,direction,confidence,exp_minutes) VALUES($1,$2,$3,$4,$5)",
            user_id, pair, direction, confidence, exp
        )
    await cb.message.edit_text(
        f"📊 Сигнал\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence:.2f}%",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def result_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = PAIRS[0]  # Берём первую пару, можно менять на логику
    exp = EXPIRATIONS[0]
    direction, confidence = await get_signal(pair, interval="8h")
    await cb.message.edit_text(
        f"📰 Новости / Сигнал выбранной пары\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence:.2f}%",
        reply_markup=result_kb()
    )
    await cb.answer()
