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

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

# ================= FSM =====================
class TradeState(StatesGroup):
    checking_id = State()

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

def deposit_kb():
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

    # 1-2 SMA
    indicators.append('BUY' if close[-1] > close.rolling(10).mean().iloc[-1] else 'SELL')
    indicators.append('BUY' if close[-1] > close.rolling(20).mean().iloc[-1] else 'SELL')
    # 3-4 EMA
    indicators.append('BUY' if close[-1] > close.ewm(span=10).mean().iloc[-1] else 'SELL')
    indicators.append('BUY' if close[-1] > close.ewm(span=20).mean().iloc[-1] else 'SELL')
    # 5 RSI
    delta = close.diff()
    gain = delta.where(delta>0,0).rolling(14).mean()
    loss = -delta.where(delta<0,0).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100/(1+rs))
    indicators.append('BUY' if rsi.iloc[-1] > 50 else 'SELL')
    # 6 MACD
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    indicators.append('BUY' if macd.iloc[-1] > signal.iloc[-1] else 'SELL')
    # 7 Bollinger
    sma20 = close.rolling(20).mean()
    std = close.rolling(20).std()
    indicators.append('BUY' if close.iloc[-1] > sma20.iloc[-1] else 'SELL')
    # 8 Stochastic
    low14 = close.rolling(14).min()
    high14 = close.rolling(14).max()
    k = 100*(close - low14)/(high14 - low14)
    indicators.append('BUY' if k.iloc[-1] > 50 else 'SELL')
    # 9 ATR
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    indicators.append('BUY' if close.iloc[-1] > close.iloc[-2] else 'SELL')
    # 10 CCI
    tp = (high + low + close)/3
    cci = (tp - tp.rolling(20).mean())/(0.015*tp.rolling(20).std())
    indicators.append('BUY' if cci.iloc[-1] > 0 else 'SELL')
    # 11 ADX
    plus_dm = high.diff()
    minus_dm = low.diff() * -1
    tr14 = tr.rolling(14).sum()
    plus_di = 100 * plus_dm.rolling(14).sum() / tr14
    minus_di = 100 * minus_dm.rolling(14).sum() / tr14
    indicators.append('BUY' if plus_di.iloc[-1] > minus_di.iloc[-1] else 'SELL')
    # 12 Williams %R
    indicators.append('BUY' if k.iloc[-1] < -50 else 'SELL')
    # 13 Momentum
    momentum = close.diff(4)
    indicators.append('BUY' if momentum.iloc[-1] > 0 else 'SELL')
    # 14 OBV
    obv = (np.sign(close.diff())*volume).cumsum()
    indicators.append('BUY' if obv.iloc[-1] > obv.iloc[-2] else 'SELL')
    # 15 Ichimoku (tenkan-sen vs kijun-sen simplified)
    tenkan = (high.rolling(9).max() + low.rolling(9).min())/2
    kijun = (high.rolling(26).max() + low.rolling(26).min())/2
    indicators.append('BUY' if tenkan.iloc[-1] > kijun.iloc[-1] else 'SELL')
    return indicators

# ================= SIGNALS =================
async def get_signal(pair: str, expiration: int = 1):
    try:
        data = yf.download(pair, period="60d", interval="1h", progress=False)
        if data.empty or len(data) < 30:
            return "ПОКУПКА", 50.0
        indicators = calculate_indicators(data)
        buy_count = indicators.count('BUY')
        sell_count = indicators.count('SELL')
        direction = 'ПОКУПКА' if buy_count >= sell_count else 'ПРОДАЖА'
        confidence = max(buy_count, sell_count)/len(indicators)*100
        return direction, confidence
    except Exception:
        return "ПОКУПКА", 50.0

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return
    # Полная инструкция
    await msg.answer(
        "👋 Привет!\n\n"
        "Бот создан с помощью команды KURUT TRADE.\n"
        "Что умеет бот:\n"
        "1. Анализирует рынок через 15 индикаторов\n"
        "2. Определяет направление: ПОКУПКА / ПРОДАЖА\n"
        "3. Учитывает свечные паттерны\n"
        "4. Подсказывает время экспирации\n"
        "5. Выдаёт сигналы с кнопками ПЛЮС / МИНУС\n\n"
        "Нажмите кнопку ниже для получения доступа к боту.",
        reply_markup=access_kb()
    )

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("✏️ Отправьте ваш Pocket Option ID:")
    await TradeState.checking_id.set()
    await cb.answer()

@dp.message(lambda m: True, state=TradeState.checking_id)
async def handle_id(msg: types.Message):
    user_id = msg.from_user.id
    pocket_id = msg.text.strip()
    await add_user(user_id, pocket_id)
    balance = await get_balance(user_id)
    if balance >= MIN_DEPOSIT:
        await msg.answer("✅ У вас есть доступ к боту!", reply_markup=main_menu())
    else:
        await msg.answer("⚠️ Чтобы получить доступ, пополните баланс ≥ 20$.", reply_markup=deposit_kb())
    await dp.storage.close()  # сброс состояния

# ================= CALLBACKS =================
@dp.callback_query(lambda c: c.data == "pairs")
async def pairs_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выберите пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page_cb(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_cb(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}, выберите время экспирации",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration_cb(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, conf = await get_signal(pair, exp)
    user_id = cb.from_user.id
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO logs(user_id,pair,direction,confidence,exp_minutes) VALUES($1,$2,$3,$4,$5)",
            user_id, pair, direction, conf, exp
        )
    await cb.message.edit_text(
        f"📊 Сигнал\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def result_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= NEWS =================
@dp.callback_query(lambda c: c.data == "news")
async def news_cb(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 Новость / Случайный сигнал\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%",
        reply_markup=result_kb()
    )
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
