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
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
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
    kb.button(text="Перейти к боту", url=REF_LINK)
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
async def get_signal(pair: str, expiration: int = 1):
    """
    Более точный сигнал с фильтрами и взвешенными индикаторами
    """
    try:
        interval = f"{expiration}m"
        data = yf.download(pair, period="8h", interval=interval, progress=False)
        if data.empty or len(data) < 20:
            return "НЕОПРЕДЕЛЕНО", 50.0

        indicators = calculate_indicators(data)
        weights = [2,2,2,2,1.5,2,1,1,1,1,1,1,1,1,2]
        weighted_buy = sum(w for i, w in enumerate(weights) if indicators[i] == "BUY")
        weighted_sell = sum(w for i, w in enumerate(weights) if indicators[i] == "SELL")
        total_weight = sum(weights)

        confidence_buy = weighted_buy / total_weight * 100
        confidence_sell = weighted_sell / total_weight * 100

        if confidence_buy > confidence_sell:
            direction = "ПОКУПКА"
            confidence = confidence_buy
        else:
            direction = "ПРОДАЖА"
            confidence = confidence_sell

        # Фильтры тренда SMA
        if len(data) >= 200:
            sma50 = data['Close'].rolling(50).mean().iloc[-1]
            sma200 = data['Close'].rolling(200).mean().iloc[-1]
            if direction == "ПОКУПКА" and sma50 < sma200:
                direction = "НЕОПРЕДЕЛЕНО"
            if direction == "ПРОДАЖА" and sma50 > sma200:
                direction = "НЕОПРЕДЕЛЕНО"

        # Фильтр ADX
        high = data['High']; low = data['Low']; close = data['Close']
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        plus_dm = high.diff()
        minus_dm = low.diff() * -1
        tr14 = tr.rolling(14).sum()
        plus_di = 100 * plus_dm.rolling(14).sum() / tr14
        minus_di = 100 * minus_dm.rolling(14).sum() / tr14
        adx = (abs(plus_di - minus_di)/(plus_di + minus_di))*100
        if adx.iloc[-1] < 20:
            direction = "НЕОПРЕДЕЛЕНО"

        # Проверка последних 3 свечей
        last_closes = close[-3:]
        if direction == "ПОКУПКА" and not all(last_closes.diff().dropna() > 0):
            direction = "НЕОПРЕДЕЛЕНО"
        if direction == "ПРОДАЖА" and not all(last_closes.diff().dropna() < 0):
            direction = "НЕОПРЕДЕЛЕНО"

        # Минимальная уверенность
        if confidence < 70:
            direction = "НЕОПРЕДЕЛЕНО"

        return direction, confidence

    except Exception as e:
        print("Ошибка get_signal:", e)
        return "НЕОПРЕДЕЛЕНО", 50.0

# ================= START & REGISTRATION =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
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

# ================= FSM HANDLERS =================
@dp.callback_query(lambda c: c.data == "check_id")
async def check_id_cb(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("✏️ Отправьте свой Pocket Option ID:")
    await state.set_state(TradeState.waiting_id)
    await cb.answer()

@dp.message()
async def receive_id(msg: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != TradeState.waiting_id:
        return

    user_id = msg.from_user.id
    pocket_id = msg.text.strip()

    await add_user(user_id, pocket_id)

    await msg.answer(
        "✅ ID зарегистрирован. Теперь пополните баланс ≥20$ и нажмите 'Я пополнил баланс'.",
        reply_markup=after_access_kb()
    )

    await state.clear()

@dp.callback_query(lambda c: c.data == "check_balance")
async def check_balance(cb: types.CallbackQuery):
    user_id = cb.from_user.id

    if user_id in AUTHORS:
        await cb.message.answer("🎉 Доступ к боту подтвержден! (Авторский доступ)", reply_markup=main_menu())
        await cb.answer()
        return

    balance = await get_balance(user_id)
    if balance >= MIN_DEPOSIT:
        await cb.message.answer("🎉 Доступ к боту подтвержден!", reply_markup=main_menu())
    else:
        await cb.message.answer(
            f"❌ Доступ не подтвержден. Минимальный депозит {MIN_DEPOSIT}$.\n"
            "Пополните баланс и нажмите снова 'Я пополнил баланс'.",
            reply_markup=after_access_kb()
        )
    await cb.answer()

# ================= POSTBACK =================
async def handle_postback(request: web.Request):
    click_id = request.query.get("click_id")
    event = request.query.get("event")
    try:
        amount = float(request.query.get("amount", 0))
    except:
        amount = 0

    if not click_id:
        return web.Response(text="No click_id", status=400)

    user_id = int(click_id)
    await add_user(user_id, pocket_id=str(click_id))

    if event in ["deposit", "reg"] and amount > 0:
        await update_balance(user_id, amount)
        balance = await get_balance(user_id)
        if balance >= MIN_DEPOSIT:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 Ваш баланс {balance}$.\nДоступ к боту подтвержден!",
                    reply_markup=main_menu()
                )
            except Exception as e:
                print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

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
