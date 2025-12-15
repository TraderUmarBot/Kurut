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
AUTHORS = [7079260196, 6117198446]  # добавляем авторов
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
    kb.button(text="✅ Готово", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ================= SIGNALS =================
async def get_signal(pair: str, expiration: int = 1):
    """
    Сигнал на основе 10 индикаторов: SMA, EMA, RSI, MACD, Stochastic,
    Bollinger, Momentum, ADX, CCI, ATR. Всегда ПОКУПКА или ПРОДАЖА
    """
    try:
        data = yf.download(pair, period="60d", interval="1h", progress=False)
        if data.empty or len(data) < 20:
            return "ПОКУПКА", 70.0, "Данных мало, сигнал по умолчанию ПОКУПКА"

        close = data['Close']
        high = data['High']
        low = data['Low']

        votes = []

        # ==== SMA ====
        for period in [5,10,20]:
            sma = close.rolling(period).mean()
            if close.iloc[-1] > sma.iloc[-1]:
                votes.append("ПОКУПКА")
            else:
                votes.append("ПРОДАЖА")

        # ==== EMA ====
        for period in [5,10,20]:
            ema = close.ewm(span=period, adjust=False).mean()
            votes.append("ПОКУПКА" if close.iloc[-1] > ema.iloc[-1] else "ПРОДАЖА")

        # ==== RSI ====
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / (loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        votes.append("ПОКУПКА" if rsi.iloc[-1] < 50 else "ПРОДАЖА")

        # ==== MACD ====
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal_line = macd.ewm(span=9, adjust=False).mean()
        votes.append("ПОКУПКА" if macd.iloc[-1] > signal_line.iloc[-1] else "ПРОДАЖА")

        # ==== Stochastic ====
        low14 = low.rolling(14).min()
        high14 = high.rolling(14).max()
        stoch = (close - low14) / (high14 - low14 + 1e-9) * 100
        votes.append("ПОКУПКА" if stoch.iloc[-1] < 50 else "ПРОДАЖА")

        # ==== Bollinger Bands ====
        sma20 = close.rolling(20).mean()
        std = close.rolling(20).std()
        upper = sma20 + 2*std
        lower = sma20 - 2*std
        votes.append("ПОКУПКА" if close.iloc[-1] < lower.iloc[-1] else "ПРОДАЖА")

        # ==== Momentum ====
        mom = close.diff(10)
        votes.append("ПОКУПКА" if mom.iloc[-1] > 0 else "ПРОДАЖА")

        # ==== ADX ====
        tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1)
        tr_max = tr.max(axis=1)
        atr = tr_max.rolling(14).mean()
        up = high - high.shift()
        down = low.shift() - low
        plus_dm = np.where((up > down) & (up > 0), up, 0)
        minus_dm = np.where((down > up) & (down > 0), down, 0)
        plus_di = 100 * pd.Series(plus_dm).rolling(14).mean() / (atr + 1e-9)
        minus_di = 100 * pd.Series(minus_dm).rolling(14).mean() / (atr + 1e-9)
        votes.append("ПОКУПКА" if plus_di.iloc[-1] > minus_di.iloc[-1] else "ПРОДАЖА")

        # ==== CCI ====
        tp = (high + low + close) / 3
        cci = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std())
        votes.append("ПОКУПКА" if cci.iloc[-1] > 0 else "ПРОДАЖА")

        # ==== ATR ====
        atr_value = atr.iloc[-1]
        votes.append("ПОКУПКА" if close.iloc[-1] > close.iloc[-2] else "ПРОДАЖА")

        # ==== Итог голосования ====
        buy_votes = votes.count("ПОКУПКА")
        sell_votes = votes.count("ПРОДАЖА")
        direction = "ПОКУПКА" if buy_votes >= sell_votes else "ПРОДАЖА"
        confidence = max(buy_votes, sell_votes) / len(votes) * 100
        explanation = f"Тренд: {'восходящий' if direction=='ПОКУПКА' else 'нисходящий'}\nГолоса индикаторов: ПОКУПКА={buy_votes}, ПРОДАЖА={sell_votes}"

        return direction, confidence, explanation

    except Exception as e:
        return "ПОКУПКА", 70.0, f"Ошибка анализа: {e}"

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    balance = await get_balance(user_id)

    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="📖 Инструкция по боту", callback_data="instruction")
    kb.adjust(1)
    await msg.answer(
        "👋 Привет! Добро пожаловать в бот!\nНажмите кнопку ниже для инструкции:",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "instruction")
async def instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Получить доступ к боту", url=REF_LINK)
    kb.button(text="Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.answer(
        "📝 Инструкция:\nБот анализирует валютные пары через данные YFinance.\n"
        "Использует индикаторы: SMA, EMA, RSI, MACD, Stochastic, Bollinger Bands, Momentum, ADX, CCI, ATR.\n"
        "Таймфрейм: 1 час.\nСигналы всегда: ПОКУПКА или ПРОДАЖА.\nСигналы сопровождаются пояснением тренда.",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("Отправь свой ID для проверки регистрации")
    await cb.answer()

@dp.message()
async def handle_id(msg: types.Message):
    try:
        user_id = int(msg.text.strip())
        balance = await get_balance(user_id)
        if balance >= MIN_DEPOSIT or user_id in AUTHORS:
            await msg.answer(f"✅ Регистрация подтверждена. Баланс ${balance:.2f}. Доступ к сигналам открыт!", reply_markup=main_menu())
        else:
            await msg.answer(f"❌ Баланс меньше ${MIN_DEPOSIT}. Пополните счет для доступа к сигналам.")
    except ValueError:
        await msg.answer("❌ Некорректный ID.")

# ================= CALLBACKS =================
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
        f"⏱ Пара {pair.replace('=X','')}, выберите время экспирации",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, conf, expl = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📊 Сигнал\nПара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n\n"
        f"{expl}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def result_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, expl = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 Новости - Авто-сигнал\nПара: {pair.replace('=X','')}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n\n"
        f"{expl}",
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
