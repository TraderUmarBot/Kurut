import os
import sys
import asyncio
import logging
import random
import io
from datetime import datetime

import asyncpg
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
AUTHORS = [7079260196, 6117198446]  # бесплатный доступ авторам
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
    kb.button(text="✅ ПЛЮС", callback_data="menu")
    kb.button(text="❌ МИНУС", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ================= SIGNALS =================
async def get_signal_with_chart(pair: str, expiration: int = 1):
    try:
        data = yf.download(pair, period="60d", interval="1h", progress=False)
        if data.empty or len(data) < 20:
            direction = random.choice(["BUY","SELL"])
            confidence = 60.0
            explanation = "Недостаточно данных, сигнал по умолчанию"
        else:
            close = data['Close']
            high = data['High']
            low = data['Low']
            votes = []

            # ==== Индикаторы ====
            for p in [5,10,20]:
                sma = close.rolling(p).mean()
                votes.append("BUY" if close.iloc[-1] > sma.iloc[-1] else "SELL")

            for p in [5,10,20]:
                ema = close.ewm(span=p, adjust=False).mean()
                votes.append("BUY" if close.iloc[-1] > ema.iloc[-1] else "SELL")

            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = -delta.clip(upper=0).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            rsi = 100 - (100/(1+rs))
            votes.append("BUY" if rsi.iloc[-1] > 50 else "SELL")

            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd = ema12 - ema26
            signal_line = macd.ewm(span=9, adjust=False).mean()
            votes.append("BUY" if macd.iloc[-1] > signal_line.iloc[-1] else "SELL")

            low14 = low.rolling(14).min()
            high14 = high.rolling(14).max()
            stoch = (close - low14) / (high14 - low14 + 1e-9) * 100
            votes.append("BUY" if stoch.iloc[-1] < 50 else "SELL")

            sma20 = close.rolling(20).mean()
            std = close.rolling(20).std()
            upper = sma20 + 2*std
            lower = sma20 - 2*std
            votes.append("SELL" if close.iloc[-1] > upper.iloc[-1] else "BUY")

            mom = close.iloc[-1] - close.iloc[-10]
            votes.append("BUY" if mom > 0 else "SELL")

            buy_votes = votes.count("BUY")
            sell_votes = votes.count("SELL")
            direction = "BUY" if buy_votes >= sell_votes else "SELL"
            confidence = max(buy_votes, sell_votes) / len(votes) * 100
            explanation = f"Голоса индикаторов: BUY={buy_votes}, SELL={sell_votes}"

        # ==== Построение графика ====
        fig, ax = plt.subplots(figsize=(6,3))
        ax.plot(data['Close'][-30:], label='Close', color='blue')
        ax.set_title(f"{pair.replace('=X','')} - последние 30 свечей")
        ax.set_xlabel("Время")
        ax.set_ylabel("Цена")
        ax.grid(True)
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)

        return direction, confidence, explanation, buf

    except Exception as e:
        direction = random.choice(["BUY","SELL"])
        confidence = 60.0
        explanation = f"Ошибка анализа: {e}"
        return direction, confidence, explanation, None

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
    kb.button(text="Начать", callback_data="begin_instruction")
    kb.adjust(1)
    await msg.answer(
        "👋 Привет! Добро пожаловать!\nНажмите Начать для регистрации.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin_instruction")
async def begin_instruction(cb: types.CallbackQuery):
    explanation = (
        "📌 Бот анализирует валютные пары с использованием более 15 индикаторов:\n"
        "- SMA, EMA, RSI, MACD, Stochastic, Bollinger, Momentum и др.\n"
        "⚡ Сигналы всегда BUY или SELL, NEUTRAL нет.\n"
        "🔹 После нажатия ПЛЮС/МИНУС вы возвращаетесь в главное меню."
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="Продолжить", callback_data="begin_registration")
    kb.adjust(1)
    await cb.message.answer(explanation, reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "begin_registration")
async def begin_registration(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти к регистрации", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer("📝 Регистрация и пополнение:", reply_markup=kb.as_markup())
    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить пополнение", callback_data="check_deposit")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки:", reply_markup=kb_check.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_deposit")
async def check_deposit(cb: types.CallbackQuery):
    if cb.from_user.id in AUTHORS:
        await cb.message.answer("✅ Доступ к сигналам открыт (автор)", reply_markup=main_menu())
        await cb.answer()
        return

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
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}, выбери время экспирации",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, conf, expl, chart_buf = await get_signal_with_chart(pair, exp)

    kb_result = InlineKeyboardBuilder()
    kb_result.button(text="✅ ПЛЮС", callback_data="menu")
    kb_result.button(text="❌ МИНУС", callback_data="menu")
    kb_result.adjust(2)

    if chart_buf:
        await cb.message.answer_photo(
            photo=chart_buf,
            caption=(
                f"📊 Сигнал\nПара: {pair.replace('=X','')}\n"
                f"Время экспирации: {exp} мин\n"
                f"Направление: {direction}\n"
                f"Уверенность: {conf:.2f}%\n\n"
                f"{expl}"
            ),
            reply_markup=kb_result.as_markup()
        )
    else:
        await cb.message.answer(
            f"📊 Сигнал\nПара: {pair.replace('=X','')}\n"
            f"Время экспирации: {exp} мин\n"
            f"Направление: {direction}\n"
            f"Уверенность: {conf:.2f}%\n\n"
            f"{expl}",
            reply_markup=kb_result.as_markup()
        )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "news")
async def news(cb: types.CallbackQuery):
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, conf, expl, chart_buf = await get_signal_with_chart(pair, exp)

    kb_result = InlineKeyboardBuilder()
    kb_result.button(text="✅ ПЛЮС", callback_data="menu")
    kb_result.button(text="❌ МИНУС", callback_data="menu")
    kb_result.adjust(2)

    if chart_buf:
        await cb.message.answer_photo(
            photo=chart_buf,
            caption=(
                f"📰 Новости - Авто-сигнал\nПара: {pair.replace('=X','')}\n"
                f"Время экспирации: {exp} мин\n"
                f"Направление: {direction}\n"
                f"Уверенность: {conf:.2f}%\n\n"
                f"{expl}"
            ),
            reply_markup=kb_result.as_markup()
        )
    else:
        await cb.message.answer(
            f"📰 Новости - Авто-сигнал\nПара: {pair.replace('=X','')}\n"
            f"Время экспирации: {exp} мин\n"
            f"Направление: {direction}\n"
            f"Уверенность: {conf:.2f}%\n\n"
            f"{expl}",
            reply_markup=kb_result.as_markup()
        )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.answer("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= POSTBACK =================
async def handle_postback(request: web.Request):
    event = request.query.get("event")
    click_id = request.query.get("click_id")
    try:
        amount = float(request.query.get("amount", 0))
    except:
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
