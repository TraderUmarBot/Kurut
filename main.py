import os
import sys
import asyncio
import logging
from datetime import datetime
import random

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
            registered BOOL DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            pair TEXT,
            direction TEXT,
            confidence FLOAT,
            timestamp TIMESTAMP
        );
        """)

async def add_user(user_id: int, pocket_id: str, balance: float = 0.0):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, pocket_id, balance) 
            VALUES ($1,$2,$3) 
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_id, pocket_id, balance
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
            "UPDATE users SET registered=TRUE WHERE user_id=$1", user_id
        )

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        return row

async def log_signal(user_id: int, pair: str, direction: str, confidence: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO signals (user_id, pair, direction, confidence, timestamp) VALUES ($1,$2,$3,$4,NOW())",
            user_id, pair, direction, confidence
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
    kb.button(text="✅ Я пополнил баланс", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ================= SIGNALS =================
async def get_signal(pair: str):
    """
    Сигнал с 5 индикаторами для тренда
    Возвращает направление, уверенность и пояснение
    """
    try:
        data = yf.download(pair, period="60d", interval="1h", progress=False)
        if data.empty:
            return "ПОКУПКА", 50.0, "Нет данных, сигнал по умолчанию ПОКУПКА"

        close = data['Close']
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        rsi = 100 - (100 / (1 + close.pct_change().rolling(14).mean()))
        delta = close.diff()

        trend_score = 0
        if ma5.iloc[-1] > ma10.iloc[-1]: trend_score +=1
        if ma10.iloc[-1] > ma20.iloc[-1]: trend_score +=1
        if rsi.iloc[-1] > 60: trend_score +=1
        if delta.iloc[-1] > 0: trend_score +=1

        if trend_score >=3:
            return "ПОКУПКА", trend_score*20, "Тренд восходящий"
        else:
            return "ПРОДАЖА", (5-trend_score)*20, "Тренд нисходящий"
    except Exception as e:
        return "ПОКУПКА", 50.0, f"Ошибка анализа: {e}"

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    user = await get_user(user_id)
    if user and (user.registered or user_id in AUTHORS):
        await msg.answer("🏠 Главное меню", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Инструкция по боту", callback_data="instruction")
    kb.adjust(1)
    await msg.answer("👋 Добро пожаловать! Ознакомьтесь с инструкцией:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "instruction")
async def instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Получить доступ к боту", url=REF_LINK)
    kb.button(text="Проверить ID", callback_data="check_id")
    kb.adjust(1)
    text = (
        "📝 Бот анализирует валютные пары через Yahoo Finance\n"
        "📊 Используются индикаторы: MA5, MA10, MA20, RSI, Delta\n"
        "⏱ Таймфрейм: 1 час\n"
        "⚡ Сигналы всегда ПОКУПКА или ПРОДАЖА\n"
        "💡 Пояснение: Тренд восходящий или нисходящий"
    )
    await cb.message.edit_text(text, reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await cb.message.answer("⏳ Проверка пополнения и регистрации через postback...")
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
    await add_user(user_id, pocket_id=str(click_id), balance=amount)
    if event in ["deposit","reg"] and amount >= MIN_DEPOSIT:
        await set_registered(user_id)
    return web.Response(text="OK")

# ================= CALLBACKS =================
@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выберите валютную пару:", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выберите валютную пару:", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(f"⏱ {pair.replace('=X','')}, выберите время экспирации:", reply_markup=expiration_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    await cb.answer("Сигнал формируется...", show_alert=False)
    try:
        _, pair, exp = cb.data.split(":")
        direction, conf, expl = await get_signal(pair)
        await log_signal(cb.from_user.id, pair, direction, conf)
        await cb.message.edit_text(
            f"📊 СИГНАЛ\n\n"
            f"Пара: {pair.replace('=X','')}\n"
            f"Время экспирации: {exp} мин\n"
            f"Направление: {direction}\n"
            f"Уверенность: {conf:.1f}%\n"
            f"Пояснение: {expl}",
            reply_markup=result_kb()
        )
    except Exception as e:
        await cb.message.edit_text(f"❌ Ошибка анализа: {e}", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "menu")
async def result_menu(cb: types.CallbackQuery):
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ================= WEBHOOK =================
async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

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
