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
async def get_signal(pair: str, expiration: int = 1):
    """
    Сигнал на основе индикаторов, всегда BUY или SELL, NEUTRAL убран
    """
    try:
        data = yf.download(pair, period="60d", interval="1h", progress=False)
        if data.empty:
            return "BUY", 70.0, "Данных недостаточно, сигнал по умолчанию BUY"

        close = data['Close']

        # Простая логика: сравнение последних закрытий
        if len(close) < 10:
            return "BUY", 70.0, "Недостаточно данных, сигнал по умолчанию BUY"

        # Считаем разницу последних закрытий
        diff = close.iloc[-1] - close.iloc[-2]

        if diff >= 0:
            return "BUY", 70.0, f"Последнее закрытие выше предыдущего на {diff:.5f}"
        else:
            return "SELL", 70.0, f"Последнее закрытие ниже предыдущего на {abs(diff):.5f}"

    except Exception as e:
        return "BUY", 70.0, f"Ошибка анализа: {e}"

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
    kb = InlineKeyboardBuilder()
    kb.button(text="Продолжить", callback_data="continue_instruction")
    kb.adjust(1)
    await cb.message.answer(
        "📝 Инструкция:\nБот анализирует валютные пары через данные YFinance.\nСигналы всегда BUY или SELL на основе последних изменений цен.",
        reply_markup=kb.as_markup()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "continue_instruction")
async def continue_instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти к регистрации", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer(
        "Регистрация и пополнение баланса",
        reply_markup=kb.as_markup()
    )
    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить пополнение", callback_data="check_deposit")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки:", reply_markup=kb_check.as_markup())
    await cb.answer()

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
