import os
import sys
import asyncio
import logging
from datetime import datetime
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import asyncpg

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

from tradingview_ta import TA_Handler, Interval, Exchange

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

AUTHORS = [7079260196, 6117198446]  # ID авторов

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME or not DATABASE_URL:
    print("❌ ENV не заданы или DATABASE_URL неверен")
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
TIMEFRAMES = [1, 2, 5, 15]
PAIRS_PER_PAGE = 6
MIN_DEPOSIT = 20.0

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
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pair TEXT,
            timeframe INT,
            direction TEXT,
            confidence FLOAT,
            explanation TEXT,
            result TEXT
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
    if DB_POOL is None:
        raise RuntimeError("DB_POOL не инициализирован!")
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return val or 0.0

async def save_trade(user_id, pair, tf, direction, confidence, explanation):
    async with DB_POOL.acquire() as c:
        return await c.fetchval(
            """INSERT INTO trades (user_id, pair, timeframe, direction, confidence, explanation)
               VALUES ($1,$2,$3,$4,$5,$6) RETURNING id""",
            user_id, pair, tf, direction, confidence, explanation
        )

async def update_trade(trade_id, result):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE trades SET result=$1 WHERE id=$2",
            result, trade_id
        )

async def get_history(user_id):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM trades WHERE user_id=$1 ORDER BY timestamp DESC LIMIT 20",
            user_id
        )

# ===================== FSM =====================
class TradeState(StatesGroup):
    choosing_pair = State()
    choosing_tf = State()

# ===================== KEYBOARDS =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📜 История сделок", callback_data="history")
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

def tf_kb(pair):
    kb = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        kb.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    kb.adjust(2)
    return kb.as_markup()

def result_kb(trade_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПЛЮС", callback_data=f"res:{trade_id}:PLUS")
    kb.button(text="❌ МИНУС", callback_data=f"res:{trade_id}:MINUS")
    kb.button(text="🏠 Меню", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ===================== HANDLERS =====================
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

    # Инструкция + кнопка Начать
    kb = InlineKeyboardBuilder()
    kb.button(text="Начать", callback_data="begin")
    kb.adjust(1)
    await msg.answer(
        "📖 Добро пожаловать!\n\n"
        "Бот анализирует свечи с TradingView, использует индикаторы SMA, EMA, RSI, MACD, Bollinger Bands и ADX для генерации сигналов.\n"
        "Сигналы основаны на анализе текущего тренда и силы рынка.\n\n"
        "Нажмите кнопку 'Начать' чтобы продолжить.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="begin")
async def begin(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Зарегистрировать аккаунт", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer(
        f"🔗 Сначала зарегистрируйтесь по нашей ссылке.\n"
        f"💰 Затем пополните баланс минимум на ${MIN_DEPOSIT}.",
        reply_markup=kb.as_markup()
    )
    # Кнопка проверки пополнения
    kb2 = InlineKeyboardBuilder()
    kb2.button(text="Проверить пополнение", callback_data="check_deposit")
    kb2.adjust(1)
    await cb.message.answer("Когда пополните, нажмите кнопку ниже:", reply_markup=kb2.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="check_deposit")
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

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}, выбери TF",
        reply_markup=tf_kb(pair)
    )
    await cb.answer()

# ===================== TRADINGVIEW TF HANDLER =====================
@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb: types.CallbackQuery):
    _, pair, tf_raw = cb.data.split(":")
    tf_map = {
        "1": Interval.INTERVAL_1_MINUTE,
        "2": Interval.INTERVAL_2_MINUTES,
        "5": Interval.INTERVAL_5_MINUTES,
        "15": Interval.INTERVAL_15_MINUTES
    }
    selected_tf = tf_map.get(tf_raw)
    if not selected_tf:
        await cb.message.answer("❌ Неверный таймфрейм")
        await cb.answer()
        return

    handler = TA_Handler(
        symbol=pair.replace("=X",""),
        screener="forex",
        exchange="FX_IDC",
        interval=selected_tf
    )

    try:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            analysis = await loop.run_in_executor(pool, handler.get_analysis)
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка получения сигнала: {e}")
        await cb.answer()
        return

    direction = analysis.summary["RECOMMENDATION"]
    confidence = round(analysis.indicators.get("RSI", 50), 1)
    expl = " | ".join([f"{k}: {v}" for k,v in analysis.indicators.items()])

    try:
        tid = await save_trade(cb.from_user.id, pair.replace("=X",""), int(tf_raw), direction, confidence, expl)
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка при сохранении сделки: {e}")
        await cb.answer()
        return

    await cb.message.edit_text(
        f"📊 Сигнал\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"TF: {tf_raw} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {confidence}%\n\n"
        f"{expl}",
        reply_markup=result_kb(tid)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    _, tid, res_val = cb.data.split(":")
    await update_trade(int(tid), res_val)
    await cb.message.edit_text("✅ Результат сохранён", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="history")
async def history(cb: types.CallbackQuery):
    trades = await get_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("📜 История пуста")
        return
    text = "📜 История сделок\n\n"
    for t in trades:
        result = t['result'] if t['result'] else "—"
        text += f"{t['timestamp']} | {t['pair']} | {t['direction']} | {result}\n"
    await cb.message.answer(text)

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

if __name__=="__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(bot.session.close())
