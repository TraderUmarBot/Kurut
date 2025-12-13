# main.py — FINAL WORKING VERSION (Render + aiogram v3 + webhook + история + статистика)

import os
import sys
import asyncio
import logging
import time
from datetime import datetime
from typing import Union

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.methods import DeleteWebhook, SetWebhook

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# ===================== CONFIG =====================
TG_TOKEN = os.environ.get("TG_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))
HOST = "0.0.0.0"
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ TG_TOKEN или RENDER_EXTERNAL_HOSTNAME не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(
    token=TG_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
)
dp = Dispatcher(storage=MemoryStorage())

DB_POOL: Union[asyncpg.Pool, None] = None

# ===================== CONSTANTS =====================
PAIRS = [
    "EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF",
    "EURJPY","GBPJPY","AUDJPY","EURGBP","EURAUD","GBPAUD",
    "CADJPY","CHFJPY","EURCAD","GBPCAD","AUDCAD","AUDCHF","CADCHF"
]
TIMEFRAMES = [1, 3, 5, 10]
PAIRS_PER_PAGE = 6

# ===================== DB =====================
async def init_db():
    global DB_POOL
    if not DATABASE_URL:
        logging.warning("⚠️ DATABASE_URL не задан — без истории")
        return
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY
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
            result TEXT
        );
        """)
    logging.info("✅ PostgreSQL готов")

async def save_user(user_id: int):
    if DB_POOL is None:
        logging.info("⚠️ DB_POOL нет, пропускаем сохранение пользователя")
        return
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def save_trade(user_id: int, pair: str, tf: int, direction: str) -> int:
    await save_user(user_id)
    if DB_POOL is None:
        return int(time.time())
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO trades (user_id, pair, timeframe, direction)
            VALUES ($1,$2,$3,$4) RETURNING id
        """, user_id, pair, tf, direction)

async def update_trade(trade_id: int, result: str):
    if DB_POOL is None:
        logging.info("⚠️ DB_POOL нет, пропускаем update_trade")
        return
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE trades SET result=$1 WHERE id=$2",
            result, trade_id
        )

async def get_history(user_id: int):
    if DB_POOL is None:
        return []
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT pair, timeframe, direction, result, timestamp
            FROM trades
            WHERE user_id=$1
            ORDER BY timestamp DESC
            LIMIT 10
        """, user_id)
        return rows

async def get_stats(user_id: int):
    if DB_POOL is None:
        return {"wins":0,"losses":0,"winrate":0}
    async with DB_POOL.acquire() as conn:
        wins = await conn.fetchval("SELECT COUNT(*) FROM trades WHERE user_id=$1 AND result='PLUS'", user_id)
        losses = await conn.fetchval("SELECT COUNT(*) FROM trades WHERE user_id=$1 AND result='MINUS'", user_id)
        total = wins + losses
        winrate = round(wins/total*100, 2) if total>0 else 0
        return {"wins":wins,"losses":losses,"winrate":winrate}

# ===================== FSM =====================
class Form(StatesGroup):
    choosing_pair = State()
    choosing_tf = State()

# ===================== KEYBOARDS =====================
def pairs_kb(page=0):
    b = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    for p in PAIRS[start:end]:
        b.button(text=p, callback_data=f"pair:{p}")
    b.adjust(2)
    if page > 0:
        b.button(text="⬅️", callback_data=f"page:{page-1}")
    if end < len(PAIRS):
        b.button(text="➡️", callback_data=f"page:{page+1}")
    return b.as_markup()

def tf_kb(pair):
    b = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        b.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    b.adjust(2)
    return b.as_markup()

def result_kb(trade_id):
    b = InlineKeyboardBuilder()
    b.button(text="✅ ПЛЮС", callback_data=f"res:{trade_id}:PLUS")
    b.button(text="❌ МИНУС", callback_data=f"res:{trade_id}:MINUS")
    b.adjust(2)
    return b.as_markup()

def history_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📜 История", callback_data="history")
    return b.as_markup()

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start_cmd(msg: types.Message, state: FSMContext):
    await state.clear()
    await save_user(msg.from_user.id)
    stats = await get_stats(msg.from_user.id)
    text = f"📈 Выбери валютную пару:\n\nСтатистика:\nWins: {stats['wins']}, Losses: {stats['losses']}, Winrate: {stats['winrate']}%"
    await msg.answer(text, reply_markup=pairs_kb())
    await state.set_state(Form.choosing_pair)

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page_cb(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_reply_markup(reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_cb(cb: types.CallbackQuery, state: FSMContext):
    pair = cb.data.split(":")[1]
    await state.update_data(pair=pair)
    await state.set_state(Form.choosing_tf)
    await cb.message.edit_text(f"Пара **{pair}**, выбери ТФ:", reply_markup=tf_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf_cb(cb: types.CallbackQuery, state: FSMContext):
    _, pair, tf = cb.data.split(":")
    tf = int(tf)
    await cb.answer("⏳ Анализ...")
    trade_id = await save_trade(cb.from_user.id, pair, tf, "BUY")
    await cb.message.edit_text(
        f"📊 **Сигнал**\n\nПара: {pair}\nTF: {tf} мин\n\nНаправление: BUY",
        reply_markup=result_kb(trade_id)
    )

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res_cb(cb: types.CallbackQuery):
    _, trade_id, result = cb.data.split(":")
    await update_trade(int(trade_id), result)
    await cb.message.edit_text(f"✅ Результат сохранён: **{result}**")
    await cb.answer()

@dp.callback_query(lambda c: c.data=="history")
async def history_cb(cb: types.CallbackQuery):
    trades = await get_history(cb.from_user.id)
    if not trades:
        text = "История пуста"
    else:
        text = "📜 Последние сделки:\n"
        for t in trades:
            ts = t["timestamp"].strftime("%d-%m %H:%M")
            text += f"{ts} | {t['pair']} | {t['direction']} | {t['result'] or '-'}\n"
    await cb.message.answer(text)
    await cb.answer()

# ===================== WEBHOOK =====================
async def on_startup(bot: Bot):
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))
    logging.info(f"✅ Webhook установлен: {WEBHOOK_URL}")

async def on_shutdown(bot: Bot):
    await bot(DeleteWebhook())
    if DB_POOL:
        await DB_POOL.close()

async def health(request):
    return web.Response(text="OK")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    app = web.Application()
    app.router.add_get("/", health)

    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=WEBHOOK_PATH)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info("🚀 BOT LIVE")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
