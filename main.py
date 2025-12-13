# main.py — FINAL WORKING VERSION (Render + aiogram v3 + webhook)

import os
import sys
import asyncio
import logging
import time
from datetime import datetime
from typing import Union

import asyncpg

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
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

# ===================== MAIN MENU =====================

main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📜 История")]
    ],
    resize_keyboard=True
)

# ===================== DB =====================

async def init_db():
    global DB_POOL
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
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def save_trade(user_id: int, pair: str, tf: int, direction: str) -> int:
    await save_user(user_id)
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO trades (user_id, pair, timeframe, direction)
            VALUES ($1,$2,$3,$4) RETURNING id
        """, user_id, pair, tf, direction)

async def update_trade(trade_id: int, result: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE trades SET result=$1 WHERE id=$2",
            result, trade_id
        )

async def get_history(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch("""
            SELECT pair, timeframe, result, timestamp
            FROM trades
            WHERE user_id=$1 AND result IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 10
        """, user_id)

async def get_stats(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("""
            SELECT
                COUNT(*) total,
                COUNT(*) FILTER (WHERE result='PLUS') plus,
                COUNT(*) FILTER (WHERE result='MINUS') minus
            FROM trades
            WHERE user_id=$1 AND result IS NOT NULL
        """, user_id)

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

# ===================== HANDLERS =====================

@dp.message(Command("start"))
async def start_cmd(msg: types.Message, state: FSMContext):
    await state.clear()
    await save_user(msg.from_user.id)
    await msg.answer("📈 Выбери валютную пару:", reply_markup=main_menu)
    await msg.answer("⬇️ Пары:", reply_markup=pairs_kb())
    await state.set_state(Form.choosing_pair)

@dp.callback_query(F.data.startswith("page:"))
async def page_cb(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_reply_markup(reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(F.data.startswith("pair:"))
async def pair_cb(cb: types.CallbackQuery, state: FSMContext):
    pair = cb.data.split(":")[1]
    await state.update_data(pair=pair)
    await state.set_state(Form.choosing_tf)
    await cb.message.edit_text(f"Пара **{pair}**, выбери ТФ:", reply_markup=tf_kb(pair))
    await cb.answer()

@dp.callback_query(F.data.startswith("tf:"))
async def tf_cb(cb: types.CallbackQuery):
    _, pair, tf = cb.data.split(":")
    tf = int(tf)
    trade_id = await save_trade(cb.from_user.id, pair, tf, "BUY")
    await cb.message.edit_text(
        f"📊 **Сигнал**\n\nПара: {pair}\nTF: {tf} мин\n\nНаправление: BUY",
        reply_markup=result_kb(trade_id)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("res:"))
async def res_cb(cb: types.CallbackQuery):
    _, trade_id, result = cb.data.split(":")
    await update_trade(int(trade_id), result)
    await cb.message.edit_text(f"💾 Результат сохранён: **{result}**")
    await cb.answer()

# ---------- HISTORY ----------
@dp.message(F.text == "📜 История")
async def history(msg: types.Message):
    rows = await get_history(msg.from_user.id)
    if not rows:
        await msg.answer("📭 История пуста")
        return
    text = "📜 **Последние сделки**\n\n"
    for r in rows:
        emoji = "✅" if r["result"] == "PLUS" else "❌"
        text += f"{emoji} {r['pair']} {r['timeframe']}м — {r['timestamp'].strftime('%d.%m %H:%M')}\n"
    await msg.answer(text)

# ---------- STATS ----------
@dp.message(F.text == "📊 Статистика")
async def stats(msg: types.Message):
    s = await get_stats(msg.from_user.id)
    if not s or s["total"] == 0:
        await msg.answer("📊 Нет данных")
        return
    winrate = round((s["plus"] / s["total"]) * 100, 2)
    await msg.answer(
        "📊 **Статистика**\n\n"
        f"📈 Всего: {s['total']}\n"
        f"✅ PLUS: {s['plus']}\n"
        f"❌ MINUS: {s['minus']}\n"
        f"🎯 Winrate: **{winrate}%**"
    )

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
