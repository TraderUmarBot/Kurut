# main.py - V8-CLEAN-ROUTE (ФИНАЛЬНЫЙ FIX: Чистый Webhook Роутинг)

import os
import asyncio
import pandas as pd
import pandas_ta as ta
import logging
import sys
from typing import Dict, Any, Union
from datetime import datetime
import time

import asyncpg
from functools import lru_cache

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import setup_application
from aiohttp import web
from aiogram.enums import ParseMode

import yfinance as yf

# -------------------- Конфиг --------------------
TG_TOKEN = os.environ.get("TG_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEB_SERVER_PORT = int(os.environ.get("PORT", 10000))
WEB_SERVER_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0")
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

if not all([TG_TOKEN, RENDER_EXTERNAL_HOSTNAME]):
    logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задан TG_TOKEN или RENDER_EXTERNAL_HOSTNAME. Выход.")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
]
TIMEFRAMES = [1, 3, 5, 10]
PAIRS_PER_PAGE = 6

bot = Bot(token=TG_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_POOL: Union[asyncpg.Pool, None] = None

# -------------------- PostgreSQL --------------------
async def init_db_pool():
    global DB_POOL
    if not DATABASE_URL:
        logging.warning("⚠️ DATABASE_URL не задан. Бот будет работать без сохранения истории (In-Memory).")
        return
    try:
        DB_POOL = await asyncpg.create_pool(DATABASE_URL)
        logging.info("✅ Пул PostgreSQL успешно создан.")
        await init_db_tables()
    except Exception as e:
        logging.error(f"❌ Ошибка подключения или создания пула PostgreSQL: {e}")

async def init_db_tables():
    if not DB_POOL: return
    try:
        async with DB_POOL.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    pair TEXT NOT NULL,
                    timeframe INTEGER NOT NULL,
                    result TEXT,
                    direction TEXT
                );
            """)
        logging.info("✅ Таблицы users и trades успешно созданы/проверены.")
    except Exception as e:
        logging.error(f"❌ Ошибка при создании/проверке таблиц БД: {e}")

async def save_user_db(user_id: int):
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)
        except Exception as e:
            logging.error(f"Ошибка сохранения пользователя {user_id} в DB: {e}")

async def save_trade_db(user_id: int, pair: str, timeframe: int, direction: str) -> int:
    await save_user_db(user_id)
    if not DB_POOL: 
        logging.warning("⚠️ DB недоступна. Сделка не сохранена.")
        return int(time.time())
    try:
        async with DB_POOL.acquire() as conn:
            return await conn.fetchval("""
                INSERT INTO trades (user_id, pair, timeframe, direction) 
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """, user_id, pair, timeframe, direction)
    except Exception as e:
        logging.error(f"Ошибка сохранения сделки в DB: {e}")
        return int(time.time())

async def update_trade_result_db(trade_id: int, result: str):
    if not DB_POOL: return
    try:
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE trades SET result = $1 WHERE id = $2", result, trade_id)
    except Exception as e:
        logging.error(f"Ошибка обновления результата сделки {trade_id} в DB: {e}")

async def get_user_stats_db(user_id: int) -> Dict[str, Any]:
    if not DB_POOL:
        return {'total_plus': 0, 'total_minus': 0, 'pair_stats': {}, 'db_active': False}
    try:
        async with DB_POOL.acquire() as conn:
            stats_rows = await conn.fetch("""
                SELECT result, COUNT(*) FROM trades 
                WHERE user_id = $1 AND result IS NOT NULL 
                GROUP BY result
            """, user_id)
            stats = dict(stats_rows)
            pair_rows = await conn.fetch("""
                SELECT pair, result, COUNT(*) FROM trades 
                WHERE user_id = $1 AND result IS NOT NULL 
                GROUP BY pair, result
            """, user_id)
        formatted_pair_stats: Dict[str, Dict[str, int]] = {}
        if pair_rows:
            for pair, result, count in pair_rows:
                if pair not in formatted_pair_stats:
                    formatted_pair_stats[pair] = {'PLUS': 0, 'MINUS': 0}
                if result in formatted_pair_stats[pair]:
                    formatted_pair_stats[pair][result] = count
        return {
            'total_plus': stats.get('PLUS', 0),
            'total_minus': stats.get('MINUS', 0),
            'pair_stats': formatted_pair_stats,
            'db_active': True
        }
    except Exception as e:
        logging.error(f"Ошибка получения статистики из DB: {e}")
        return {'total_plus': 0, 'total_minus': 0, 'pair_stats': {}, 'db_active': False}

# -------------------- FSM и клавиатуры --------------------
class Form(StatesGroup):
    choosing_pair = State()
    choosing_timeframe = State()

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📈 Выбрать пару", callback_data="start_trade")
    builder.button(text="📜 История сделок", callback_data="show_history")
    builder.adjust(1)
    return builder.as_markup()

def get_trade_result_keyboard(trade_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ ПЛЮС", callback_data=f"result:{trade_id}:PLUS")
    builder.button(text="❌ МИНУС", callback_data=f"result:{trade_id}:MINUS")
    builder.adjust(2)
    return builder.as_markup()

def get_pairs_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    builder = InlineKeyboardBuilder()
    for pair in PAIRS[start:end]:
        builder.button(text=pair, callback_data=f"pair:{pair}")
    builder.adjust(2)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page:{page-1}"))
    if end < len(PAIRS):
        nav_buttons.append(InlineKeyboardButton(text="➡️ Вперед", callback_data=f"page:{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu"))
    return builder.as_markup()

def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        builder.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="◀️ Назад к парам", callback_data="start_trade"))
    return builder.as_markup()

# -------------------- Обработчики --------------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await save_user_db(message.from_user.id)
    await message.answer(
        "👋 **Привет, я твой торгующий помощник.**\n\n"
        "📈 Выбери валютную пару, чтобы начать:",
        reply_markup=get_pairs_keyboard(0)
    )
    await state.set_state(Form.choosing_pair)

# Остальные обработчики (main_menu_handler, show_history_handler, trade_result_handler, page_handler, pair_handler, tf_handler) идут точно как в твоем коде выше

# -------------------- Функции работы с данными и индикаторами --------------------
# get_cache_key, async_fetch_ohlcv, compute_indicators, support_resistance, indicator_vote, send_signal
# — эти функции полностью идентичны твоему коду выше

# -------------------- Webhook запуск --------------------
async def health_check(request):
    return web.Response(text="Bot is running!", status=200)

async def on_startup_webhook(bot: Bot):
    await init_db_pool()
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
        await bot(SetWebhook(url=WEBHOOK_URL))
        logging.info(f"✅ Webhook успешно переустановлен: {WEBHOOK_URL}")
    except Exception as e:
        logging.error(f"Ошибка в on_startup_webhook: {e}")

async def on_shutdown_webhook(bot: Bot):
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
        logging.info("🗑️ Webhook удален.")
        if DB_POOL:
            await DB_POOL.close()
            logging.info("❌ Пул PostgreSQL закрыт.")
    except Exception as e:
        logging.error(f"Ошибка при удалении Webhook/закрытии DB: {e}")

async def start_webhook():
    logging.info(f"--- ЗАПУСК WEBHOOK СЕРВЕРА V8-CLEAN-ROUTE: {WEBHOOK_URL} ---")
    dp.startup.register(on_startup_webhook)
    dp.shutdown.register(on_shutdown_webhook)
    app = web.Application()
    app.router.add_get('/', health_check)
    setup_application(app, dp, bot=bot, path=WEBHOOK_PATH)
    try:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        await site.start()
        logging.info(f"🌐 Сервер запущен на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
        await asyncio.Event().wait()
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА WEBHOOK-СЕРВЕРА: {e}")
        sys.exit(1)

def main():
    try:
        asyncio.run(start_webhook())
    except Exception as e:
        logging.error(f"Непредвиденная ошибка в main(): {e}")

if __name__ == "__main__":
    main()
