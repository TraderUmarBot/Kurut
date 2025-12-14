import os
import sys
import asyncio
import logging
from datetime import datetime
from collections import Counter

import pandas as pd
import pandas_ta as ta
import yfinance as yf
import asyncpg

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.environ.get("PORT", 10000))  # Render сам задаёт порт
HOST = "0.0.0.0"

REF_LINK = "https://u3.shortink.io/login?social=Google&utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"  # Telegram шлёт апдейты сюда
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(
    token=TG_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL = None

# ===================== CONSTANTS =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
TIMEFRAMES = [1, 2, 5, 15]
PAIRS_PER_PAGE = 6

# ===================== DB =====================
async def init_db():
    global DB_POOL
    if not DATABASE_URL:
        return
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT
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

async def has_access(user_id: int) -> bool:
    if not DB_POOL:
        return False
    async with DB_POOL.acquire() as conn:
        return bool(await conn.fetchval(
            "SELECT pocket_id FROM users WHERE user_id=$1 AND pocket_id IS NOT NULL",
            user_id
        ))

async def save_pocket_id(user_id: int, pocket_id: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET pocket_id=$1 WHERE user_id=$2",
            pocket_id, user_id
        )

async def save_trade(user_id, pair, tf, direction, confidence, explanation):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval(
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
class AccessState(StatesGroup):
    waiting_pocket_id = State()

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

def reg_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=REF_LINK)
    kb.button(text="✅ Я зарегистрировался", callback_data="reg_done")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
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

# ===================== ANALYSIS =====================
def get_signal(df: pd.DataFrame):
    signals, expl = [], []

    sma5 = ta.sma(df.Close, 5)
    sma20 = ta.sma(df.Close, 20)
    signals.append("BUY" if sma5.iloc[-1] > sma20.iloc[-1] else "SELL")
    expl.append("SMA 5/20 определяет тренд")

    ema5 = ta.ema(df.Close, 5)
    ema20 = ta.ema(df.Close, 20)
    signals.append("BUY" if ema5.iloc[-1] > ema20.iloc[-1] else "SELL")
    expl.append("EMA подтверждает направление")

    rsi = ta.rsi(df.Close)
    signals.append("BUY" if rsi.iloc[-1] < 30 else "SELL" if rsi.iloc[-1] > 70 else "BUY")
    expl.append("RSI анализ перекупленности")

    macd = ta.macd(df.Close)
    signals.append("BUY" if macd.iloc[-1,0] > macd.iloc[-1,1] else "SELL")
    expl.append("MACD сигнал")

    bb = ta.bbands(df.Close)
    signals.append("BUY" if df.Close.iloc[-1] < bb.iloc[-1,0] else "SELL")
    expl.append("Bollinger Bands")

    adx = ta.adx(df.High, df.Low, df.Close)
    signals.append("BUY" if adx.iloc[-1,0] > 25 else "SELL")
    expl.append("ADX сила тренда")

    counter = Counter(signals)
    direction, count = counter.most_common(1)[0]
    confidence = round(count / len(signals) * 100, 1)

    return direction, confidence, "\n".join(expl)

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    if not await has_access(msg.from_user.id):
        await msg.answer(
            "🚀 *Доступ к боту*\n\n"
            "1️⃣ Зарегистрируйся по ссылке\n"
            "2️⃣ Нажми «Я зарегистрировался»\n"
            "3️⃣ Отправь Pocket Option ID",
            reply_markup=reg_menu()
        )
        return
    await msg.answer("🏠 Главное меню", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "reg_done")
async def reg_done(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(AccessState.waiting_pocket_id)
    await cb.message.answer("✍️ Отправь свой Pocket Option ID")
    await cb.answer()

@dp.message(AccessState.waiting_pocket_id)
async def pocket_id(msg: types.Message, state: FSMContext):
    await save_pocket_id(msg.from_user.id, msg.text.strip())
    await state.clear()
    await msg.answer("✅ Доступ открыт!", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "pairs")
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

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb: types.CallbackQuery):
    _, pair, tf = cb.data.split(":")
    df = yf.download(pair, period="5d", interval=f"{tf}m")
    direction, confidence, expl = get_signal(df)

    trade_id = await save_trade(
        cb.from_user.id,
        pair.replace("=X",""),
        int(tf),
        direction,
        confidence,
        expl
    )

    await cb.message.edit_text(
        f"📊 *Сигнал*\n\n"
        f"Пара: {pair.replace('=X','')}\n"
        f"TF: {tf} мин\n"
        f"Направление: *{direction}*\n"
        f"Уверенность: *{confidence}%*\n\n"
        f"{expl}",
        reply_markup=result_kb(trade_id)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    _, tid, res = cb.data.split(":")
    await update_trade(int(tid), res)
    await cb.message.edit_text("✅ Результат сохранён", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "history")
async def history(cb: types.CallbackQuery):
    trades = await get_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("📜 История пуста")
        return
    text = "📜 *История сделок*\n\n"
    for t in trades:
        text += f"{t['timestamp']} | {t['pair']} | {t['direction']} | {t['result']}\n"
    await cb.message.answer(text)

# ===================== WEBHOOK =====================
async def on_startup(bot: Bot):
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))  # Telegram будет слать на /webhook

async def main():
    dp.startup.register(on_startup)

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)  # /webhook

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)  # слушаем порт Render
    await site.start()

    logging.info(f"🚀 BOT LIVE на порту {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
