# main.py — AI TECH SIGNAL BOT (Pocket Option + aiogram v3 + webhook + 10 индикаторов + подписка на канал)

import os
import sys
import asyncio
import logging
from datetime import datetime

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
from aiogram.methods import DeleteWebhook, SetWebhook, GetChatMember

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# ===================== CONFIG =====================

TG_TOKEN = os.environ.get("TG_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 10000))
HOST = "0.0.0.0"
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

CHANNEL_USERNAME = "@KURUTTRADING"  # канал для подписки

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ TG_TOKEN или RENDER_EXTERNAL_HOSTNAME не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================

bot = Bot(token=TG_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher(storage=MemoryStorage())

DB_POOL = None

# ===================== CONSTANTS =====================

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
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
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

async def save_trade(user_id: int, pair: str, tf: int, direction: str) -> int:
    await save_user(user_id)
    if not DB_POOL:
        return int(datetime.now().timestamp())
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO trades (user_id, pair, timeframe, direction) VALUES ($1,$2,$3,$4) RETURNING id",
            user_id, pair, tf, direction
        )

async def update_trade(trade_id: int, result: str):
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE trades SET result=$1 WHERE id=$2", result, trade_id)

async def get_trade_history(user_id: int):
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            return await conn.fetch("SELECT * FROM trades WHERE user_id=$1 ORDER BY timestamp DESC", user_id)
    return []

# ===================== FSM =====================

class Form(StatesGroup):
    choosing_pair = State()
    choosing_tf = State()

# ===================== KEYBOARDS =====================

def main_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📈 Валютные пары", callback_data="choose_pair")
    b.button(text="📜 История сделок", callback_data="history")
    b.adjust(2)
    return b.as_markup()

def pairs_kb(page=0):
    b = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    for p in PAIRS[start:end]:
        b.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
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

# ===================== SUBSCRIPTION =====================

async def check_subscription(user_id: int):
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status not in ["left", "kicked"]
    except:
        return False

# ===================== ANALYSIS =====================

def get_signal(df: pd.DataFrame):
    signals = []

    # 1. SMA
    sma_short = ta.sma(df['Close'], length=5)
    sma_long = ta.sma(df['Close'], length=20)
    signals.append("BUY" if sma_short.iloc[-1] > sma_long.iloc[-1] else "SELL")

    # 2. EMA
    ema_short = ta.ema(df['Close'], length=5)
    ema_long = ta.ema(df['Close'], length=20)
    signals.append("BUY" if ema_short.iloc[-1] > ema_long.iloc[-1] else "SELL")

    # 3. RSI
    rsi = ta.rsi(df['Close'], length=14)
    if rsi.iloc[-1] < 30:
        signals.append("BUY")
    elif rsi.iloc[-1] > 70:
        signals.append("SELL")

    # 4. MACD
    macd = ta.macd(df['Close'])
    signals.append("BUY" if macd["MACD_12_26_9"].iloc[-1] > macd["MACDs_12_26_9"].iloc[-1] else "SELL")

    # 5. Stochastic
    stoch = ta.stoch(df['High'], df['Low'], df['Close'])
    if stoch["STOCHk_14_3_3"].iloc[-1] < 20:
        signals.append("BUY")
    elif stoch["STOCHk_14_3_3"].iloc[-1] > 80:
        signals.append("SELL")

    # 6. Bollinger Bands
    bb = ta.bbands(df['Close'])
    if df['Close'].iloc[-1] < bb['BBL_5_2.0'].iloc[-1]:
        signals.append("BUY")
    elif df['Close'].iloc[-1] > bb['BBU_5_2.0'].iloc[-1]:
        signals.append("SELL")

    # 7. ADX
    adx = ta.adx(df['High'], df['Low'], df['Close'])
    if adx['ADX_14'].iloc[-1] > 25:
        signals.append("BUY" if df['Close'].iloc[-1] > df['Close'].iloc[-2] else "SELL")

    # 8. CCI
    cci = ta.cci(df['High'], df['Low'], df['Close'])
    if cci.iloc[-1] < -100:
        signals.append("BUY")
    elif cci.iloc[-1] > 100:
        signals.append("SELL")

    # 9. OBV
    obv = ta.obv(df['Close'], df['Volume'])
    signals.append("BUY" if obv.iloc[-1] > obv.iloc[-2] else "SELL")

    # 10. ATR trend
    atr = ta.atr(df['High'], df['Low'], df['Close'])
    signals.append("BUY" if df['Close'].iloc[-1] > df['Close'].iloc[-2] else "SELL")

    # Консенсус
    from collections import Counter
    final_signal = Counter(signals).most_common(1)[0][0]
    return final_signal

# ===================== HANDLERS =====================

@dp.message(Command("start"))
async def start_cmd(msg: types.Message, state: FSMContext):
    await state.clear()
    if not await check_subscription(msg.from_user.id):
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Подписаться на канал", url=CHANNEL_USERNAME)
        kb.adjust(1)
        await msg.answer("📢 Чтобы использовать бота, подпишитесь на канал", reply_markup=kb.as_markup())
        return
    await save_user(msg.from_user.id)
    await msg.answer("Добро пожаловать! Выберите опцию:", reply_markup=main_kb())

@dp.callback_query(lambda c: c.data=="choose_pair")
async def choose_pair_cb(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.choosing_pair)
    await cb.message.edit_text("📈 Выбери валютную пару:", reply_markup=pairs_kb())
    await cb.answer()

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
    await cb.message.edit_text(f"Пара **{pair.replace('=X','')}**, выбери ТФ:", reply_markup=tf_kb(pair))
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf_cb(cb: types.CallbackQuery, state: FSMContext):
    _, pair, tf = cb.data.split(":")
    tf = int(tf)
    await cb.answer("⏳ Анализ...")

    # Получаем данные через yfinance
    df = yf.download(pair, period="1d", interval=f"{tf}m")
    if df.empty:
        await cb.message.edit_text("❌ Ошибка получения данных")
        return

    direction = get_signal(df)
    trade_id = await save_trade(cb.from_user.id, pair.replace("=X",""), tf, direction)
    await cb.message.edit_text(
        f"📊 **Сигнал**\n\nПара: {pair.replace('=X','')}\nTF: {tf} мин\n\nНаправление: {direction}",
        reply_markup=result_kb(trade_id)
    )

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res_cb(cb: types.CallbackQuery):
    _, trade_id, result = cb.data.split(":")
    await update_trade(int(trade_id), result)
    await cb.message.answer("✅ Результат сохранён")
    # Возврат в главное меню
    await cb.message.answer("Выберите опцию:", reply_markup=main_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data=="history")
async def history_cb(cb: types.CallbackQuery):
    trades = await get_trade_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("📜 История пустая")
    else:
        text = "📜 История сделок:\n\n"
        for t in trades[:20]:
            ts = t["timestamp"].strftime("%Y-%m-%d %H:%M")
            text += f"{ts} | {t['pair']} | {t['timeframe']} мин | {t['direction']} | {t['result']}\n"
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
