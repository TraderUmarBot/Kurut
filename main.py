import os
import sys
import asyncio
import logging
import io
from typing import Tuple
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InputFile

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.methods import DeleteWebhook, SetWebhook

# ================= CONFIG =================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR")
    sys.exit(1)

# ================= BOT =================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================= CONSTANTS =================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = [1, 5, 15]
PAIRS_PER_PAGE = 6

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    15: "15m"
}

# ================= DATABASE =================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0
        );
        """)

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, user_id)

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= UTILS =================
def last(v: pd.Series):
    return float(v.iloc[-1])

# ================= SIGNAL CORE =================
async def get_signal(pair: str, exp: int) -> Tuple[str, io.BytesIO]:
    try:
        interval = INTERVAL_MAP[exp]
        period = "1d" if interval in ["1m", "5m"] else "2d"
        df = yf.download(pair, period=period, interval=interval, progress=False, threads=True, auto_adjust=True)
        if df.empty or len(df) < 50:
            return "NO_DATA", None

        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        volume = df["Volume"]

        # EMA
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        ema200 = close.ewm(span=200).mean()

        # RSI
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        # MACD
        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        signal_line = macd.ewm(span=9).mean()

        # OBV
        obv = (np.sign(delta) * volume).fillna(0).cumsum()

        # Scores
        buy_score = sell_score = 0
        if ema20.iloc[-1] > ema50.iloc[-1] > ema200.iloc[-1]: buy_score += 3
        if ema20.iloc[-1] < ema50.iloc[-1] < ema200.iloc[-1]: sell_score += 3
        if rsi.iloc[-1] > 55: buy_score += 2
        if rsi.iloc[-1] < 45: sell_score += 2
        if macd.iloc[-1] > signal_line.iloc[-1]: buy_score += 2
        else: sell_score += 2
        if obv.iloc[-1] > obv.iloc[-2]: buy_score += 1
        else: sell_score += 1

        if buy_score >= sell_score + 2:
            direction = "ВВЕРХ 📈"
        elif sell_score >= buy_score + 2:
            direction = "ВНИЗ 📉"
        else:
            direction = "NO_SIGNAL"

        # Graph
        graph_buf = io.BytesIO()
        plt.figure(figsize=(10,5))
        plt.plot(close, label="Close")
        plt.plot(ema20, label="EMA20")
        plt.plot(ema50, label="EMA50")
        plt.plot(ema200, label="EMA200")
        plt.title(f"{pair.replace('=X','')} Signal: {direction}")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(graph_buf, format="png")
        plt.close()
        graph_buf.seek(0)

        return direction, graph_buf

    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "ERROR", None

# ================= KEYBOARDS =================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def back_to_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Главное меню", callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr2")
    await msg.answer(
        "📘 ИНСТРУКЦИЯ KURUT TRADE\n\n"
        "Бот анализирует рынок по 15 индикаторам\n"
        "Использует 3 стратегии\n"
        "Работает 24/7",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    await cb.message.edit_text(
        "Чтобы получить доступ:\n"
        "1️⃣ Регистрация по ссылке\n"
        "2️⃣ Пополнение от 20$\n"
        "3️⃣ Проверка ID",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("Доступ к боту:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить пополнение", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text("⏳ Ожидаем пополнение от 20$", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_balance")
async def check_balance(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, graph_buf = await get_signal(pair, exp)
    kb = back_to_menu_kb()
    if direction in ["NO_SIGNAL","NO_DATA","ERROR"]:
        await cb.message.edit_text("⚠️ Сейчас нет сильного сигнала", reply_markup=kb)
        return
    graph_buf.seek(0)
    # Исправлено: используем InputFile с BytesIO напрямую
    photo_file = InputFile(file=graph_buf, filename="signal.png")
    await cb.message.answer_photo(
        photo=photo_file,
        caption=f"📊 СИГНАЛ KURUT TRADE\n\nПара: {pair.replace('=X','')}\nЭкспирация: {exp} мин\nНаправление: {direction}",
        reply_markup=kb
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, graph_buf = await get_signal(pair, exp)
    kb = back_to_menu_kb()
    if direction in ["NO_SIGNAL","NO_DATA","ERROR"]:
        await cb.message.edit_text("⚠️ Сейчас нет новостного сигнала", reply_markup=kb)
        return
    graph_buf.seek(0)
    photo_file = InputFile(file=graph_buf, filename="signal.png")
    await cb.message.answer_photo(
        photo=photo_file,
        caption=f"📰 НОВОСТНОЙ СИГНАЛ\n\n{pair.replace('=X','')} — {exp} мин\n{direction}",
        reply_markup=kb
    )

# ================= POSTBACK =================
async def postback(request: web.Request):
    click_id = request.query.get("click_id","").strip()
    amount = request.query.get("amount","0")
    if not click_id.isdigit():
        return web.Response(text="NO CLICK_ID")
    await upsert_user(int(click_id))
    await update_balance(int(click_id), float(amount))
    return web.Response(text="OK")

# ================= START =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
