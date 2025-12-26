import os
import sys
import asyncio
import logging
import asyncpg
import yfinance as yf
import pandas as pd

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.methods import DeleteWebhook, SetWebhook

# ================== CONFIG ==================

TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
ADMIN_USERNAME = "https://t.me/KURUTTRADING"

AUTHORS = [6117198446, 7079260196, 5156851527]

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

if not TG_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_HOSTNAME:
    print("ENV ERROR")
    sys.exit(1)

# ================== BOT ==================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================== DATA ==================

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

EXPIRATIONS = {
    "M1": "1m",
    "M5": "5m",
    "M15": "15m"
}

# ================== DATABASE ==================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            access BOOLEAN DEFAULT FALSE
        );
        """)

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def grant_access(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET access=TRUE WHERE user_id=$1",
            user_id
        )

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT access FROM users WHERE user_id=$1", user_id)
        return bool(row and row["access"])

# ================== SIGNAL CORE ==================

def last(series):
    return float(series.iloc[-1])

async def get_signal(pair: str, tf: str):
    try:
        df = yf.download(pair, period="2d", interval=tf, progress=False)
        if df.empty or len(df) < 60:
            return None

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        buy = sell = 0

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 55:
            buy += 2
        elif last(rsi) < 45:
            sell += 2

        direction = "📈 ВВЕРХ" if buy > sell else "📉 ВНИЗ"
        strength = abs(buy - sell)

        if strength >= 3:
            quality = "🔥 СИЛЬНЫЙ"
        elif strength == 2:
            quality = "⚡ СРЕДНИЙ"
        else:
            quality = "⚠️ СЛАБЫЙ"

        return direction, quality

    except Exception as e:
        logging.error(f"signal error: {e}")
        return None

# ================== KEYBOARDS ==================

def kb_start():
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="next")
    kb.adjust(1)
    return kb.as_markup()

def kb_access():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="🆔 Проверить ID", callback_data="my_id")
    kb.adjust(1)
    return kb.as_markup()

def kb_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Получить сигнал", callback_data="pairs")
    kb.adjust(1)
    return kb.as_markup()

def kb_pairs():
    kb = InlineKeyboardBuilder()
    for p in PAIRS:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.adjust(2)
    return kb.as_markup()

def kb_tf(pair):
    kb = InlineKeyboardBuilder()
    for name, tf in EXPIRATIONS.items():
        kb.button(text=name, callback_data=f"tf:{pair}:{tf}:{name}")
    kb.adjust(3)
    return kb.as_markup()

def kb_back():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

# ================== HANDLERS ==================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)

    await msg.answer(
        "👋 Добро пожаловать в KURUT TRADE\n\n"
        "📊 Профессиональные сигналы\n"
        "🤖 Анализ рынка в реальном времени\n\n"
        "Нажмите «Далее», чтобы продолжить",
        reply_markup=kb_start()
    )

@dp.callback_query(lambda c: c.data=="next")
async def next_step(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📘 ИНСТРУКЦИЯ\n\n"
        "1️⃣ Зарегистрируйтесь по ссылке ниже\n"
        "2️⃣ Нажмите «Проверить ID»\n"
        "3️⃣ Отправьте ID администратору\n"
        "4️⃣ Админ откроет доступ\n\n"
        "⚠️ Доступ выдаётся вручную",
        reply_markup=kb_access()
    )

@dp.callback_query(lambda c: c.data=="my_id")
async def my_id(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Написать админу", url=ADMIN_USERNAME)
    kb.adjust(1)

    await cb.message.edit_text(
        f"🆔 Ваш Telegram ID:\n\n{cb.from_user.id}\n\n"
        "📌 Скопируйте ID и отправьте администратору",
        reply_markup=kb.as_markup()
    )

@dp.message(Command("grant"))
async def grant(msg: types.Message):
    if msg.from_user.id not in AUTHORS:
        return
    try:
        uid = int(msg.text.split()[1])
        await upsert_user(uid)
        await grant_access(uid)
        await msg.answer(f"✅ Доступ выдан пользователю {uid}")
    except:
        await msg.answer("❌ Использование: /grant USER_ID")

@dp.callback_query(lambda c: c.data=="menu")
async def menu(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Главное меню:", reply_markup=kb_main())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=kb_pairs())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите таймфрейм:", reply_markup=kb_tf(pair))

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb: types.CallbackQuery):
    _, pair, tf, name = cb.data.split(":")
    result = await get_signal(pair, tf)

    if not result:
        await cb.message.edit_text("⚠️ Сейчас нет сильного сигнала", reply_markup=kb_back())
        return

    direction, quality = result

    await cb.message.edit_text(
        f"💎 СИГНАЛ KURUT TRADE\n\n"
        f"📊 Пара: {pair.replace('=X','')}\n"
        f"⏱ Таймфрейм: {name}\n\n"
        f"🎯 Направление: {direction}\n"
        f"📌 Качество: {quality}\n\n"
        f"🧠 Анализ: EMA + RSI\n"
        f"⚠️ Торгуйте с управлением риска",
        reply_markup=kb_back()
    )

# ================== START ==================

async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
