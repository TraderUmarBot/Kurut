import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import yfinance as yf

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

ADMIN_ID = 7079260196

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"

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

PAIRS_PER_PAGE = 6

# ================== DATABASE ==================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            is_allowed BOOLEAN DEFAULT FALSE
        );
        """)

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def add_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def grant_access(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET is_allowed=TRUE WHERE user_id=$1",
            user_id
        )

async def has_access(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    user = await get_user(user_id)
    return bool(user and user["is_allowed"])

# ================== SIGNAL CORE ==================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, tf: str):
    try:
        df = yf.download(pair, period="2d", interval=tf, progress=False)
        if df.empty or len(df) < 50:
            return None

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        signal = macd.ewm(span=9).mean()

        buy = sell = 0

        if last(ema20) > last(ema50): buy += 2
        else: sell += 2

        if last(rsi) > 55: buy += 2
        elif last(rsi) < 45: sell += 2

        if last(macd) > last(signal): buy += 1
        else: sell += 1

        direction = "BUY 📈" if buy > sell else "SELL 📉"
        power = abs(buy - sell)

        strength = (
            "🔥 СИЛЬНЫЙ СИГНАЛ" if power >= 4 else
            "⚡ СРЕДНИЙ СИГНАЛ" if power == 3 else
            "⚠️ СЛАБЫЙ РЫНОК"
        )

        confidence = min(95, 50 + power * 10)

        return direction, strength, confidence

    except Exception as e:
        logging.error(e)
        return None

# ================== KEYBOARDS ==================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Далее", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def tf_kb(pair):
    kb = InlineKeyboardBuilder()
    for name in EXPIRATIONS:
        kb.button(text=name, callback_data=f"tf:{pair}:{name}")
    kb.adjust(3)
    return kb.as_markup()

# ================== HANDLERS ==================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await add_user(msg.from_user.id)

    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="step2")
    await msg.answer(
        "👋 Добро пожаловать в KURUT TRADE\n\n"
        "🤖 Бот даёт сигналы на основе технического анализа\n"
        "📈 Таймфреймы: M1 / M5 / M15\n\n"
        "Нажмите «Далее»",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="step2")
async def step2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="🆔 Проверить ID", callback_data="my_id")
    kb.adjust(1)
    await cb.message.edit_text(
        "📘 КАК ПОЛУЧИТЬ ДОСТУП\n\n"
        "1️⃣ Зарегистрируйтесь по ссылке\n"
        "2️⃣ Нажмите «Проверить ID»\n"
        "3️⃣ Отправьте ID админу\n"
        "4️⃣ Админ выдаст доступ\n",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="my_id")
async def my_id(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="✉️ Написать админу", url=f"tg://user?id={ADMIN_ID}")
    await cb.message.edit_text(
        f"🆔 Ваш Telegram ID:\n\n{cb.from_user.id}\n\n"
        "Отправьте этот ID админу",
        reply_markup=kb.as_markup()
    )

@dp.message(Command("grant"))
async def grant(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return

    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("❌ Использование: /grant USER_ID")
        return

    uid = int(parts[1])
    await add_user(uid)
    await grant_access(uid)

    await msg.answer(f"✅ Доступ выдан пользователю {uid}")
    try:
        await bot.send_message(
            uid,
            "✅ Вам выдан доступ!\n\nНажмите кнопку ниже 👇",
            reply_markup=main_menu()
        )
    except:
        pass

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("⛔ Доступ не выдан", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите таймфрейм:", reply_markup=tf_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf(cb: types.CallbackQuery):
    _, pair, tf_name = cb.data.split(":")
    res = await get_signal(pair, EXPIRATIONS[tf_name])
    if not res:
        await cb.message.edit_text("⚠️ Сейчас нет сильного сигнала")
        return

    direction, strength, confidence = res

    bar = "█" * (confidence // 10) + "░" * (10 - confidence // 10)

    await cb.message.edit_text(
        f"💎 VIP СИГНАЛ KURUT TRADE\n\n"
        f"📊 Пара: {pair.replace('=X','')}\n"
        f"⏱ Таймфрейм: {tf_name}\n\n"
        f"🎯 Направление: {direction}\n"
        f"📌 Качество: {strength}\n\n"
        f"📈 Уверенность: {confidence}%\n{bar}",
        reply_markup=main_menu()
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📰 НОВОСТИ\n\n"
        "Следите за экономическим календарём\n"
        "Избегайте входов во время сильных новостей",
        reply_markup=main_menu()
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
