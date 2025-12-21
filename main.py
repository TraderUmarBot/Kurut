import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage

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

EXPIRATIONS = [1, 5, 10]

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    10: "15m"
}

LANGS = {
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
    "uz": "🇺🇿 O‘zbek",
    "tj": "🇹🇯 Тоҷикӣ",
    "kz": "🇰🇿 Қазақ",
    "kg": "🇰🇬 Кыргыз"
}

# ================= DATABASE =================

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0,
            lang TEXT DEFAULT 'ru'
        );
        """)
        await conn.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS lang TEXT DEFAULT 'ru';
        """)

async def upsert_user(user_id: int, lang: str = "ru"):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, lang)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_id, lang
        )

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def set_lang(user_id: int, lang: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET lang=$1 WHERE user_id=$2",
            lang, user_id
        )

async def get_lang(user_id: int) -> str:
    async with DB_POOL.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT lang FROM users WHERE user_id=$1",
            user_id
        )
        return user["lang"] if user and user["lang"] else "ru"

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance=$1 WHERE user_id=$2",
            amount, user_id
        )

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= SIGNAL CORE (УЛУЧШЕННЫЙ) =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int):
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)

        if df.empty or len(df) < 60:
            return None

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        buy, sell = 0, 0

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 60:
            buy += 2
        elif last(rsi) < 40:
            sell += 2

        if abs(buy - sell) < 2:
            return None

        direction = "📈 BUY" if buy > sell else "📉 SELL"
        strength = "🔥 СИЛЬНЫЙ СИГНАЛ" if abs(buy - sell) >= 3 else "⚡ СРЕДНИЙ СИГНАЛ"

        return direction, strength

    except Exception as e:
        logging.error(e)
        return None

# ================= KEYBOARDS =================

def lang_kb():
    kb = InlineKeyboardBuilder()
    for k, v in LANGS.items():
        kb.button(text=v, callback_data=f"lang:{k}")
    kb.adjust(2)
    return kb.as_markup()

def main_menu(lang):
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Сигналы", callback_data="pairs")
    kb.button(text="🌍 Сменить язык", callback_data="change_lang")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb():
    kb = InlineKeyboardBuilder()
    for p in PAIRS:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(3)
    return kb.as_markup()

def back_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Главное меню", callback_data="main_menu")
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    await msg.answer(
        "🌍 Выберите язык / Choose language",
        reply_markup=lang_kb()
    )

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def set_language(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    await set_lang(cb.from_user.id, lang)

    await cb.message.edit_text(
        "📘 ИНСТРУКЦИЯ KURUT TRADE\n\n"
        "1️⃣ Выбираете валютную пару\n"
        "2️⃣ Выбираете время экспирации\n"
        "3️⃣ Получаете готовый сигнал\n\n"
        "Бот анализирует рынок автоматически\n"
        "Используются EMA + RSI\n\n"
        "⬇️ Нажмите Далее",
        reply_markup=InlineKeyboardBuilder()
        .button(text="➡️ Далее", callback_data="instr2")
        .as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "🔐 ДОСТУП К БОТУ\n\n"
        "Для работы сигналов нужно:\n"
        "1️⃣ Зарегистрироваться\n"
        "2️⃣ Пополнить от 20$\n"
        "3️⃣ Проверить доступ\n\n"
        "⬇️ Продолжить",
        reply_markup=InlineKeyboardBuilder()
        .button(text="➡️ Продолжить", callback_data="get_access")
        .as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить доступ", callback_data="check_id")
    kb.adjust(1)

    await cb.message.edit_text(
        "💼 ПОЛУЧЕНИЕ ДОСТУПА\n\n"
        "Регистрация по кнопке ниже\n"
        "После пополнения нажмите Проверить",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    if await has_access(cb.from_user.id):
        lang = await get_lang(cb.from_user.id)
        await cb.message.edit_text(
            "✅ Доступ открыт\n\nВыберите действие:",
            reply_markup=main_menu(lang)
        )
    else:
        await cb.answer("⏳ Пополнение не найдено", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def mm(cb: types.CallbackQuery):
    lang = await get_lang(cb.from_user.id)
    await cb.message.edit_text(
        "🏠 Главное меню",
        reply_markup=main_menu(lang)
    )

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("❌ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text(
        "📊 Выберите валютную пару",
        reply_markup=pairs_kb()
    )

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Выберите экспирацию\n{pair.replace('=X','')}",
        reply_markup=exp_kb(pair)
    )

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    signal = await get_signal(pair, int(exp))

    if not signal:
        await cb.message.edit_text(
            "⚠️ Сейчас нет сильного сигнала\n\nПопробуйте позже",
            reply_markup=back_menu()
        )
        return

    direction, strength = signal

    await cb.message.edit_text(
        f"━━━━━━━━━━━━━━\n"
        f"📊 KURUT TRADE SIGNAL\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"💱 Пара: {pair.replace('=X','')}\n"
        f"⏱ Экспирация: {exp} мин\n\n"
        f"{direction}\n"
        f"{strength}\n\n"
        f"⚠️ Соблюдайте риск-менеджмент",
        reply_markup=back_menu()
    )

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id", "")
    amount = request.query.get("amount", "0")

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
