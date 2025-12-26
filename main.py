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

# ================= CONFIG =================

TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

ADMIN_ID = 7079260196  # единственный админ
REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"

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
            user_id BIGINT PRIMARY KEY
        );
        """)

async def grant_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def has_access(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    async with DB_POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        return bool(row)

# ================= SIGNAL CORE =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int):
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)

        if df.empty or len(df) < 50:
            return "❌ Нет данных", "Рынок слабый"

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        buy = sell = 0

        if last(ema20) > last(ema50):
            buy += 1
        else:
            sell += 1

        if last(rsi) > 55:
            buy += 1
        elif last(rsi) < 45:
            sell += 1

        direction = "📈 ВВЕРХ (BUY)" if buy > sell else "📉 ВНИЗ (SELL)"
        strength = abs(buy - sell)

        if strength == 2:
            level = "🔥 СИЛЬНЫЙ СИГНАЛ"
        else:
            level = "⚠️ СРЕДНИЙ СИГНАЛ"

        return direction, level

    except Exception as e:
        logging.error(e)
        return "❌ Ошибка", "Ошибка анализа"

# ================= KEYBOARDS =================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Получить сигнал", callback_data="pairs")
    kb.adjust(1)
    return kb.as_markup()

def back_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️", callback_data=f"page:{page+1}")
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
    if msg.from_user.id == ADMIN_ID:
        await msg.answer("👑 Админ-доступ открыт", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="info")
    await msg.answer(
        "📘 Добро пожаловать в KURUT TRADE\n\n"
        "Бот даёт торговые сигналы.\n"
        "Для доступа нужно отправить ваш Telegram ID админу.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "info")
async def info(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="🆔 Получить мой ID", callback_data="myid")
    kb.adjust(1)
    await cb.message.edit_text(
        "🧾 КАК ПОЛУЧИТЬ ДОСТУП:\n\n"
        "1️⃣ Зарегистрируйтесь по ссылке\n"
        "2️⃣ Нажмите «Получить мой ID»\n"
        "3️⃣ Отправьте ID админу\n"
        "4️⃣ Админ выдаст доступ",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "myid")
async def myid(cb: types.CallbackQuery):
    uid = cb.from_user.id
    kb = InlineKeyboardBuilder()
    kb.button(text="✉️ Написать админу", url=f"https://t.me/{ADMIN_ID}")
    kb.adjust(1)
    await cb.message.edit_text(
        f"🆔 ВАШ TELEGRAM ID:\n\n{uid}\n\n"
        "Отправьте этот ID админу для получения доступа.",
        reply_markup=kb.as_markup()
    )

@dp.message(Command("grant"))
async def grant(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return

    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("❌ Использование:\n/grant USER_ID")
        return

    user_id = int(parts[1])
    await grant_user(user_id)
    await msg.answer(f"✅ Доступ выдан пользователю {user_id}")

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию:", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level = await get_signal(pair, int(exp))

    await cb.message.edit_text(
        f"💎 СИГНАЛ KURUT TRADE\n\n"
        f"📊 Пара: {pair.replace('=X','')}\n"
        f"⏱ Экспирация: {exp} мин\n\n"
        f"🎯 Направление: {direction}\n"
        f"📌 Качество: {level}",
        reply_markup=back_menu()
    )

# ================= START =================

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
