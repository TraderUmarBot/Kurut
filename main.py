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

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
AUTHOR_IDS = [6117198446, 7079260196, 5156851527]  # Твои авторские ID
MIN_DEPOSIT = 20.0

INSTAGRAM = "https://www.instagram.com/kurut_trading?igsh=MWVtZHJzcjRvdTlmYw=="
TELEGRAM = "https://t.me/KURUTTRADING"

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
PAIRS_PER_PAGE = 6

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    10: "15m"
}

# ================= DATABASE =================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            has_access BOOLEAN DEFAULT FALSE
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

async def update_access(user_id: int, access: bool):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, has_access) VALUES ($1, $2) "
            "ON CONFLICT (user_id) DO UPDATE SET has_access=$2",
            user_id, access
        )

async def has_access(user_id: int) -> bool:
    if user_id in AUTHOR_IDS:
        return True
    user = await get_user(user_id)
    return bool(user and user["has_access"])

# ================= SIGNALS =================
def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int) -> tuple[str, str]:
    """Пример максимально точного сигнала"""
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)
        if df.empty or len(df) < 50:
            return "ВНИЗ 📉", "⚠️ Слабый рынок"

        close = df["Close"]
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        buy = 0
        sell = 0

        if last(ema20) > last(ema50): buy += 2
        else: sell += 2
        if last(rsi) > 55: buy += 2
        elif last(rsi) < 45: sell += 2

        direction = "ВВЕРХ 📈" if buy > sell else "ВНИЗ 📉"
        strength = abs(buy - sell)
        if strength >= 3: level = "🔥 СИЛЬНЫЙ сигнал"
        elif strength == 2: level = "⚡ СРЕДНИЙ сигнал"
        else: level = "⚠️ СЛАБЫЙ рынок (риск)"

        return direction, level
    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "ВНИЗ 📉", "⚠️ Ошибка данных"

# ================= KEYBOARDS =================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def back_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Главное меню", callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0: kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
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
    await upsert_user(msg.from_user.id)
    if msg.from_user.id in AUTHOR_IDS:
        await msg.answer(
            "👑 Авторский доступ открыт\n"
            "Вы можете использовать все функции бота.",
            reply_markup=main_menu()
        )
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr2")
    kb.button(text="📸 Instagram", url=INSTAGRAM)
    kb.button(text="💬 Telegram", url=TELEGRAM)
    kb.adjust(1)
    await msg.answer(
        "📘 Добро пожаловать в KURUT TRADE!\n\n"
        "Ниже наши соцсети для связи и обучения:",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    kb.adjust(1)
    await cb.message.edit_text(
        "📘 ИНСТРУКЦИЯ KURUT TRADE\n"
        "1. Зарегистрируйтесь по ссылке\n"
        "2. Пополните баланс не менее 20$\n"
        "3. Проверьте свой Telegram ID и отправьте админу",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Зарегистрироваться", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("Доступ к боту:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Написать админу", url="https://t.me/KURUTTRADING")
    kb.adjust(1)
    await cb.message.edit_text(
        f"Ваш Telegram ID: {cb.from_user.id}\n"
        f"Отправьте его админу для получения доступа",
        reply_markup=kb.as_markup()
    )

@dp.message(Command("grant"))
async def grant_access(msg: types.Message):
    if msg.from_user.id not in AUTHOR_IDS:
        await msg.answer("❌ У вас нет прав давать доступ")
        return
    parts = msg.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await msg.answer("❌ Использование: /grant USER_ID")
        return
    user_id = int(parts[1])
    await update_access(user_id, True)
    await msg.answer(f"✅ Доступ выдан пользователю {user_id}")
    try:
        await bot.send_message(
            user_id,
            "🎉 Вам предоставлен доступ к боту KURUT TRADE!\n"
            "Теперь вы можете использовать все функции.",
            reply_markup=main_menu()
        )
    except Exception as e:
        logging.warning(f"Не удалось уведомить пользователя {user_id}: {e}")

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs_cb(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page_cb(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_cb(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию:", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp_cb(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level = await get_signal(pair, int(exp))
    strength_map = {"⚠️ СЛАБЫЙ рынок (риск)": 33, "⚡ СРЕДНИЙ сигнал": 66, "🔥 СИЛЬНЫЙ сигнал": 90}
    confidence = strength_map.get(level, 50)
    blocks = int(confidence // 10)
    empty = 10 - blocks
    bar = "█" * blocks + "░" * empty
    await cb.message.edit_text(
        f"💎 VIP СИГНАЛ KURUT TRADE\n\n"
        f"📊 Пара: {pair.replace('=X','')}\n"
        f"⏱ Экспирация: {exp} мин\n\n"
        f"🎯 Направление: {direction}\n"
        f"📌 Качество: {level}\n\n"
        f"📈 Уверенность: {confidence}%\n{bar}",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data=="news")
async def news_cb(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, level = await get_signal(pair, exp)
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\n"
        f"{pair.replace('=X','')} — {exp} мин\n"
        f"{direction}\n{level}",
        reply_markup=back_menu_kb()
    )

# ================= START WEBHOOK =================
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
