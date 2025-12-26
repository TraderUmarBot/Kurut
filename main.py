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
AUTHORS = [6117198446, 7079260196, 5156851527]  # Авторы
MIN_DEPOSIT = 20.0
ADMIN_TG = "https://t.me/KURUTTRADING"

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
INTERVAL_MAP = {1: "1m", 5: "5m", 10: "15m"}

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
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

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

# ================= SIGNAL CORE =================
def last(v: pd.Series) -> float:
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int) -> tuple[str, str, int]:
    """Максимально точный сигнал с расчетом уверенности"""
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)
        if df.empty or len(df) < 50:
            return "ВНИЗ 📉", "⚠️ Слабый рынок", 30

        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        # EMA и RSI
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        # Простая версия ADX
        tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
        plus_dm = high.diff()
        minus_dm = low.diff() * -1
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        plus_di = 100 * plus_dm.ewm(span=14).mean() / tr.ewm(span=14).mean()
        minus_di = 100 * minus_dm.ewm(span=14).mean() / tr.ewm(span=14).mean()
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.ewm(span=14).mean()

        # Счетчики buy/sell
        buy = 0
        sell = 0

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 55:
            buy += 1
        elif last(rsi) < 45:
            sell += 1

        if last(adx) > 25:  # сильный тренд
            buy += 1
            sell += 1

        direction = "ВВЕРХ 📈" if buy > sell else "ВНИЗ 📉"
        strength = abs(buy - sell)
        if strength >= 3:
            level = "🔥 СИЛЬНЫЙ сигнал"
        elif strength == 2:
            level = "⚡ СРЕДНИЙ сигнал"
        else:
            level = "⚠️ СЛАБЫЙ рынок (риск)"

        # Уверенность %
        confidence = min(100, 30 + strength * 20)
        return direction, level, confidence

    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "ВНИЗ 📉", "⚠️ Ошибка данных", 0

# ================= KEYBOARDS =================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.button(text="💰 Проверить ID", callback_data="check_id")
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

def to_admin_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 Написать админу", url=ADMIN_TG)
    kb.adjust(1)
    return kb.as_markup()

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ открыт", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr2")
    await msg.answer("📘 Добро пожаловать в KURUT TRADE!\n\nБот анализирует рынок и дает сигналы.", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    kb.adjust(1)
    await cb.message.edit_text(
        "📘 ИНСТРУКЦИЯ KURUT TRADE\n\n"
        "1️⃣ Выберите валютную пару\n"
        "2️⃣ Выберите экспирацию\n"
        "3️⃣ Получите сигнал и анализ\n"
        "4️⃣ Чтобы получить доступ к боту, нажмите далее",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.adjust(1)
    await cb.message.edit_text("Для доступа к боту:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    if cb.from_user.id in AUTHORS:
        await cb.message.edit_text("👑 Авторский доступ открыт", reply_markup=main_menu())
        return
    await cb.message.edit_text(
        f"Ваш Telegram ID: {cb.from_user.id}\n"
        "Отправьте этот ID админу для получения доступа.",
        reply_markup=to_admin_kb()
    )

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа. Проверьте ID у администратора.", show_alert=True)
        return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию:", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level, confidence = await get_signal(pair, int(exp))
    blocks = int(confidence // 10)
    bar = "█" * blocks + "░" * (10 - blocks)
    await cb.message.edit_text(
        f"💎 VIP СИГНАЛ KURUT TRADE\n\n"
        f"📊 Пара: {pair.replace('=X','')}\n"
        f"⏱ Экспирация: {exp} мин\n"
        f"🎯 Направление: {direction}\n"
        f"📌 Качество: {level}\n\n"
        f"📈 Уверенность: {confidence}%\n{bar}\n\n"
        f"🧠 Сигнал рассчитан по рынку в момент запроса",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, level, confidence = await get_signal(pair, exp)
    blocks = int(confidence // 10)
    bar = "█" * blocks + "░" * (10 - blocks)
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\n"
        f"{pair.replace('=X','')} — {exp} мин\n"
        f"{direction}\n{level}\n"
        f"📈 Уверенность: {confidence}%\n{bar}",
        reply_markup=back_menu_kb()
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
