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

# ================= CONFIG =================

TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"

INSTAGRAM_URL = "https://www.instagram.com/kurut_trading?igsh=MWVtZHJzcjRvdTlmYw=="
TELEGRAM_URL = "https://t.me/KURUTTRADING"

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

# ================= DATA =================

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
        await conn.execute(
            "UPDATE users SET balance=$1 WHERE user_id=$2",
            amount, user_id
        )

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= SIGNAL CORE (VIP) =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int):
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="3d", interval=interval, progress=False)

        if df.empty or len(df) < 120:
            return "⏸ ОЖИДАНИЕ", "Недостаточно данных", 0

        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        buy, sell, total = 0, 0, 0

        # === EMA TREND ===
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        ema100 = close.ewm(span=100).mean()
        total += 3

        if last(ema20) > last(ema50) > last(ema100):
            buy += 3
        elif last(ema20) < last(ema50) < last(ema100):
            sell += 3

        # === RSI ===
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        total += 2

        if 55 < last(rsi) < 70:
            buy += 2
        elif 30 < last(rsi) < 45:
            sell += 2

        # === MACD ===
        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        macd_signal = macd.ewm(span=9).mean()
        total += 2

        if last(macd) > last(macd_signal):
            buy += 2
        else:
            sell += 2

        # === BOLLINGER ===
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper = sma20 + 2 * std20
        lower = sma20 - 2 * std20
        total += 2

        if last(close) <= last(lower):
            buy += 2
        elif last(close) >= last(upper):
            sell += 2

        # === FINAL ===
        if buy > sell:
            confidence = int((buy / total) * 100)
            return "ВВЕРХ 📈", "🔥 СИЛЬНЫЙ СИГНАЛ", confidence
        elif sell > buy:
            confidence = int((sell / total) * 100)
            return "ВНИЗ 📉", "🔥 СИЛЬНЫЙ СИГНАЛ", confidence
        else:
            return "⏸ ОЖИДАНИЕ", "⚠️ Слабый рынок", 40

    except Exception as e:
        logging.error(e)
        return "❌ ОШИБКА", "Нет данных", 0

# ================= KEYBOARDS =================

def socials_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Instagram", url=INSTAGRAM_URL)
    kb.button(text="📢 Telegram", url=TELEGRAM_URL)
    kb.button(text="➡️ Далее", callback_data="instr1")
    kb.adjust(1)
    return kb.as_markup()

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
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

def back_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Главное меню", callback_data="main_menu")
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return

    await msg.answer(
        "👋 *KURUT TRADE VIP*\n\n"
        "Подписывайся на официальные каналы:",
        reply_markup=socials_kb(),
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.edit_text("📈 Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "⏱ Выберите экспирацию:",
        reply_markup=exp_kb(cb.data.split(":")[1])
    )

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level, confidence = await get_signal(pair, int(exp))

    bar = "█" * (confidence // 10) + "░" * (10 - confidence // 10)

    await cb.message.edit_text(
        f"💎 *VIP СИГНАЛ KURUT TRADE*\n\n"
        f"📊 *Пара:* `{pair.replace('=X','')}`\n"
        f"⏱ *Экспирация:* `{exp} мин`\n\n"
        f"🎯 *Направление:* {direction}\n"
        f"📌 *Качество:* {level}\n\n"
        f"📈 *Уверенность:* `{confidence}%`\n"
        f"`{bar}`\n\n"
        f"🧠 _Сигнал рассчитан по рынку в момент запроса_",
        reply_markup=back_menu(),
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, level, confidence = await get_signal(pair, exp)

    await cb.message.edit_text(
        f"📰 *НОВОСТНОЙ ИМПУЛЬС*\n\n"
        f"{pair.replace('=X','')} | {exp} мин\n"
        f"{direction}\n"
        f"Уверенность: {confidence}%",
        reply_markup=back_menu(),
        parse_mode="Markdown"
    )

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id","")
    amount = float(request.query.get("amount","0"))

    if not click_id.isdigit():
        return web.Response(text="NO CLICK")

    await upsert_user(int(click_id))
    await update_balance(int(click_id), amount)
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
