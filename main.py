import os
import sys
import asyncio
import logging
import asyncpg
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

# ================= SIGNAL =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int):
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="3d", interval=interval, progress=False)

        if df.empty or len(df) < 100:
            return "⏳ Нет данных", "⚠️ Ожидание"

        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        # === TREND ===
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        ema100 = close.ewm(span=100).mean()

        trend_up = last(ema20) > last(ema50) > last(ema100)
        trend_down = last(ema20) < last(ema50) < last(ema100)

        # === RSI ===
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        rsi_buy = 50 < last(rsi) < 70
        rsi_sell = 30 < last(rsi) < 50

        # === MACD ===
        macd = close.ewm(span=12).mean() - close.ewm(span=26).mean()
        signal = macd.ewm(span=9).mean()

        macd_buy = last(macd) > last(signal)
        macd_sell = last(macd) < last(signal)

        # === ATR (volatility) ===
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()

        volatility_ok = last(atr) > atr.mean()

        # === STOCHASTIC ===
        low14 = low.rolling(14).min()
        high14 = high.rolling(14).max()
        stoch = 100 * (close - low14) / (high14 - low14)

        stoch_buy = last(stoch) > 50
        stoch_sell = last(stoch) < 50

        # === SCORE SYSTEM (15 условий) ===
        buy, sell = 0, 0

        buy += trend_up
        sell += trend_down

        buy += rsi_buy
        sell += rsi_sell

        buy += macd_buy
        sell += macd_sell

        buy += volatility_ok
        sell += volatility_ok

        buy += last(ema20) > last(ema50)
        sell += last(ema20) < last(ema50)

        buy += last(close) > last(ema20)
        sell += last(close) < last(ema20)

        buy += stoch_buy
        sell += stoch_sell

        # === FINAL ===
        if buy >= 6 and buy > sell:
            return "ВВЕРХ 📈", "🔥 СИЛЬНЫЙ СИГНАЛ"
        elif sell >= 6 and sell > buy:
            return "ВНИЗ 📉", "🔥 СИЛЬНЫЙ СИГНАЛ"
        else:
            return "⏸ ОЖИДАНИЕ", "⚠️ Слабый рынок"

    except Exception as e:
        return "❌ Ошибка", "Нет данных"

# ================= KEYBOARDS =================

def socials_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Instagram", url=INSTAGRAM_URL)
    kb.button(text="📢 Telegram", url=TELEGRAM_URL)
    kb.button(text="➡️ Далее", callback_data="instr1")
    kb.adjust(1)
    return kb.as_markup()

def instr_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Далее", callback_data="instr2")
    return kb.as_markup()

def access_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    return kb.as_markup()

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.adjust(1)
    return kb.as_markup()

# ================= START =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu())
        return

    await msg.answer(
        "👋 Добро пожаловать в KURUT TRADE\n\n"
        "Подписывайся на официальные каналы 👇",
        reply_markup=socials_kb()
    )

@dp.callback_query(lambda c: c.data == "instr1")
async def instr1(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📘 ИНСТРУКЦИЯ\n\n"
        "• Анализ рынка\n"
        "• Точные сигналы\n"
        "• Подходит новичкам",
        reply_markup=instr_kb()
    )

@dp.callback_query(lambda c: c.data == "instr2")
async def instr2(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "💰 Как получить доступ:\n\n"
        "1️⃣ Зарегистрируйся\n"
        "2️⃣ Пополни от 20$\n"
        "3️⃣ Нажми «Проверить ID»",
        reply_markup=access_kb()
    )

@dp.callback_query(lambda c: c.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)

    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return

    pair = PAIRS[0]
    exp = EXPIRATIONS[0]
    direction, level = await get_signal(pair, exp)

    await cb.message.edit_text(
        f"📊 СИГНАЛ\n\n"
        f"{pair.replace('=X','')}\n"
        f"{direction}\n{level}"
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

# ================= RUN =================

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
