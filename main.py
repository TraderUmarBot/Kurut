import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import io
import matplotlib.pyplot as plt
import mplfinance as mpf

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
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, user_id)

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= SIGNAL CORE =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int) -> tuple[str, str]:
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)

        if df.empty or len(df) < 50:
            return "ВНИЗ 📉", "Слабый рынок"

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        buy = 0
        sell = 0

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 55:
            buy += 2
        elif last(rsi) < 45:
            sell += 2

        if buy > sell:
            direction = "ВВЕРХ 📈"
        else:
            direction = "ВНИЗ 📉"

        strength = abs(buy - sell)

        if strength >= 3:
            level = "🔥 СИЛЬНЫЙ сигнал"
        elif strength == 2:
            level = "⚡ СРЕДНИЙ сигнал"
        else:
            level = "⚠️ СЛАБЫЙ рынок (риск)"

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
        "Бот анализирует рынок\n"
        "Использует профессиональные индикаторы\n"
        "Подходит для новичков и профи",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    await cb.message.edit_text(
        "Как получить доступ:\n"
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

    if cb.from_user.id in AUTHORS:
        await cb.message.edit_text("👑 Авторский доступ открыт", reply_markup=main_menu())
        return

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
    if cb.from_user.id in AUTHORS or (user and user["balance"] >= MIN_DEPOSIT):
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu())

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

# ==================== ИСПРАВЛЕННЫЙ EXP ====================

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    direction, level = await get_signal(pair, exp)

    # ===== Загрузка данных =====
    interval = INTERVAL_MAP[exp]
    df = yf.download(pair, period="2d", interval=interval, progress=False)
    if df.empty or len(df) < 50:
        await cb.message.edit_text("⚠️ Нет данных для построения графика", reply_markup=back_menu_kb())
        return

    # ===== Построение графика с RSI/MACD =====
    df.index.name = 'Date'
    df_mpf = df[['Open','High','Low','Close','Volume']]

    # Рассчёт индикаторов
    df['EMA20'] = df['Close'].ewm(span=20).mean()
    df['EMA50'] = df['Close'].ewm(span=50).mean()
    df['RSI'] = 100 - (100 / (1 + (df['Close'].diff().clip(lower=0).rolling(14).mean() /
                                  (-df['Close'].diff().clip(upper=0).rolling(14).mean()))))
    df['MACD'] = df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()
    df['Signal'] = df['MACD'].ewm(span=9).mean()

    # ===== Рисуем график =====
    mc = mpf.make_marketcolors(up='g', down='r', inherit=True)
    s  = mpf.make_mpf_style(marketcolors=mc, mavcolors=['#1f77b4','#ff7f0e'], rc={'font.size':10})
    addplots = [
        mpf.make_addplot(df['EMA20'], color='blue'),
        mpf.make_addplot(df['EMA50'], color='orange'),
        mpf.make_addplot(df['RSI'], panel=1, color='purple', ylabel='RSI'),
        mpf.make_addplot(df['MACD'], panel=2, color='green', ylabel='MACD'),
        mpf.make_addplot(df['Signal'], panel=2, color='red')
    ]

    fig, axlist = mpf.plot(
        df_mpf,
        type='candle',
        style=s,
        addplot=addplots,
        volume=True,
        returnfig=True,
        figsize=(10,6),
        title=f"{pair.replace('=X','')} - KURUT"
    )

    # Добавим стрелку точки входа
    if direction == "ВВЕРХ 📈":
        axlist[0].annotate('BUY', xy=(len(df)-1, df['Low'].iloc[-1]*0.995),
                           xytext=(len(df)-10, df['Low'].iloc[-1]*0.98),
                           arrowprops=dict(facecolor='green', shrink=0.05))
    else:
        axlist[0].annotate('SELL', xy=(len(df)-1, df['High'].iloc[-1]*1.005),
                           xytext=(len(df)-10, df['High'].iloc[-1]*1.02),
                           arrowprops=dict(facecolor='red', shrink=0.05))

    # Сохраняем в BytesIO
    chart_buf = io.BytesIO()
    fig.savefig(chart_buf, format='png', bbox_inches='tight')
    plt.close(fig)
    chart_buf.seek(0)
    photo = InputFile(chart_buf, filename="chart.png")

    await bot.send_photo(
        chat_id=cb.from_user.id,
        photo=photo,
        caption=f"📊 СИГНАЛ KURUT TRADE\n\nПара: {pair.replace('=X','')}\nЭкспирация: {exp} мин\nНаправление: {direction}\nКачество: {level}",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
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
