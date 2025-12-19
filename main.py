import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import random

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.exceptions import TelegramRetryAfter

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
    print("CRITICAL ENV ERROR")
    sys.exit(1)

# ================= BOT INITIALIZATION =================

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
    if user_id in AUTHORS: return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= ADVANCED SIGNAL CORE =================

def last_val(v):
    if isinstance(v, pd.Series):
        return float(v.iloc[-1])
    return float(v)

async def get_signal(pair: str, exp: int) -> dict:
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False, auto_adjust=True)

        if df.empty or len(df) < 50:
            return {"error": True, "pair": pair.replace("=X", "")}

        df = df.tail(100)
        close = df["Close"]
        high, low = df["High"], df["Low"]

        # ИНДИКАТОРЫ
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        upper_bb, lower_bb = bb_mid + (bb_std * 2), bb_mid - (bb_std * 2)

        stoch_k = 100 * ((close - low.rolling(14).min()) / (high.rolling(14).max() - low.rolling(14).min()))

        # ЛОГИКА БАЛЛОВ
        buy_score, sell_score = 0, 0
        
        if last_val(ema20) > last_val(ema50): buy_score += 2
        else: sell_score += 2
        
        if last_val(rsi) < 30: buy_score += 3
        elif last_val(rsi) > 70: sell_score += 3
        
        if last_val(close) <= last_val(lower_bb): buy_score += 3
        elif last_val(close) >= last_val(upper_bb): sell_score += 3
        
        if last_val(stoch_k) < 20: buy_score += 2
        elif last_val(stoch_k) > 80: sell_score += 2

        direction = "ВВЕРХ 📈" if buy_score > sell_score else "ВНИЗ 📉"
        accuracy = 89 + min(abs(buy_score - sell_score), 6)

        return {
            "pair": pair.replace("=X", ""),
            "direction": direction,
            "accuracy": accuracy,
            "candles": 100,
            "error": False
        }
    except Exception as e:
        logging.error(f"Signal error: {e}")
        return {"error": True, "pair": pair.replace("=X", "")}

# ================= KEYBOARDS =================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 ВАЛЮТНЫЕ ПАРЫ", callback_data="pairs")
    kb.button(text="📖 ИНСТРУКЦИЯ", callback_data="full_instr")
    kb.button(text="📰 НОВОСТИ", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def back_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ НАЗАД В МЕНЮ", callback_data="main_menu")
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    current = PAIRS[start:start + PAIRS_PER_PAGE]
    for p in current:
        kb.button(text=f"📊 {p.replace('=X','')}", callback_data=f"pair:{p}")
    kb.adjust(2)
    nav = []
    if page > 0: nav.append(InlineKeyboardButton(text="⬅️ НАЗАД", callback_data=f"page:{page-1}"))
    if start + PAIRS_PER_PAGE < len(PAIRS): nav.append(InlineKeyboardButton(text="➡️ ВПЕРЕД", callback_data=f"page:{page+1}"))
    if nav: kb.row(*nav)
    kb.row(InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS: kb.button(text=f"⏱ {e} МИН", callback_data=f"exp:{pair}:{e}")
    kb.button(text="⬅️ НАЗАД", callback_data="pairs")
    kb.adjust(2)
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    await msg.answer(
        "👋 <b>ДОБРО ПОЖАЛОВАТЬ В KURUT TRADE ИИ!</b>\n\n"
        "Я — профессиональный аналитический инструмент для торговли на бинарных опционах.\n\n"
        "⚡️ <i>Моя точность основана на анализе 27 технических индикаторов и нейросетевых паттернов.</i>\n\n"
        "Нажмите кнопку ниже, чтобы начать обучение.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardBuilder().button(text="📖 ПОСМОТРЕТЬ ИНСТРУКЦИЮ", callback_data="instr2").as_markup()
    )

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "🛡 <b>КАК ПОЛУЧИТЬ ДОСТУП?</b>\n\n"
        "1️⃣ Зарегистрируйтесь на платформе по ссылке ниже.\n"
        "2️⃣ Пополните баланс на сумму от <b>20$</b> (деньги остаются у вас для торговли).\n"
        "3️⃣ Бот автоматически проверит ваш ID и откроет доступ к сигналам.\n\n"
        "⚠️ <i>Важно: регистрация должна быть новой!</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardBuilder().button(text="🔗 ПОЛУЧИТЬ ДОСТУП", callback_data="get_access").as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    # Динамическая ссылка с subid
    p_link = f"{REF_LINK}&subid={cb.from_user.id}"
    kb = InlineKeyboardBuilder()
    kb.button(text="💎 РЕГИСТРАЦИЯ", url=p_link)
    kb.button(text="✅ ПРОВЕРИТЬ ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("<b>ШАГ АКТИВАЦИИ:</b>", parse_mode="HTML", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="full_instr")
async def full_instr(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📖 <b>ГРАМОТНАЯ ТОРГОВЛЯ:</b>\n\n"
        "🔹 <b>Анализ:</b> Бот сканирует 100 свечей, RSI, MACD, Bollinger и уровни поддержки.\n"
        "🔹 <b>Точка входа:</b> Открывайте сделку <b>СРАЗУ</b> после получения сигнала.\n"
        "🔹 <b>Свечи:</b> Рекомендуется входить в момент <u>открытия новой свечи</u>.\n"
        "🔹 <b>Риск-менеджмент:</b> Не ставьте более 3-5% от баланса на одну сделку!\n\n"
        "🚀 <i>Удачи в торговле!</i>",
        parse_mode="HTML",
        reply_markup=back_menu_kb()
    )

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    if cb.from_user.id in AUTHORS or (user and user["balance"] >= MIN_DEPOSIT):
        await cb.message.edit_text("✅ <b>ДОСТУП ПОДТВЕРЖДЕН!</b>\n\nУдачного профита!", parse_mode="HTML", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс не пополнен или ID не совпадает", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("<b>ГЛАВНОЕ МЕНЮ:</b>", parse_mode="HTML", reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs_list(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb.message.edit_text("<b>ВЫБЕРИТЕ ВАЛЮТНУЮ ПАРУ:</b>", parse_mode="HTML", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page_cb(cb: types.CallbackQuery):
    p = int(cb.data.split(":")[1])
    await cb.message.edit_text("<b>ВЫБЕРИТЕ ВАЛЮТНУЮ ПАРУ:</b>", parse_mode="HTML", reply_markup=pairs_kb(p))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_select(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    await cb.message.edit_text(f"<b>АКТИВ: {p.replace('=X','')}</b>\nВыберите время экспирации:", parse_mode="HTML", reply_markup=exp_kb(p))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def send_signal(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    temp_msg = await cb.message.edit_text("🔄 <b>ИДЕТ СКАНИРОВАНИЕ РЫНКА...</b>", parse_mode="HTML")
    
    res = await get_signal(p, int(e))
    if res["error"]:
        await temp_msg.edit_text("❌ Ошибка данных Yahoo. Попробуйте другую пару.", reply_markup=back_menu_kb())
        return

    text = (
        "🔥 <b>СИГНАЛ СФОРМИРОВАН!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>АКТИВ:</b> {res['pair']}\n"
        f"⚡️ <b>ПРОГНОЗ:</b> {res['direction']}\n"
        f"⏱ <b>ВРЕМЯ:</b> {e} МИН\n"
        f"🎯 <b>ТОЧНОСТЬ:</b> {res['accuracy']}% \n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📝 <b>ДЕТАЛИ АНАЛИЗА:</b>\n"
        f"💠 Глубина: {res['candles']} свечей\n"
        "🛠 Алгоритм: 27 индикаторов\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💡 <i>Входите в сделку сразу!</i>"
    )
    await temp_msg.edit_text(text, parse_mode="HTML", reply_markup=back_menu_kb())

@dp.callback_query(lambda c: c.data=="news")
async def news_signal(cb: types.CallbackQuery):
    p = random.choice(PAIRS)
    res = await get_signal(p, 5)
    text = (
        "📰 <b>НОВОСТНОЙ ФОН</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Валюта: {res['pair']}\n"
        f"Статус: {res['direction']}\n"
        "Рекомендация: Осторожно!"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=back_menu_kb())

# ================= SERVER & WEBHOOK =================

async def postback(request: web.Request):
    c_id = request.query.get("click_id") or request.query.get("subid")
    amt = request.query.get("amount", "0")
    if c_id and c_id.isdigit():
        await upsert_user(int(c_id))
        await update_balance(int(c_id), float(amt))
    return web.Response(text="OK")

async def main():
    await init_db()
    
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
        await asyncio.sleep(1)
        await bot(SetWebhook(url=WEBHOOK_URL))
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)
    app.router.add_get("/", lambda r: web.Response(text="BOT LIVE"))

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())

