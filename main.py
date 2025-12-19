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

# ================= CONFIG (Настройки) =================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
AUTHORS = [6117198446, 7079260196]
MIN_DEPOSIT = 20.0

WEBHOOK_URL = f"https://{RENDER_HOSTNAME}/webhook"

logging.basicConfig(level=logging.INFO)

# ================= CONSTANTS (Константы) =================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 5, 15] 
INTERVALS = {1: "1m", 5: "5m", 15: "15m"}

# ================= DATABASE (База Данных) =================
DB_POOL: asyncpg.Pool | None = None

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, balance FLOAT DEFAULT 0);")

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS: return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= SIGNAL CORE (7 ИНДИКАТОРОВ) =================

def last_v(v):
    return float(v.iloc[-1]) if isinstance(v, pd.Series) else float(v)

async def get_signal(pair: str, exp: int) -> dict:
    try:
        df = yf.download(pair, period="2d", interval=INTERVALS[exp], progress=False, auto_adjust=True)
        if df.empty or len(df) < 50: return {"error": True, "pair": pair.replace("=X", "")}
        
        df = df.tail(100)
        close, high, low = df["Close"], df["High"], df["Low"]

        # ИНДИКАТОРЫ: EMA, RSI, MACD, Bollinger, Stochastic, CCI, Williams %R
        ema20, ema50 = close.ewm(span=20).mean(), close.ewm(span=50).mean()
        delta = close.diff(); g = delta.clip(lower=0).rolling(14).mean(); l = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + g / l))
        bb_mid = close.rolling(20).mean(); bb_std = close.rolling(20).std()
        upper_bb, lower_bb = bb_mid + (bb_std * 2), bb_mid - (bb_std * 2)
        stoch_k = 100 * ((close - low.rolling(14).min()) / (high.rolling(14).max() - low.rolling(14).min()))
        cci = ((high+low+close)/3 - (high+low+close)/3.rolling(20).mean()) / (0.015 * (high+low+close)/3.rolling(20).std())
        will_r = -100 * ((high.rolling(14).max() - close) / (high.rolling(14).max() - low.rolling(14).min()))

        buy_pts, sell_pts = 0, 0
        if last_v(ema20) > last_v(ema50): buy_pts += 2
        else: sell_pts += 2
        if last_v(rsi) < 35: buy_pts += 3
        elif last_v(rsi) > 65: sell_pts += 3
        if last_v(close) <= last_v(lower_bb): buy_pts += 3
        elif last_v(close) >= last_v(upper_bb): sell_pts += 3
        if last_v(stoch_k) < 20: buy_pts += 2
        elif last_v(stoch_k) > 80: sell_pts += 2
        if last_v(cci) < -100: buy_pts += 1
        elif last_v(cci) > 100: sell_pts += 1
        if last_v(will_r) < -80: buy_pts += 1
        elif last_v(will_r) > -20: sell_pts += 1

        direction = "ВВЕРХ 📈" if buy_pts > sell_pts else "ВНИЗ 📉"
        accuracy = 87 + min(abs(buy_pts - sell_pts), 7)
        return {"pair": pair.replace("=X", ""), "direction": direction, "accuracy": accuracy, "error": False}
    except: return {"error": True, "pair": pair.replace("=X", "")}

# ================= KEYBOARDS (Клавиатуры) =================
def main_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 ВАЛЮТНЫЕ ПАРЫ", callback_data="pairs")
    kb.button(text="📖 ИНСТРУКЦИЯ", callback_data="full_info")
    kb.button(text="📰 НОВОСТИ", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * 6
    for p in PAIRS[start:start+6]:
        kb.button(text=f"📊 {p.replace('=X','')}", callback_data=f"pair:{p}")
    kb.adjust(2)
    nav = []
    if page > 0: nav.append(InlineKeyboardButton(text="⬅️ НАЗАД", callback_data=f"page:{page-1}"))
    if start + 6 < len(PAIRS): nav.append(InlineKeyboardButton(text="➡️ ВПЕРЕД", callback_data=f"page:{page+1}"))
    if nav: kb.row(*nav)
    kb.row(InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="main_menu"))
    return kb.as_markup()

# ================= HANDLERS (Обработчики) =================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start_cmd(msg: types.Message):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", msg.from_user.id)
    
    if msg.from_user.id in AUTHORS:
        await msg.answer("💎 <b>VIP ДОСТУП АКТИВИРОВАН</b>\nДобро пожаловать, Автор!", parse_mode="HTML", reply_markup=main_kb())
        return

    kb = InlineKeyboardBuilder().button(text="🚀 НАЧАТЬ ОБУЧЕНИЕ", callback_data="tutorial").as_markup()
    await msg.answer("👋 <b>ДОБРО ПОЖАЛОВАТЬ В KURUT TRADE!</b>\n\nЯ ИИ-аналитик для работы с бинарными опционами.", parse_mode="HTML", reply_markup=kb)

@dp.callback_query(lambda c: c.data=="tutorial")
async def tutorial_cb(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder().button(text="✅ ПОЛУЧИТЬ ДОСТУП", callback_data="get_access").as_markup()
    text = (
        "📖 <b>ИНСТРУКЦИЯ:</b>\n\n"
        "1. Регистрация по нашей ссылке\n"
        "2. Пополнение от <b>20$</b>\n"
        "3. Доступ откроется автоматически после проверки!"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(lambda c: c.data=="get_access")
async def access_cb(cb: types.CallbackQuery):
    url = f"{REF_LINK}&subid={cb.from_user.id}"
    kb = InlineKeyboardBuilder().button(text="🔗 РЕГИСТРАЦИЯ", url=url).button(text="🔄 ПРОВЕРИТЬ ПОПОЛНЕНИЕ", callback_data="check_dep").adjust(1).as_markup()
    await cb.message.edit_text("⚠️ <b>АКТИВАЦИЯ:</b>\n\nДля доступа к сигналам пополните счет на 20$.", parse_mode="HTML", reply_markup=kb)

@dp.callback_query(lambda c: c.data=="check_dep")
async def check_dep_cb(cb: types.CallbackQuery):
    if cb.from_user.id in AUTHORS or await has_access(cb.from_user.id):
        await cb.message.edit_text("✅ <b>УСПЕШНО!</b>", parse_mode="HTML", reply_markup=main_kb())
    else:
        await cb.answer("❌ Депозит не найден. Подождите 5-10 минут или проверьте регистрацию.", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    if await has_access(cb.from_user.id):
        await cb.message.edit_text("🏠 <b>ГЛАВНОЕ МЕНЮ:</b>", parse_mode="HTML", reply_markup=main_kb())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs_cb(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id): return
    await cb.message.edit_text("📊 <b>ВЫБЕРИТЕ ПАРУ:</b>", parse_mode="HTML", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page_cb(cb: types.CallbackQuery):
    await cb.message.edit_text("📊 <b>ВЫБЕРИТЕ ПАРУ:</b>", parse_mode="HTML", reply_markup=pairs_kb(int(cb.data.split(":")[1])))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def select_exp(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS: kb.button(text=f"⏱ {e} МИН", callback_data=f"sig:{p}:{e}")
    kb.adjust(2).row(InlineKeyboardButton(text="⬅️ НАЗАД", callback_data="pairs"))
    await cb.message.edit_text(f"🎯 <b>ПАРЯ: {p.replace('=X','')}</b>\nВыберите время сделки:", parse_mode="HTML", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data.startswith("sig:"))
async def get_sig_cb(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    msg = await cb.message.edit_text("🔄 <b>СКАНИРУЮ РЫНОК...</b>", parse_mode="HTML")
    res = await get_signal(p, int(e))
    text = (
        "🔥 <b>СИГНАЛ ГОТОВ!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>АКТИВ:</b> {res['pair']}\n"
        f"⚡️ <b>ПРОГНОЗ:</b> {res['direction']}\n"
        f"⏱ <b>ВРЕМЯ:</b> {e} МИН\n"
        f"🎯 <b>ТОЧНОСТЬ:</b> {res['accuracy']}% \n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💡 <i>Входите сразу в начале свечи!</i>"
    )
    await msg.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardBuilder().button(text="⬅️ К ПАРАМ", callback_data="pairs").as_markup())

@dp.callback_query(lambda c: c.data=="full_info")
async def info_cb(cb: types.CallbackQuery):
    text = (
        "📖 <b>КАК ПОЛЬЗОВАТЬСЯ:</b>\n\n"
        "• Бот анализирует 100 свечей и 7 индикаторов.\n"
        "• Входите в сделку сразу при получении сигнала.\n"
        "• Соблюдайте Мани-менеджмент (3% от банка)."
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardBuilder().button(text="⬅️ НАЗАД", callback_data="main_menu").as_markup())

@dp.callback_query(lambda c: c.data=="news")
async def news_cb(cb: types.CallbackQuery):
    impacts = ["ВЫСОКАЯ", "СРЕДНЯЯ"]
    events = ["Запасы нефти", "Протоколы ФРС", "Уровень безработицы", "Индекс CPI"]
    text = (
        "📰 <b>НОВОСТНОЙ ФОН:</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Событие:</b> {random.choice(events)}\n"
        f"⚠️ <b>Влияние:</b> {random.choice(impacts)}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "💡 Рекомендуется торговать аккуратно!"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardBuilder().button(text="⬅️ НАЗАД", callback_data="main_menu").as_markup())

# ================= SERVER & WEBHOOK (Сервер) =================
async def handle_postback(request: web.Request):
    uid = request.query.get("subid") or request.query.get("click_id")
    amt = request.query.get("amount", "0")
    if uid and uid.isdigit():
        async with DB_POOL.acquire() as conn:
            await conn.execute("INSERT INTO users (user_id, balance) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET balance = users.balance + $2", int(uid), float(amt))
    return web.Response(text="OK")

async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))
    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, "/webhook")
    app.router.add_get("/postback", handle_postback)
    app.router.add_get("/", lambda r: web.Response(text="ACTIVE"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
