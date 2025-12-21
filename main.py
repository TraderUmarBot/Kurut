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
PAIRS_PER_PAGE = 6

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    10: "15m"
}

# ================= TEXT / LANG =================

TEXT = {
    "choose_lang": {
        "ru": "🌍 Выберите язык",
        "en": "🌍 Choose language",
        "uz": "🌍 Tilni tanlang",
        "tj": "🌍 Забонро интихоб кунед",
        "kz": "🌍 Тілді таңдаңыз",
        "kg": "🌍 Тилди тандаңыз"
    },
    "start": {
        "ru": "📘 ИНСТРУКЦИЯ KURUT TRADE\n\nБот анализирует рынок\nИспользует профессиональные индикаторы\nПодходит для новичков и профи",
        "en": "📘 KURUT TRADE GUIDE\n\nBot analyzes the market\nUses professional indicators\nSuitable for beginners and pros",
        "uz": "📘 KURUT TRADE\n\nBot bozorni tahlil qiladi\nProfessional indikatorlar ishlatadi",
        "tj": "📘 KURUT TRADE\n\nБот бозорро таҳлил мекунад",
        "kz": "📘 KURUT TRADE\n\nБот нарықты талдайды",
        "kg": "📘 KURUT TRADE\n\nБот рынокту талдайт"
    },
    "choose_pair": {
        "ru": "Выберите пару",
        "en": "Choose a pair",
        "uz": "Juftlikni tanlang",
        "tj": "Ҷуфтро интихоб кунед",
        "kz": "Жұпты таңдаңыз",
        "kg": "Жупту тандаңыз"
    },
    "choose_exp": {
        "ru": "Выберите экспирацию",
        "en": "Choose expiration",
        "uz": "Ekspiratsiyani tanlang",
        "tj": "Мӯҳлатро интихоб кунед",
        "kz": "Экспирацияны таңдаңыз",
        "kg": "Экспирацияны тандаңыз"
    },
    "signal": {
        "ru": "📊 СИГНАЛ KURUT TRADE",
        "en": "📊 KURUT TRADE SIGNAL",
        "uz": "📊 KURUT TRADE SIGNAL",
        "tj": "📊 СИГНАЛИ KURUT",
        "kz": "📊 KURUT СИГНАЛЫ",
        "kg": "📊 KURUT СИГНАЛЫ"
    },
    "menu": {
        "ru": "Главное меню:",
        "en": "Main menu:",
        "uz": "Asosiy menyu:",
        "tj": "Менюи асосӣ:",
        "kz": "Басты мәзір:",
        "kg": "Башкы меню:"
    }
}

def t(key: str, lang: str) -> str:
    return TEXT.get(key, {}).get(lang, TEXT[key]["ru"])

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

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
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
    user = await get_user(user_id)
    return user["lang"] if user else "ru"

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

# ================= SIGNAL CORE (НЕ ТРОГАЛ) =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int) -> tuple[str, str]:
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

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 55:
            buy += 2
        elif last(rsi) < 45:
            sell += 2

        direction = "ВВЕРХ 📈" if buy > sell else "ВНИЗ 📉"
        strength = abs(buy - sell)

        if strength >= 3:
            level = "🔥 СИЛЬНЫЙ сигнал"
        elif strength == 2:
            level = "⚡ СРЕДНИЙ сигнал"
        else:
            level = "⚠️ СЛАБЫЙ рынок (риск)"

        return direction, level

    except Exception as e:
        logging.error(e)
        return "ВНИЗ 📉", "⚠️ Ошибка данных"

# ================= KEYBOARDS =================

def lang_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🇷🇺 Русский", callback_data="lang:ru")
    kb.button(text="🇺🇿 O‘zbek", callback_data="lang:uz")
    kb.button(text="🇹🇯 Тоҷикӣ", callback_data="lang:tj")
    kb.button(text="🇰🇿 Қазақша", callback_data="lang:kz")
    kb.button(text="🇰🇬 Кыргызча", callback_data="lang:kg")
    kb.button(text="🇬🇧 English", callback_data="lang:en")
    kb.adjust(2)
    return kb.as_markup()

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.adjust(1)
    return kb.as_markup()

def back_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Главное меню", callback_data="main_menu")
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
    await upsert_user(msg.from_user.id)
    await msg.answer(TEXT["choose_lang"]["ru"], reply_markup=lang_kb())

@dp.callback_query(lambda c: c.data.startswith("lang:"))
async def choose_lang(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    await set_lang(cb.from_user.id, lang)
    await cb.message.edit_text(t("start", lang), reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    lang = await get_lang(cb.from_user.id)
    await cb.message.edit_text(t("menu", lang), reply_markup=main_menu())

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    lang = await get_lang(cb.from_user.id)
    await cb.message.edit_text(t("choose_pair", lang), reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    lang = await get_lang(cb.from_user.id)
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text(t("choose_pair", lang), reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    lang = await get_lang(cb.from_user.id)
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(t("choose_exp", lang), reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    lang = await get_lang(cb.from_user.id)

    direction, level = await get_signal(pair, int(exp))

    await cb.message.edit_text(
        f"{t('signal', lang)}\n\n"
        f"{pair.replace('=X','')}\n"
        f"{exp} min\n"
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
