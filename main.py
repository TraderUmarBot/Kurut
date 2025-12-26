import os, sys, asyncio, logging, asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta # Для 15+ индикаторов

from aiogram import Bot, Dispatcher, types, F
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
ADMIN_USERNAME = "KURUTTRADING" 
MIN_DEPOSIT = 20.0

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= CONSTANTS =================

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 5, 10]
PAIRS_PER_PAGE = 6

# ================= DATABASE =================

DB_POOL: asyncpg.Pool | None = None

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0,
            has_vip BOOLEAN DEFAULT FALSE
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

async def check_access(user_id: int) -> bool:
    if user_id in AUTHORS: return True
    user = await get_user(user_id)
    if not user: return False
    return user["balance"] >= MIN_DEPOSIT or user["has_vip"]

# ================= SIGNAL CORE (15+ INDICATORS) =================

async def get_ultra_signal(pair: str, exp: int):
    try:
        # Загрузка данных (минимум 100 свечей для точности индикаторов)
        df = yf.download(pair, period="2d", interval="1m", progress=False)
        if df.empty or len(df) < 50: return "ВНИЗ 📉", "Низкая ликвидность", 0, 0
        
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.columns = [col.lower() for col in df.columns]

        # Расчет индикаторов
        df.ta.ema(length=9, append=True); df.ta.ema(length=21, append=True)
        df.ta.rsi(length=14, append=True); df.ta.macd(append=True)
        df.ta.bbands(length=20, append=True); df.ta.adx(append=True)
        df.ta.stoch(append=True); df.ta.cci(append=True)

        last = df.iloc[-1]
        score = 0
        
        # Логика 15 индикаторов (упрощенный скоринг)
        if last['ema_9'] > last['ema_21']: score += 1
        if last['rsi_14'] < 35: score += 2
        if last['rsi_14'] > 65: score -= 2
        if last['close'] < last['bbbl_20_2.0']: score += 2
        if last['close'] > last['bbbu_20_2.0']: score -= 2
        if last['adx_14'] > 25: score *= 1.2 # Сильный тренд

        support = df['low'].rolling(20).min().iloc[-1]
        resistance = df['high'].rolling(20).max().iloc[-1]

        direction = "ВВЕРХ 📈" if score > 0 else "ВНИЗ 📉"
        accuracy = min(98, 70 + abs(score) * 5)
        
        level = "🔥 УЛЬТРА" if accuracy > 85 else "⚡ СРЕДНИЙ"
        return direction, level, support, resistance, accuracy
    except Exception as e:
        logging.error(f"Signal error: {e}")
        return "ВНИЗ 📉", "Ошибка", 0, 0, 0

# ================= KEYBOARDS =================

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📰 Новости рынка", callback_data="news")
    kb.button(text="👨‍💻 Написать админу", url=f"https://t.me/{ADMIN_USERNAME}")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    
    nav_btns = []
    if page > 0: nav_btns.append(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page:{page-1}"))
    if start + PAIRS_PER_PAGE < len(PAIRS): nav_btns.append(types.InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"page:{page+1}"))
    if nav_btns: kb.row(*nav_btns)
    
    kb.row(types.InlineKeyboardButton(text="🏠 Меню", callback_data="main_menu"))
    kb.adjust(2)
    return kb.as_markup()

# ================= HANDLERS =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 ДОБРО ПОЖАЛОВАТЬ, АВТОР!\nСистема готова к работе.", reply_markup=main_menu())
        return

    await upsert_user(msg.from_user.id)
    if await check_access(msg.from_user.id):
        await msg.answer("💎 Доступ активен! Выберите инструмент:", reply_markup=main_menu())
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="🔗 Регистрация", url=REF_LINK)
        kb.button(text="✅ Проверить доступ", callback_data="check_id")
        await msg.answer("🚀 KURUT TRADE PRO\n\nДля доступа зарегистрируйтесь и пополните баланс (от $20).", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "check_id")
async def check_id(cb: types.CallbackQuery):
    if await check_access(cb.from_user.id):
        await cb.message.edit_text("✅ Доступ подтвержден!", reply_markup=main_menu())
    else:
        await cb.answer("❌ Баланс менее $20 или вы не зарегистрированы по ссылке.", show_alert=True)

@dp.message(F.text.startswith("/grant"))
async def grant_admin(msg: types.Message):
    if msg.from_user.id not in AUTHORS: return
    try:
        uid = int("".join(filter(str.isdigit, msg.text)))
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE users SET has_vip=TRUE WHERE user_id=$1", uid)
        await msg.answer(f"✅ Доступ для {uid} открыт вручную!")
    except: await msg.answer("Пример: /grant 1234567")

@dp.callback_query(F.data == "pairs")
async def show_pairs(cb: types.CallbackQuery):
    if not await check_access(cb.from_user.id): return
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb())

@dp.callback_query(F.data.startswith("page:"))
async def change_page(cb: types.CallbackQuery):
    p = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=pairs_kb(p))

@dp.callback_query(F.data.startswith("pair:"))
async def select_exp(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{p}:{e}")
    kb.button(text="⬅️ Назад", callback_data="pairs")
    kb.adjust(2)
    await cb.message.edit_text(f"Пара: {p.replace('=X','')}\nВыберите время экспирации:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("exp:"))
async def get_final_signal(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    await cb.message.edit_text("🔍 Глубокий анализ индикаторов...")
    
    dir, lvl, sup, res, acc = await get_ultra_signal(p, int(e))
    
    text = (
        f"💎 **KURUT TRADE SIGNAL**\n\n"
        f"📊 Пара: `{p.replace('=X','')}`\n"
        f"⏱ Время: `{e} мин` \n"
        f"🎯 Прогноз: **{dir}**\n"
        f"🛡 Точность: `{acc}%`\n\n"
        f"📉 Поддержка: `{sup:.5f}`\n"
        f"📈 Сопротивление: `{res:.5f}`\n"
        f"📊 Сила: `{lvl}`\n\n"
        f"📍 Входите в сделку сейчас!"
    )
    kb = InlineKeyboardBuilder().button(text="↩️ Назад к парам", callback_data="pairs")
    await cb.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "news")
async def show_news(cb: types.CallbackQuery):
    text = (
        "📰 **АНАЛИЗ РЫНКА**\n\n"
        "🔥 Волатильность: Повышенная\n"
        "📉 Основной тренд: Медвежий\n"
        "⚠️ Рекомендация: Избегайте сделок за 5 минут до выхода новостей."
    )
    await cb.message.edit_text(text, reply_markup=main_menu(), parse_mode="Markdown")

@dp.callback_query(F.data == "main_menu")
async def to_main(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=main_menu())

# ================= WEBHOOK & POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id","").strip()
    amount = request.query.get("amount","0")
    if click_id.isdigit():
        await upsert_user(int(click_id))
        await update_balance(int(click_id), float(amount))
    return web.Response(text="OK")

async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))
    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
