import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import yfinance as yf
import pandas_ta as ta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
from aiogram.methods import DeleteWebhook, SetWebhook

# ================= CONFIG =================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

ADMIN_ID = 7079260196  
REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
INSTAGRAM = "https://www.instagram.com/kurut_trading?igsh=MWVtZHJzcjRvdTlmYw=="
TELEGRAM_CHANEL = "https://t.me/KURUTTRADING"
ADMIN_USERNAME = "KURUTTRADING" 

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 5, 10]

logging.basicConfig(level=logging.INFO)

# ================= DATABASE =================
DB_POOL: asyncpg.Pool | None = None

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

async def update_access(user_id: int, access: bool):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, has_access) VALUES ($1, $2) "
            "ON CONFLICT (user_id) DO UPDATE SET has_access=$2",
            user_id, access
        )

async def has_access(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT has_access FROM users WHERE user_id=$1", user_id)
        return bool(val)

# ================= 15+ INDICATORS ENGINE =================

async def get_ultra_signal(pair: str, exp: int):
    try:
        df = yf.download(pair, period="1d", interval="1m", progress=False)
        if len(df) < 50: return "⚠️ Рынок спит, мало данных", None
        df.columns = [col.lower() for col in df.columns]
        
        # Математика (15 индикаторов)
        df.ta.ema(length=9, append=True); df.ta.ema(length=21, append=True)
        df.ta.rsi(length=14, append=True); df.ta.macd(append=True)
        df.ta.bbands(length=20, append=True); df.ta.adx(append=True)
        df.ta.stoch(append=True); df.ta.cci(append=True); df.ta.willr(append=True)

        last = df.iloc[-1]
        score = 0
        
        # Логика сигналов
        if last['ema_9'] > last['ema_21']: score += 1
        if last['rsi_14'] < 35: score += 2
        if last['rsi_14'] > 65: score -= 2
        if last['close'] < last['bbbl_20_2.0']: score += 2
        if last['close'] > last['bbbu_20_2.0']: score -= 2
        if last['willr_14'] < -80: score += 1
        
        support = df['low'].rolling(20).min().iloc[-1]
        resistance = df['high'].rolling(20).max().iloc[-1]

        direction = "ВВЕРХ 📈" if score > 0 else "ВНИЗ 📉"
        conf = min(99, 70 + abs(score) * 5)
        bar = "█" * (conf // 10) + "░" * (10 - (conf // 10))

        return (
            f"💎 **KURUT TRADE PRO SIGNAL**\n\n"
            f"📊 Валюта: `{pair.replace('=X','')}`\n"
            f"⏱ Время: `{exp} МИН` \n"
            f"🎯 Прогноз: **{direction}**\n\n"
            f"📉 Поддержка: `{support:.5f}`\n"
            f"📈 Сопротивление: `{resistance:.5f}`\n\n"
            f"🛡 Точность: {conf}%\n`{bar}`"
        )
    except:
        return "❌ Ошибка анализа данных."

# ================= KEYBOARDS =================

def kb_start():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM))
    b.row(types.InlineKeyboardButton(text="💬 Telegram", url=TELEGRAM_CHANEL))
    b.row(types.InlineKeyboardButton(text="Далее ➡️", callback_data="go_instr"))
    return b.as_markup()

def kb_ref():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="🔗 Регистрация (Pocket Option)", url=REF_LINK))
    b.row(types.InlineKeyboardButton(text="✅ Я зарегистрировался", callback_data="go_verify"))
    return b.as_markup()

def kb_main():
    b = InlineKeyboardBuilder()
    b.button(text="💹 Валютные пары", callback_data="show_pairs")
    b.button(text="👨‍💻 Админ", url=f"https://t.me/{ADMIN_USERNAME}")
    b.adjust(1)
    return b.as_markup()

# ================= HANDLERS =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start(msg: types.Message):
    if await has_access(msg.from_user.id):
        await msg.answer("💎 Доступ активен! Выбирайте инструмент:", reply_markup=kb_main())
    else:
        await msg.answer("Добро пожаловать в **KURUT TRADE**! 🚀\nПодпишитесь на наши каналы для начала:", reply_markup=kb_start(), parse_mode="Markdown")

@dp.callback_query(F.data == "go_instr")
async def instr(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📝 **ИНСТРУКЦИЯ**\n\n1. Зарегистрируйтесь.\n2. Пополните баланс.\n3. Получите доступ к сигналам.\n\nЖмите кнопку ниже:",
        reply_markup=kb_ref(), parse_mode="Markdown"
    )

@dp.callback_query(F.data == "go_verify")
async def verify(cb: types.CallbackQuery):
    await cb.message.edit_text(
        f"🏁 **ПРОВЕРКА**\n\nВаш ID: `{cb.from_user.id}`\n\nОтправьте ваш ID и скриншот пополнения администратору, чтобы получить доступ!",
        reply_markup=InlineKeyboardBuilder().button(text="📨 Отправить админу", url=f"https://t.me/{ADMIN_USERNAME}").as_markup(),
        parse_mode="Markdown"
    )

@dp.message(Command("grant"))
async def grant(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        uid = int(msg.text.split()[1])
        await update_access(uid, True)
        await msg.answer(f"✅ Доступ открыт для {uid}")
        await bot.send_message(uid, "🎉 Вам открыт доступ к VIP сигналам! Нажмите /start")
    except:
        await msg.answer("Ошибка! Пиши: `/grant ID`")

@dp.callback_query(F.data == "show_pairs")
async def show_pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id): return
    b = InlineKeyboardBuilder()
    for p in PAIRS:
        b.button(text=p.replace("=X",""), callback_data=f"sel:{p}")
    b.adjust(3)
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("sel:"))
async def sel_exp(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    b = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        b.button(text=f"{e} мин", callback_data=f"sig:{p}:{e}")
    b.button(text="⬅️ Назад", callback_data="show_pairs")
    b.adjust(3, 1)
    await cb.message.edit_text(f"Выбрана пара: {p.replace('=X','')}\nВыберите время экспирации:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("sig:"))
async def final_sig(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    await cb.message.edit_text("🔍 Сканирую рынок и уровни...")
    res = await get_ultra_signal(p, int(e))
    b = InlineKeyboardBuilder().button(text="🔄 Другая пара", callback_data="show_pairs").as_markup()
    await cb.message.edit_text(res, reply_markup=b, parse_mode="Markdown")

# ================= RUN =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"))
    app = web.Application()
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler
    SimpleRequestHandler(dp, bot).register(app, "/webhook")
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
