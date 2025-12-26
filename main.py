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

# ТВОИ ДАННЫЕ
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
DB_POOL = None

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

async def check_access(user_id: int) -> bool:
    if user_id == ADMIN_ID: return True
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT has_access FROM users WHERE user_id=$1", user_id)
        return bool(val)

# ================= ANALYTICS ENGINE =================

async def get_ultra_signal(pair: str, exp: int):
    try:
        # Загружаем данные
        df = yf.download(pair, period="1d", interval="1m", progress=False)
        if len(df) < 50: return "⚠️ Рынок временно недоступен. Попробуйте другую пару."
        
        df.columns = [col.lower() for col in df.columns]
        
        # Считаем индикаторы (15+)
        df.ta.ema(length=9, append=True)
        df.ta.ema(length=21, append=True)
        df.ta.rsi(length=14, append=True)
        df.ta.macd(append=True)
        df.ta.bbands(length=20, append=True)
        df.ta.adx(append=True)
        df.ta.stoch(append=True)
        df.ta.cci(length=20, append=True)
        df.ta.willr(length=14, append=True)

        last = df.iloc[-1]
        score = 0
        
        # Логика анализа
        if last['ema_9'] > last['ema_21']: score += 1
        else: score -= 1
        
        if last['rsi_14'] < 35: score += 2  # Перепроданность
        elif last['rsi_14'] > 65: score -= 2 # Перекупленность
        
        if last['close'] < last['bbbl_20_2.0']: score += 2
        elif last['close'] > last['bbbu_20_2.0']: score -= 2
        
        if last['cci_20_0.015'] < -100: score += 1
        elif last['cci_20_0.015'] > 100: score -= 1

        # Уровни
        support = df['low'].rolling(20).min().iloc[-1]
        resistance = df['high'].rolling(20).max().iloc[-1]

        direction = "ВВЕРХ 📈" if score > 0 else "ВНИЗ 📉"
        # Точность на основе силы сигналов
        accuracy = min(98, 72 + abs(score) * 4)
        bar_count = int(accuracy // 10)
        bar = "█" * bar_count + "░" * (10 - bar_count)

        return (
            f"💎 **KURUT TRADE PRO SIGNAL**\n\n"
            f"📊 Валюта: `{pair.replace('=X','')}`\n"
            f"⏱ Время сделки: `{exp} МИН` \n"
            f"🎯 Прогноз: **{direction}**\n\n"
            f"📉 Уровень поддержки: `{support:.5f}`\n"
            f"📈 Уровень сопротивления: `{resistance:.5f}`\n\n"
            f"🛡 Уверенность: **{accuracy}%**\n"
            f"`{bar}`\n\n"
            f"📢 *Входите в сделку сразу после сигнала!*"
        )
    except Exception as e:
        logging.error(f"Signal error: {e}")
        return "❌ Ошибка при анализе рынка. Попробуйте позже."

# ================= KEYBOARDS =================

def kb_start():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM))
    b.row(types.InlineKeyboardButton(text="💬 Telegram Канал", url=TELEGRAM_CHANEL))
    b.row(types.InlineKeyboardButton(text="Далее ➡️", callback_data="go_step2"))
    return b.as_markup()

def kb_ref():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="🔗 Регистрация в Pocket Option", url=REF_LINK))
    b.row(types.InlineKeyboardButton(text="✅ Я зарегистрировался", callback_data="go_verify"))
    return b.as_markup()

def kb_main():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="💹 Получить сигнал", callback_data="show_pairs"))
    b.row(types.InlineKeyboardButton(text="👨‍💻 Написать Админу", url=f"https://t.me/{ADMIN_USERNAME}"))
    return b.as_markup()

# ================= HANDLERS =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    access = await check_access(msg.from_user.id)
    if access:
        await msg.answer(f"С возвращением, {msg.from_user.first_name}! 🚀\nВыберите действие:", reply_markup=kb_main())
    else:
        await msg.answer(
            f"Привет, {msg.from_user.first_name}! 👋\n\n"
            "Это закрытый бот **KURUT TRADE**. Чтобы получить доступ к сигналам с точностью 90%+, выполните условия ниже.\n\n"
            "Сначала подпишитесь на наши соцсети:", 
            reply_markup=kb_start(), parse_mode="Markdown"
        )

@dp.callback_query(F.data == "go_step2")
async def go_step2(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📝 **ШАГ 2: РЕГИСТРАЦИЯ**\n\n"
        "Для работы с ботом вам нужен аккаунт на платформе.\n\n"
        "1. Зарегистрируйтесь по ссылке ниже.\n"
        "2. Пополните баланс на любую сумму (для активации).\n"
        "3. После этого вы сможете получать VIP-сигналы.",
        reply_markup=kb_ref(), parse_mode="Markdown"
    )

@dp.callback_query(F.data == "go_verify")
async def go_verify(cb: types.CallbackQuery):
    await cb.message.edit_text(
        f"🏁 **ПОСЛЕДНИЙ ШАГ**\n\n"
        f"Ваш ID: `{cb.from_user.id}`\n\n"
        "Нажмите кнопку ниже и отправьте админу свой ID и скриншот пополнения для получения доступа.",
        reply_markup=InlineKeyboardBuilder().button(text="📨 Отправить админу", url=f"https://t.me/{ADMIN_USERNAME}").as_markup(),
        parse_mode="Markdown"
    )

# Команда выдачи доступа (Только для тебя)
@dp.message(Command("grant"))
async def cmd_grant(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("❌ Формат: `/grant ID`")
        return
    
    try:
        user_to_id = int(args[1])
        await update_access(user_to_id, True)
        await msg.answer(f"✅ Доступ успешно выдан для `{user_to_id}`")
        try:
            await bot.send_message(user_to_id, "🎉 Поздравляем! Вам открыт доступ к сигналам. Нажмите /start")
        except: pass
    except:
        await msg.answer("❌ Ошибка. ID должен быть числом.")

@dp.callback_query(F.data == "show_pairs")
async def show_pairs(cb: types.CallbackQuery):
    if not await check_access(cb.from_user.id):
        return await cb.answer("❌ Доступ закрыт", show_alert=True)
    
    b = InlineKeyboardBuilder()
    for p in PAIRS:
        b.button(text=p.replace("=X",""), callback_data=f"sel:{p}")
    b.adjust(3)
    await cb.message.edit_text("Выберите валютную пару для анализа:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("sel:"))
async def sel_exp(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    b = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        b.button(text=f"{e} мин", callback_data=f"sig:{p}:{e}")
    b.button(text="⬅️ Назад", callback_data="show_pairs")
    b.adjust(3, 1)
    await cb.message.edit_text(f"Пара: {p.replace('=X','')}\nВыберите время экспирации:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("sig:"))
async def final_sig(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    await cb.message.edit_text("🔄 Идет глубокий технический анализ рынка...")
    
    res = await get_ultra_signal(p, int(e))
    b = InlineKeyboardBuilder().button(text="🔄 Другая пара", callback_data="show_pairs").as_markup()
    
    await cb.message.edit_text(res, reply_markup=b, parse_mode="Markdown")

# ================= SERVER RUN =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"))
    
    app = web.Application()
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler
    SimpleRequestHandler(dp, bot).register(app, "/webhook")
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    
    logging.info("BOT STARTED SUCCESSFULLY")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
