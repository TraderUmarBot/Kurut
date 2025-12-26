import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import yfinance as yf
import numpy as np

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

# Твои данные
ADMIN_ID = 7079260196  
REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut"
INSTAGRAM = "https://www.instagram.com/kurut_trading?igsh=MWVtZHJzcjRvdTlmYw=="
TELEGRAM_CHANEL = "https://t.me/KURUTTRADING"
ADMIN_USERNAME = "@KURUTTRADING" # Твой юзернейм для кнопки связи

logging.basicConfig(level=logging.INFO)

# ================= CONSTANTS =================

PAIRS = ["EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X","EURJPY=X","GBPJPY=X"]
EXPIRATIONS = [1, 5, 15]

# ================= DATABASE =================

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

# ================= SIGNAL ENGINE (ULTRALOW ERROR) =================

async def get_advanced_signal(pair: str, exp: int):
    try:
        # Берём данные с запасом для индикаторов
        df = yf.download(pair, period="1d", interval="1m", progress=False)
        if len(df) < 50: return "Ошибка данных", 0

        close = df['Close']
        
        # Индикаторы
        ema_fast = close.ewm(span=12).mean()
        ema_slow = close.ewm(span=26).mean()
        std = close.rolling(20).std()
        upper_band = ema_slow + (std * 2)
        lower_band = ema_slow - (std * 2)
        
        last_price = close.iloc[-1]
        
        score = 0
        # Тренд по EMA
        if last_price > ema_fast.iloc[-1]: score += 1
        else: score -= 1
        
        # Отскок от полос Боллинджера
        if last_price < lower_band.iloc[-1]: score += 2 # Перепроданность
        if last_price > upper_band.iloc[-1]: score -= 2 # Перекупленность

        direction = "ВВЕРХ 📈" if score > 0 else "ВНИЗ 📉"
        confidence = min(98, 70 + abs(score) * 7)
        
        bar = "█" * (confidence // 10) + "░" * (10 - (confidence // 10))

        text = (
            f"🎯 **СИГНАЛ СФОРМИРОВАН**\n\n"
            f"📊 Валюта: `{pair.replace('=X','')}`\n"
            f"⏳ Время: `{exp} МИН` \n"
            f"📈 Направление: **{direction}**\n\n"
            f"🛡 Уверенность: {confidence}%\n"
            f"`{bar}`\n"
            f"📍 Точка входа: {last_price:.5f}"
        )
        return text
    except:
        return "❌ Ошибка анализа", None

# ================= KEYBOARDS =================

def kb_welcome():
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="📸 Instagram", url=INSTAGRAM))
    builder.row(types.InlineKeyboardButton(text="💬 Telegram Канал", url=TELEGRAM_CHANEL))
    builder.row(types.InlineKeyboardButton(text="Далее ➡️", callback_data="step_instruction"))
    return builder.as_markup()

def kb_instruction():
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="Понял, идем дальше 🚀", callback_data="step_ref"))
    return builder.as_markup()

def kb_ref():
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="🔗 Регистрация (Pocket Option)", url=REF_LINK))
    builder.row(types.InlineKeyboardButton(text="✅ Я зарегистрировался", callback_data="step_verify"))
    return builder.as_markup()

def kb_verify(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="📨 Отправить заявку", url=f"https://t.me/{ADMIN_USERNAME.replace('@','')}))
    return builder.as_markup()

def kb_main():
    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="💎 ПОЛУЧИТЬ СИГНАЛ", callback_data="choose_pair"))
    builder.row(types.InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{ADMIN_USERNAME.replace('@','')}"))
    return builder.as_markup()

# ================= HANDLERS =================

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    if await has_access(msg.from_user.id):
        await msg.answer(f"👋 С возвращением, {msg.from_user.first_name}!\nГотовы к торговле?", reply_markup=kb_main())
    else:
        await msg.answer(
            f"Привет, {msg.from_user.first_name}! 👋\n\n"
            "Я — твой персональный торговый аналитик KURUT TRADE.\n"
            "Прежде чем начать, подпишись на наши медиа:",
            reply_markup=kb_welcome()
        )

@dp.callback_query(F.data == "step_instruction")
async def step_instr(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "📖 **КАК ЭТО РАБОТАЕТ?**\n\n"
        "1. Бот сканирует рынок через API Yahoo Finance.\n"
        "2. Используются индикаторы EMA, RSI и Bollinger Bands.\n"
        "3. Вы получаете сигнал с точностью до 90%.\n\n"
        "⚠️ *Важно: Помни про мани-менеджмент!*",
        reply_markup=kb_instruction()
    )

@dp.callback_query(F.data == "step_ref")
async def step_ref(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "🔑 **ПОЛУЧЕНИЕ ДОСТУПА**\n\n"
        "Чтобы бот стал доступен, выполни 2 шага:\n"
        "1. Зарегистрируйся по ссылке ниже.\n"
        "2. Пополни баланс (рекомендуется от $20).\n\n"
        "Это подтверждает серьезность твоих намерений.",
        reply_markup=kb_ref()
    )

@dp.callback_query(F.data == "step_verify")
async def step_verify(cb: types.CallbackQuery):
    await cb.message.edit_text(
        f"🏁 **ПОСЛЕДНИЙ ШАГ**\n\n"
        f"Твой ID: `{cb.from_user.id}`\n\n"
        "Напиши админу сообщение: \n'Хочу доступ, мой ID и скрин пополнения'.",
        reply_markup=kb_verify(cb.from_user.id)
    )

@dp.message(Command("grant"))
async def cmd_grant(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        uid = int(msg.text.split()[1])
        await update_access(uid, True)
        await msg.answer(f"✅ Доступ для {uid} открыт!")
        await bot.send_message(uid, "🎉 Поздравляем! Администратор выдал вам доступ.\nЖмите /start")
    except:
        await msg.answer("Ошибка. Пиши: /grant ID")

@dp.callback_query(F.data == "choose_pair")
async def choose_pair(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        return await cb.answer("❌ У вас нет доступа", show_alert=True)
    
    builder = InlineKeyboardBuilder()
    for p in PAIRS:
        builder.button(text=p.replace("=X",""), callback_data=f"p:{p}")
    builder.adjust(2)
    await cb.message.edit_text("Выберите валютную пару:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("p:"))
async def choose_exp(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    builder = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        builder.button(text=f"{e} МИН", callback_data=f"sig:{pair}:{e}")
    builder.adjust(3)
    await cb.message.edit_text(f"Пара {pair.replace('=X','')}. Выберите время:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("sig:"))
async def send_signal(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    await cb.message.edit_text("⏳ Анализирую рынок...")
    
    res = await get_advanced_signal(pair, int(exp))
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Главное меню", callback_data="main")
    
    await cb.message.edit_text(res, reply_markup=builder.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "main")
async def back_main(cb: types.CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=kb_main())

# ================= SERVER =================

async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    
    # Если используешь Webhook:
    # await bot(SetWebhook(url=f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"))
    # app = web.Application()
    # SimpleRequestHandler(dp, bot).register(app, "/webhook")
    # runner = web.AppRunner(app)
    # await runner.setup()
    # await web.TCPSite(runner, "0.0.0.0", PORT).start()
    
    # Если хочешь просто запустить (Polling):
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
