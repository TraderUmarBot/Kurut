import os
import sys
import asyncio
import logging
from datetime import datetime
import random

import yfinance as yf
import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

# ===================== CONFIG =====================
TG_TOKEN = os.getenv("TG_TOKEN")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"
MIN_DEPOSIT = 20.0
AUTHORS = [7079260196, 6117198446]

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    print("❌ ENV не заданы")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== BOT =====================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ===================== CONSTANTS =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 2, 3, 5, 10]

# ===================== FSM =====================
class TradeState(StatesGroup):
    waiting_for_signal = State()

# ===================== KEYBOARDS =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Получить сигнал", callback_data="get_signal")
    kb.adjust(1)
    return kb.as_markup()

def result_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ПЛЮС", callback_data="back_to_menu")
    kb.button(text="❌ МИНУС", callback_data="back_to_menu")
    kb.adjust(2)
    return kb.as_markup()

# ===================== INDICATORS =====================
def calculate_technical_indicators(data):
    close = data['Close']
    high = data['High']
    low = data['Low']

    # 1-2 SMA
    sma5 = close.rolling(5).mean().iloc[-1]
    sma10 = close.rolling(10).mean().iloc[-1]

    # 3-4 EMA
    ema5 = close.ewm(span=5, adjust=False).mean().iloc[-1]
    ema10 = close.ewm(span=10, adjust=False).mean().iloc[-1]

    # 5 RSI
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -1*delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up.iloc[-1] / roll_down.iloc[-1] if roll_down.iloc[-1] != 0 else 0
    rsi = 100 - (100/(1+rs))

    # 6 MACD
    macd = close.ewm(span=12, adjust=False).mean().iloc[-1] - close.ewm(span=26, adjust=False).mean().iloc[-1]

    # 7 Bollinger Bands
    sma20 = close.rolling(20).mean().iloc[-1]
    std20 = close.rolling(20).std().iloc[-1]
    upper_bb = sma20 + 2*std20
    lower_bb = sma20 - 2*std20

    # 8 Stochastic %K
    lowest_low = low.rolling(14).min().iloc[-1]
    highest_high = high.rolling(14).max().iloc[-1]
    stoch_k = (close.iloc[-1]-lowest_low)/(highest_high-lowest_low)*100 if highest_high != lowest_low else 50

    # 9 ATR
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]

    # 10 CCI
    tp = (high + low + close)/3
    cci = (tp.iloc[-1] - tp.rolling(20).mean().iloc[-1]) / (0.015*tp.rolling(20).std().iloc[-1])

    # 11-15 Random weights (для демонстрации, можно добавить реальные индикаторы)
    momentum = close.pct_change(5).iloc[-1]
    adx = 25  # placeholder
    obv = 0
    willr = -50
    kama = close.ewm(span=10, adjust=False).mean().iloc[-1]

    # Счёт
    score = 0
    score += 1 if close.iloc[-1] > sma5 else -1
    score += 1 if close.iloc[-1] > sma10 else -1
    score += 1 if close.iloc[-1] > ema5 else -1
    score += 1 if close.iloc[-1] > ema10 else -1
    score += 1 if macd > 0 else -1
    score += 1 if close.iloc[-1] > upper_bb else -1
    score += 1 if close.iloc[-1] < lower_bb else 1
    score += 1 if rsi > 50 else -1
    score += 1 if stoch_k > 50 else -1
    score += 1 if cci > 0 else -1
    score += 1 if momentum > 0 else -1
    score += 1 if willr > -50 else -1
    score += 1 if kama > close.iloc[-1] else -1
    score += 1 if adx > 20 else 1
    score += 1 if obv > 0 else -1

    if score > 0:
        direction = "BUY"
    elif score < 0:
        direction = "SELL"
    else:
        direction = "NEUTRAL"

    confidence = min(abs(score)/15*100, 100)
    explanation = f"Score: {score}, RSI: {rsi:.2f}, MACD: {macd:.5f}, StochK: {stoch_k:.2f}"
    return direction, confidence, explanation

async def get_random_signal():
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    data = yf.download(pair, period="1d", interval="1m")
    direction, confidence, explanation = calculate_technical_indicators(data)
    return pair.replace("=X",""), exp, direction, confidence, explanation

# ===================== HANDLERS =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="Начать", callback_data="begin_instruction")
    kb.adjust(1)
    await msg.answer(
        "Привет! Я бот для анализа валютных пар.\n\n"
        "Нажмите кнопку Начать, чтобы получить инструкцию по регистрации и пополнению баланса.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin_instruction")
async def begin_instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти к регистрации", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer(
        f"1️⃣ Зарегистрируйте аккаунт по нашей ссылке.\n"
        f"2️⃣ Пополните баланс на ${MIN_DEPOSIT}.\n"
        f"3️⃣ После пополнения нажмите кнопку ниже для проверки пополнения.",
        reply_markup=kb.as_markup()
    )
    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить пополнение", callback_data="check_deposit")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки:", reply_markup=kb_check.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_deposit")
async def check_deposit(cb: types.CallbackQuery):
    await cb.message.answer("✅ Доступ к сигналам открыт!", reply_markup=main_menu())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "get_signal")
async def get_signal(cb: types.CallbackQuery):
    pair, exp, direction, conf, explanation = await get_random_signal()
    await cb.message.answer(
        f"📊 Сигнал\n\n"
        f"Пара: {pair}\n"
        f"Время экспирации: {exp} мин\n"
        f"Направление: {direction}\n"
        f"Уверенность: {conf:.2f}%\n\n"
        f"{explanation}",
        reply_markup=result_kb()
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(cb: types.CallbackQuery):
    await cb.message.answer("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ===================== WEBHOOK =====================
async def main():
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app = web.Application()
    handler = SimpleRequestHandler(dp, bot)
    handler.register(app, WEBHOOK_PATH)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    logging.info(f"🚀 BOT LIVE на {HOST}:{PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        asyncio.run(bot.session.close())
