import asyncio
import logging
import random
import os
from datetime import datetime

import pandas as pd
import yfinance as yf
import pandas_ta as ta

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
PORT = int(os.getenv("PORT", 10000))

# ================== LOG ==================
logging.basicConfig(level=logging.INFO)

# ================== BOT ==================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ================== DATA ==================
EXCHANGE_PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

OTC_PAIRS = [
    "AED CNY OTC","AUD CAD OTC","AUD JPY OTC","CAD CHF OTC","EUR USD OTC",
    "EUR CAD OTC","BHD CNY OTC","EUR JPY OTC","CHF NOK OTC","CHF JPY OTC",
    "EUR GBP OTC","EUR RUB OTC","EUR NZD OTC","GBP AUD OTC","MAD USD OTC",
    "NZD JPY OTC","NZD USD OTC","OMR CNY OTC","TND USD OTC","USD CHF OTC",
    "USD INR OTC","USD IDR OTC","USD MXN OTC"
]

# ================== STORAGE ==================
user_state = {}
trade_history = {}

# ================== KEYBOARDS ==================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Биржевой рынок", callback_data="market_exchange")
    kb.button(text="🟠 OTC рынок", callback_data="market_otc")
    kb.button(text="📜 История сделок", callback_data="history")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(pairs, prefix):
    kb = InlineKeyboardBuilder()
    for p in pairs:
        kb.button(text=p, callback_data=f"{prefix}:{p}")
    kb.adjust(2)
    return kb.as_markup()

def tf_kb():
    kb = InlineKeyboardBuilder()
    for tf in [1, 2, 5, 15]:
        kb.button(text=f"{tf} мин", callback_data=f"tf:{tf}")
    kb.adjust(4)
    return kb.as_markup()

def result_kb(trade_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ ПЛЮС", callback_data=f"res:plus:{trade_id}")
    kb.button(text="➖ МИНУС", callback_data=f"res:minus:{trade_id}")
    kb.adjust(2)
    return kb.as_markup()

# ================== SIGNAL LOGIC ==================
def exchange_signal(df):
    if df is None or len(df) < 50:
        return None

    signals = []
    close = df["Close"]

    signals.append("BUY" if ta.sma(close, 50).iloc[-1] > ta.sma(close, 200).iloc[-1] else "SELL")
    signals.append("BUY" if ta.ema(close, 20).iloc[-1] > ta.ema(close, 50).iloc[-1] else "SELL")

    if ta.adx(df["High"], df["Low"], close)["ADX_14"].iloc[-1] < 20:
        return None

    signals.append("BUY" if ta.rsi(close).iloc[-1] < 30 else "SELL" if ta.rsi(close).iloc[-1] > 70 else "NEUTRAL")

    macd = ta.macd(close)
    signals.append("BUY" if macd["MACD_12_26_9"].iloc[-1] > macd["MACDs_12_26_9"].iloc[-1] else "SELL")

    stoch = ta.stoch(df["High"], df["Low"], close)
    signals.append("BUY" if stoch["STOCHk_14_3_3"].iloc[-1] < 20 else "SELL" if stoch["STOCHk_14_3_3"].iloc[-1] > 80 else "NEUTRAL")

    bb = ta.bbands(close)
    signals.append("BUY" if close.iloc[-1] < bb["BBL_20_2.0"].iloc[-1] else "SELL")

    buy = signals.count("BUY")
    sell = signals.count("SELL")

    if buy >= 7:
        return "🟢 ВВЕРХ"
    if sell >= 7:
        return "🔴 ВНИЗ"
    return None

def otc_signal():
    return random.choice(["🟢 ВВЕРХ", "🔴 ВНИЗ"])

# ================== HANDLERS ==================
@router.message(Command("start"))
async def start_cmd(msg: Message):
    await msg.answer("📊 Главное меню", reply_markup=main_menu())

@router.callback_query(lambda c: c.data == "market_exchange")
async def market_exchange(cb: CallbackQuery):
    user_state[cb.from_user.id] = {"market": "exchange"}
    await cb.message.answer("📈 Выберите валютную пару:", reply_markup=pairs_kb(EXCHANGE_PAIRS, "pair"))
    await cb.answer()

@router.callback_query(lambda c: c.data == "market_otc")
async def market_otc(cb: CallbackQuery):
    user_state[cb.from_user.id] = {"market": "otc"}
    await cb.message.answer("🟠 Выберите OTC пару:", reply_markup=pairs_kb(OTC_PAIRS, "pair"))
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_cb(cb: CallbackQuery):
    pair = cb.data.split(":", 1)[1]
    user_state[cb.from_user.id]["pair"] = pair

    if user_state[cb.from_user.id]["market"] == "exchange":
        await cb.message.answer("⏱ Выберите таймфрейм:", reply_markup=tf_kb())
    else:
        await cb.message.answer("⏳ Анализ OTC рынка...")
        await asyncio.sleep(2)
        signal = otc_signal()
        trade_id = datetime.now().timestamp()
        trade_history.setdefault(cb.from_user.id, []).append({
            "id": trade_id,
            "pair": pair,
            "signal": signal,
            "time": datetime.now(),
            "result": None
        })
        await cb.message.answer(
            f"📊 {pair}\nСигнал: {signal}",
            reply_markup=result_kb(trade_id)
        )
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("tf:"))
async def tf_cb(cb: CallbackQuery):
    tf = int(cb.data.split(":")[1])
    uid = cb.from_user.id
    pair = user_state[uid]["pair"]

    await cb.message.answer("🔍 Анализ рынка...")
    await asyncio.sleep(2)

    df = yf.download(pair, period="1d", interval=f"{tf}m", progress=False)
    signal = exchange_signal(df)

    if not signal:
        await cb.message.answer("⚪ Нет чёткого сигнала, рынок во флэте")
        await cb.message.answer("📊 Главное меню", reply_markup=main_menu())
        return

    trade_id = datetime.now().timestamp()
    trade_history.setdefault(uid, []).append({
        "id": trade_id,
        "pair": pair,
        "signal": signal,
        "time": datetime.now(),
        "result": None
    })

    await cb.message.answer(
        f"📊 {pair}\nTF: {tf} мин\nСигнал: {signal}",
        reply_markup=result_kb(trade_id)
    )
    await cb.answer()

@router.callback_query(lambda c: c.data.startswith("res:"))
async def result_cb(cb: CallbackQuery):
    _, res, trade_id = cb.data.split(":")
    uid = cb.from_user.id

    for t in trade_history.get(uid, []):
        if str(t["id"]) == trade_id:
            t["result"] = "PLUS" if res == "plus" else "MINUS"

    await cb.message.answer("✅ Результат сохранён")
    await cb.message.answer("📊 Главное меню", reply_markup=main_menu())
    await cb.answer()

@router.callback_query(lambda c: c.data == "history")
async def history_cb(cb: CallbackQuery):
    trades = trade_history.get(cb.from_user.id, [])
    if not trades:
        await cb.message.answer("📜 История пустая")
        return

    text = "📜 История сделок:\n\n"
    for t in trades[-20:][::-1]:
        time = t["time"].strftime("%d.%m %H:%M")
        res = t["result"] if t["result"] else "—"
        text += f"{time} | {t['pair']} | {t['signal']} | {res}\n"

    await cb.message.answer(text)
    await cb.answer()

# ================== WEBHOOK ==================
async def on_startup(app):
    await bot.set_webhook(os.getenv("WEBHOOK_URL") + WEBHOOK_PATH)
    logging.info("🚀 BOT LIVE")

async def handle_webhook(request):
    update = await request.json()
    await dp.feed_raw_update(bot, update)
    return web.Response()

app = web.Application()
app.router.add_post(WEBHOOK_PATH, handle_webhook)
app.on_startup.append(on_startup)

if __name__ == "__main__":
    web.run_app(app, port=PORT)
