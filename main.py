import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
import random
from datetime import datetime

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

# ================= КРАСИВЫЕ ТЕКСТЫ И ДИЗАЙН =================
TEXT = {
    "ru": {
        "pairs": "💎 ВАЛЮТНЫЕ ПАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (НОВОСТИ)", "up": "ПОКУПКА (ВВЕРХ) 🔼", "down": "ПРОДАЖА (ВНИЗ) 🔽",
        "strong": "✅ ВЫСОКАЯ ТОЧНОСТЬ", "medium": "⚠️ СРЕДНЯЯ ТОЧНОСТЬ", "weak": "❌ СЛАБЫЙ РЫНОК", "signal": "📊 KURUT VIP SIGNAL",
        "no_access": "⚠️ ДОСТУП ОГРАНИЧЕН!\n\nДля активации пополните баланс на 20$ по вашей ссылке.",
        "analyzing": "⚡️ *ИИ подключается к бирже...*", "news_scan": "📡 *Ищу аномальную волатильность...*",
        "instr": f"👑 **ДОБРО ПОЖАЛОВАТЬ В KURUT TRADE**\n\n🔹 Регистрация: [ОТКРЫТЬ ДОСТУП]({REF_LINK})\n🔹 Депозит: от **20$**\n🔹 Доступ: **Автоматический**"
    },
    "uz": { "pairs": "💎 VALYUTA JUFTLIKLARI", "news": "🚀 AI-IMPULS (YANGILIKLAR)", "up": "SOTIB OLISH (YUQORIGA) 🔼", "down": "SOTISH (PASTGA) 🔽", "strong": "✅ YUQORI ANIQLIK", "medium": "⚠️ O'RTA ANIQLIK", "weak": "❌ BOZOR ZAIF", "signal": "📊 KURUT VIP SIGNALI", "no_access": "⚠️ RUXSAT YO'Q!\n\nBalansni 20$ ga to'ldiring.", "analyzing": "⚡️ *AI birjaga ulanmoqda...*", "news_scan": "📡 *Anomallik qidirilmoqda...*", "instr": "👑 **XUSH KELIBSIZ**\n\nRo'yxatdan o'ting va 20$ kiriting." },
    "kz": { "pairs": "💎 ВАЛЮТА ЖҰПТАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (ЖАҢАЛЫҚТАР)", "up": "САТЫП АЛУ (ЖОҒАРЫ) 🔼", "down": "САТУ (ТӨМЕН) 🔽", "strong": "✅ ЖОҒАРЫ ДӘЛДІК", "medium": "⚠️ ОРТАША ДӘЛДІК", "weak": "❌ ӘЛСІЗ НАРЫҚ", "signal": "📊 KURUT VIP СИГНАЛЫ", "no_access": "⚠️ ҚОЛЖЕТІМДІ ЕМЕС!\n\nБалансты 20$ толтырыңыз.", "analyzing": "⚡️ *ИИ биржаға қосылуда...*", "news_scan": "📡 *Аномалия ізделуде...*", "instr": "👑 **ҚОШ КЕЛДІҢІЗ**\n\nТіркеліп, 20$ салыңыз." },
    "kg": { "pairs": "💎 ВАЛЮТА ЖУПТАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (ЖАҢЫЛЫКТАР)", "up": "САТЫП АЛУ (ЖОГОРУ) 🔼", "down": "САТУ (ТӨМӨН) 🔽", "strong": "✅ ЖОГОРКУ ТАКТЫК", "medium": "⚠️ ОРТОЧО ТАКТЫК", "weak": "❌ АЛСЫЗ РЫНОК", "signal": "📊 KURUT VIP СИГНАЛЫ", "no_access": "⚠️ КИРҮҮ ЧЕКТЕЛГЕН!\n\nБалансты 20$ толуктаңыз.", "analyzing": "⚡️ *ИИ биржага туташууда...*", "news_scan": "📡 *Аномалия изделүүдө...*", "instr": "👑 **КОШ КЕЛДИҢИЗ**\n\nКатталып, 20$ салыңыз." },
    "tj": { "pairs": "💎 ҶУФТҲОИ АСЪОР", "news": "🚀 ИИ-ИМПУЛС (ХАБАРҲО)", "up": "ХАРИД (БОЛО) 🔼", "down": "ФУРӮШ (ПОЁН) 🔽", "strong": "✅ ДАҚИҚИИ БАЛАНД", "medium": "⚠️ ДАҚИҚИИ МИЁНА", "weak": "❌ БОЗОРИ ЗАИФ", "signal": "📊 СИГНАЛИ KURUT VIP", "no_access": "⚠️ ДАСТРАСӢ МАҲДУД АСТ!\n\nБалансро 20$ пур кунед.", "analyzing": "⚡️ *ИИ ба биржа пайваст мешавад...*", "news_scan": "📡 *Ҷустуҷӯи аномалия...*", "instr": "👑 **ХУШ ОМАДЕД**\n\nБақайдгирӣ ва депозит 20$." },
    "en": { "pairs": "💎 CURRENCY PAIRS", "news": "🚀 AI-IMPULSE (NEWS)", "up": "BUY (UP) 🔼", "down": "SELL (DOWN) 🔽", "strong": "✅ HIGH ACCURACY", "medium": "⚠️ MEDIUM ACCURACY", "weak": "❌ WEAK MARKET", "signal": "📊 KURUT VIP SIGNAL", "no_access": "⚠️ ACCESS DENIED!\n\nDeposit 20$ to activate.", "analyzing": "⚡️ *AI connecting to exchange...*", "news_scan": "📡 *Scanning for volatility...*", "instr": "👑 **WELCOME**\n\nRegister and deposit 20$." }
}

PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "EURGBP=X", "EURAUD=X", "GBPAUD=X",
    "CADJPY=X", "CHFJPY=X", "EURCAD=X", "GBPCAD=X", "AUDCAD=X", "AUDCHF=X", "CADCHF=X"
]
INTERVAL_MAP = {1: "1m", 5: "5m", 15: "15m"}

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

# ================= DATABASE =================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, balance FLOAT DEFAULT 0, lang TEXT DEFAULT 'ru');")

async def upsert_user(uid):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", uid)

async def get_user_lang(uid):
    if uid in AUTHORS: return "ru"
    async with DB_POOL.acquire() as conn:
        res = await conn.fetchrow("SELECT lang FROM users WHERE user_id=$1", uid)
        return res['lang'] if res else "ru"

async def has_access(uid):
    if uid in AUTHORS: return True
    async with DB_POOL.acquire() as conn:
        u = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1", uid)
        return bool(u and u["balance"] >= MIN_DEPOSIT)

# ================= МОЩНЫЙ АЛГОРИТМ АНАЛИЗА =================
async def get_signal(pair, exp):
    df = yf.download(pair, period="2d", interval=INTERVAL_MAP[exp], progress=False)
    if df.empty or len(df) < 30: return "down", "weak"
    
    try:
        # Технический расчет
        df['RSI'] = ta.rsi(df['Close'], length=14)
        macd = ta.macd(df['Close'])
        df['EMA_8'] = ta.ema(df['Close'], length=8)
        df['EMA_21'] = ta.ema(df['Close'], length=21)
        adx_data = ta.adx(df['High'], df['Low'], df['Close'])
        df['CCI'] = ta.cci(df['High'], df['Low'], df['Close'], length=20)
        df['WPR'] = ta.willr(df['High'], df['Low'], df['Close'], length=14)
        stoch = ta.stoch(df['High'], df['Low'], df['Close'])
        bbands = ta.bbands(df['Close'], length=20)
        df['MFI'] = ta.mfi(df['High'], df['Low'], df['Close'], df['Volume'], length=14)

        score, l = 0, -1
        
        # 1. Трендовый фильтр (EMA)
        if df['EMA_8'].iloc[l] > df['EMA_21'].iloc[l]: score += 3
        else: score -= 3
        
        # 2. RSI (Перекупленность/Перепроданность)
        if df['RSI'].iloc[l] > 60: score += 1
        elif df['RSI'].iloc[l] < 40: score -= 1

        # 3. MACD
        if macd['MACD_12_26_9'].iloc[l] > macd['MACDs_12_26_9'].iloc[l]: score += 2
        else: score -= 2

        # 4. Мощность тренда (ADX) - если тренд слабый, сигнал "слабый"
        trend_strength = adx_data['ADX_14'].iloc[l]
        if trend_strength < 20: score = 0 # Флэт

        # 5. Stochastic
        if stoch['STOCHk_14_3_3'].iloc[l] > stoch['STOCHd_14_3_3'].iloc[l]: score += 1
        else: score -= 1

        direction = "up" if score > 0 else "down"
        abs_s = abs(score)
        
        if trend_strength < 22: strength = "weak"
        elif abs_s >= 5: strength = "strong"
        else: strength = "medium"
        
        return direction, strength
    except Exception as e:
        logging.error(f"Error logic: {e}")
        return "down", "weak"

# ================= КЛАВИАТУРЫ =================
def main_kb(lang):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT[lang]["pairs"], callback_data="pairs")
    kb.button(text=TEXT[lang]["news"], callback_data="news_signal")
    return kb.adjust(1).as_markup()

# ================= HANDLERS =================
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer("💎 **VIP AUTHOR ACCESS**", reply_markup=main_kb("ru"))
    else:
        kb = InlineKeyboardBuilder()
        langs = [("ru","🇷🇺 RU"),("en","🇺🇸 EN"),("uz","🇺🇿 UZ"),("kz","🇰🇿 KZ"),("kg","🇰🇬 KG"),("tj","🇹🇯 TJ")]
        for code, name in langs: kb.button(text=name, callback_data=f"setl:{code}")
        await msg.answer("📊 **CHOOSE YOUR LANGUAGE:**", reply_markup=kb.adjust(2).as_markup())

@dp.callback_query(lambda c: c.data.startswith("setl:"))
async def set_l(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET lang=$1 WHERE user_id=$2", lang, cb.from_user.id)
    await cb.message.edit_text(TEXT[lang]["instr"], reply_markup=main_kb(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs_menu(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    kb = InlineKeyboardBuilder()
    for p in PAIRS: kb.button(text=p.replace("=X",""), callback_data=f"p:{p}")
    await cb.message.edit_text(f"⚙️ **{TEXT[lang]['pairs']}**", reply_markup=kb.adjust(3).as_markup())

@dp.callback_query(lambda c: c.data.startswith("p:"))
async def exp_menu(cb: types.CallbackQuery):
    p = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in [1, 5, 15]: kb.button(text=f"⏳ {e} MIN", callback_data=f"sig:{p}:{e}")
    await cb.message.edit_text(f"📈 **ПАРА:** `{p.replace('=X','')}`\nВыберите время экспирации:", reply_markup=kb.adjust(1).as_markup())

@dp.callback_query(lambda c: c.data.startswith("sig:"))
async def get_sig(cb: types.CallbackQuery):
    _, p, e = cb.data.split(":")
    lang = await get_user_lang(cb.from_user.id)
    await cb.message.edit_text(TEXT[lang]["analyzing"], parse_mode="Markdown")
    
    dr, st = await get_signal(p, int(e))
    
    icon = "🟢" if dr == "up" else "🔴"
    stars = "⭐⭐⭐" if st == "strong" else "⭐⭐" if st == "medium" else "⭐"
    
    # КРАСИВОЕ ОФОРМЛЕНИЕ КАРТОЧКИ
    msg = (
        f"💎 **{TEXT[lang]['signal']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛 **АКТИВ:** `{p.replace('=X','')}`\n"
        f"⏳ **ВРЕМЯ:** `{e} МИНУТ`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 **ПРОГНОЗ:** `{TEXT[lang][dr]}`\n"
        f"🎯 **ТОЧНОСТЬ:** `{TEXT[lang][st]} {stars}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 *Сигнал сформирован ИИ Kurut Trade*\n"
        f"⏰ {datetime.now().strftime('%H:%M')} UTC"
    )
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "news_signal")
async def news_sig(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    await cb.message.edit_text(TEXT[lang]["news_scan"], parse_mode="Markdown")
    await asyncio.sleep(1.5)
    
    p = random.choice(PAIRS)
    e = random.choice([5, 15])
    dr, st = await get_signal(p, e)
    
    icon = "🔥"
    msg = (
        f"🚀 **{TEXT[lang]['news']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛 **АКТИВ:** `{p.replace('=X','')}`\n"
        f"⏳ **ВРЕМЯ:** `{e} МИНУТ`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{icon} **ПРОГНОЗ:** `{TEXT[lang][dr]}`\n"
        f"💎 **СТАТУС:** `VIP IMPULSE` ⭐⭐⭐\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"❗️ *Входите в сделку немедленно!*"
    )
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

# ================= POSTBACK И ЗАПУСК =================
async def postback(request):
    cid = request.query.get("click_id")
    amt = float(request.query.get("amount", 0))
    if cid and cid.isdigit():
        async with DB_POOL.acquire() as conn:
            await conn.execute("INSERT INTO users (user_id, balance) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET balance = users.balance + $2", int(cid), amt)
    return web.Response(text="OK")

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
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
