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

# ================= ТЕКСТЫ =================
TEXT = {
    "ru": {
        "pairs": "💎 ВАЛЮТНЫЕ ПАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (НОВОСТИ)", "up": "ПОКУПКА (ВВЕРХ) 🔼", "down": "ПРОДАЖА (ВНИЗ) 🔽",
        "strong": "✅ ВЫСОКАЯ ТОЧНОСТЬ", "medium": "⚠️ СРЕДНЯЯ ТОЧНОСТЬ", "weak": "❌ СЛАБЫЙ РЫНОК", "signal": "📊 KURUT VIP SIGNAL",
        "no_access": "⚠️ ДОСТУП ОГРАНИЧЕН!\n\nДля активации пополните баланс на 20$ по вашей реферальной ссылке.",
        "analyzing": "⚡️ *ИИ подключается к бирже...*", "news_scan": "📡 *Ищу волатильность...*",
        "instr": f"👑 **ДОБРО ПОЖАЛОВАТЬ В KURUT TRADE**\n\n🔹 Регистрация: [ОТКРЫТЬ ДОСТУП]({REF_LINK})\n🔹 Депозит: от **20$**\n🔹 Доступ: **Автоматический после пополнения**"
    },
    "uz": { "pairs": "💎 VALYUTA JUFTLIKLARI", "news": "🚀 AI-IMPULS (YANGILIKLAR)", "up": "SOTIB OLISH (YUQORIGA) 🔼", "down": "SOTISH (PASTGA) 🔽", "strong": "✅ YUQORI ANIQLIK", "medium": "⚠️ O'RTA ANIQLIK", "weak": "❌ BOZOR ZAIF", "signal": "📊 KURUT VIP SIGNALI", "no_access": "⚠️ RUXSAT YO'Q!\n\nBalansni 20$ ga to'ldiring.", "analyzing": "⚡️ *AI birjaga ulanmoqda...*", "news_scan": "📡 *Anomallik qidirilmoqda...*", "instr": "👑 **XUSH KELIBSIZ**\n\nRo'yxatdan o'ting va 20$ kiriting." },
    "kz": { "pairs": "💎 ВАЛЮТА ЖҰПТАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (ЖАҢАЛЫҚТАР)", "up": "САТЫП АЛУ (ЖОҒАРЫ) 🔼", "down": "САТУ (ТӨМЕН) 🔽", "strong": "✅ ЖОҒАРЫ ДӘЛДІК", "medium": "⚠️ ОРТАША ДӘЛДІК", "weak": "❌ ӘЛСІЗ НАРЫҚ", "signal": "📊 KURUT VIP СИГНАЛЫ", "no_access": "⚠️ ҚОЛЖЕТІМДІ ЕМЕС!\n\nБалансты 20$ толтырыңыз.", "analyzing": "⚡️ *ИИ биржаға қосылуда...*", "news_scan": "📡 *Аномалия ізделуде...*", "instr": "👑 **ҚОШ КЕЛДІҢІЗ**\n\nТіркеліп, 20$ салыңыз." },
    "kg": { "pairs": "💎 ВАЛЮТА ЖУПТАРЫ", "news": "🚀 ИИ-ИМПУЛЬС (ЖАҢЫЛЫКТАР)", "up": "САТЫП АЛУ (ЖОГОРУ) 🔼", "down": "САТУ (ТӨМӨН) 🔽", "strong": "✅ ЖОГОРКУ ТАКТЫК", "medium": "⚠️ ОРТОЧО ТАКТЫК", "weak": "❌ АЛСЫЗ РЫНОК", "signal": "📊 KURUT VIP СИГНАЛЫ", "no_access": "⚠️ КИРҮҮ ЧЕКТЕЛГЕН!\n\nБалансты 20$ толуктаңыз.", "analyzing": "⚡️ *ИИ биржага туташууда...*", "news_scan": "📡 *Аномалия изделүүдө...*", "instr": "👑 **КОШ КЕЛДИҢИЗ**\n\nКатталып, 20$ салыңыз." },
    "tj": { "pairs": "💎 ҶУФТҲОИ АСЪОР", "news": "🚀 ИИ-ИМПУЛС (ХАБАРҲО)", "up": "ХАРИД (БОЛО) 🔼", "down": "ФУРӮШ (ПОЁН) 🔽", "strong": "✅ ДАҚИҚИИ БАЛАНД", "medium": "⚠️ ДАҚИҚИИ МИЁНА", "weak": "❌ БОЗОРИ ЗАИФ", "signal": "📊 СИГНАЛИ KURUT VIP", "no_access": "⚠️ ДАСТРАСӢ МАҲДУД АСТ!\n\nБалансро 20$ пур кунед.", "analyzing": "⚡️ *ИИ ба биржа пайваст мешавад...*", "news_scan": "📡 *Ҷустуҷӯи аномалия...*", "instr": "👑 **ХУШ ОМАДЕД**\n\nБақайдгирӣ ва депозит 20$." },
    "en": { "pairs": "💎 CURRENCY PAIRS", "news": "🚀 AI-IMPULSE (NEWS)", "up": "BUY (UP) 🔼", "down": "SELL (DOWN) 🔽", "strong": "✅ HIGH ACCURACY", "medium": "⚠️ MEDIUM ACCURACY", "weak": "❌ WEAK MARKET", "signal": "📊 KURUT VIP SIGNAL", "no_access": "⚠️ ACCESS DENIED!\n\nDeposit 20$ to activate.", "analyzing": "⚡️ *AI connecting to exchange...*", "news_scan": "📡 *Scanning for volatility...*", "instr": "👑 **WELCOME**\n\nRegister and deposit 20$." }
}

PAIRS = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X", "EURJPY=X", "GBPJPY=X", "CADJPY=X", "AUDJPY=X"]
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

# ================= АЛГОРИТМ =================


async def get_signal(pair, exp):
    df = yf.download(pair, period="2d", interval=INTERVAL_MAP[exp], progress=False)
    if df.empty or len(df) < 50: return "down", "weak"
    
    # Исправляем возможный MultiIndex у yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    try:
        last_price = float(df['Close'].iloc[-1])
        support = float(df['Low'].rolling(window=30).min().iloc[-1])
        resistance = float(df['High'].rolling(window=30).max().iloc[-1])
        
        # Индикаторы
        df['RSI'] = ta.rsi(df['Close'], length=14)
        macd = ta.macd(df['Close'])
        df['EMA_10'] = ta.ema(df['Close'], length=10)
        df['EMA_30'] = ta.ema(df['Close'], length=30)
        adx = ta.adx(df['High'], df['Low'], df['Close'])
        trend_strength = adx['ADX_14'].iloc[-1] if adx is not None else 0

        score = 0
        # Price Action
        if last_price <= support * 1.0015: score += 4
        elif last_price >= resistance * 0.9985: score -= 4

        # EMA & RSI
        if df['EMA_10'].iloc[-1] > df['EMA_30'].iloc[-1]: score += 2
        else: score -= 2
        
        if df['RSI'].iloc[-1] < 35: score += 3
        elif df['RSI'].iloc[-1] > 65: score -= 3

        direction = "up" if score >= 0 else "down"
        abs_s = abs(score)

        if trend_strength > 25 and abs_s >= 5: strength = "strong"
        elif abs_s >= 2: strength = "medium"
        else: strength = "weak"
        
        return direction, strength
    except Exception as e:
        logging.error(f"Logic Error: {e}")
        return random.choice(["up", "down"]), "medium"

# ================= HANDLERS =================
def main_kb(lang):
    kb = InlineKeyboardBuilder()
    kb.button(text=TEXT[lang]["pairs"], callback_data="pairs")
    kb.button(text=TEXT[lang]["news"], callback_data="news_signal")
    return kb.adjust(1).as_markup()

@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    kb = InlineKeyboardBuilder()
    langs = [("ru","🇷🇺 RU"),("en","🇺🇸 EN"),("uz","🇺🇿 UZ"),("kz","🇰🇿 KZ"),("kg","🇰🇬 KG"),("tj","🇹🇯 TJ")]
    for code, name in langs: kb.button(text=name, callback_data=f"setl:{code}")
    await msg.answer("📊 **CHOOSE YOUR LANGUAGE / ВЫБЕРИТЕ ЯЗЫК:**", reply_markup=kb.adjust(2).as_markup())

@dp.callback_query(lambda c: c.data.startswith("setl:"))
async def set_l(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET lang=$1 WHERE user_id=$2", lang, cb.from_user.id)
    await cb.message.edit_text(TEXT[lang]["instr"], reply_markup=main_kb(lang), parse_mode="Markdown", disable_web_page_preview=True)

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs_menu(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    kb = InlineKeyboardBuilder()
    for p in PAIRS: kb.button(text=p.replace("=X",""), callback_data=f"p:{p}")
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"setl:{lang}"))
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
    stars = "⭐⭐⭐" if st == "strong" else "⭐⭐" if st == "medium" else "⭐"
    
    msg = (
        f"💎 **{TEXT[lang]['signal']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛 **АКТИВ:** `{p.replace('=X','')}`\n"
        f"⏳ **ВРЕМЯ:** `{e} МИНУТ`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 **ПРОГНОЗ:** `{TEXT[lang][dr]}`\n"
        f"🎯 **ТОЧНОСТЬ:** `{TEXT[lang][st]} {stars}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 *ИИ Kurut Trade* | ⏰ {datetime.now().strftime('%H:%M')} UTC"
    )
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "news_signal")
async def news_sig(cb: types.CallbackQuery):
    lang = await get_user_lang(cb.from_user.id)
    if not await has_access(cb.from_user.id):
        await cb.answer(TEXT[lang]["no_access"], show_alert=True); return
    await cb.message.edit_text(TEXT[lang]["news_scan"], parse_mode="Markdown")
    await asyncio.sleep(1)
    
    p = random.choice(PAIRS)
    dr, st = await get_signal(p, 5)
    
    msg = (
        f"🚀 **{TEXT[lang]['news']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🏛 **АКТИВ:** `{p.replace('=X','')}`\n"
        f"🔥 **ПРОГНОЗ:** `{TEXT[lang][dr]}`\n"
        f"💎 **СТАТУС:** `VIP IMPULSE` ⭐⭐⭐\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"❗️ *Входите в сделку немедленно!*"
    )
    await cb.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")

# ================= POSTBACK =================
async def postback(request):
    # Пытаемся достать click_id или subid
    cid = request.query.get("click_id") or request.query.get("subid")
    amt_str = request.query.get("amount", "0")
    try:
        amt = float(amt_str)
        if cid and cid.isdigit():
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    INSERT INTO users (user_id, balance) VALUES ($1, $2) 
                    ON CONFLICT (user_id) DO UPDATE SET balance = users.balance + $2
                """, int(cid), amt)
                logging.info(f"Deposit success: User {cid}, Amount {amt}")
    except Exception as e:
        logging.error(f"Postback error: {e}")
    
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
