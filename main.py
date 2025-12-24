import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
import pandas_ta as ta
import yfinance as yf
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
MIN_DEPOSIT = 20.0

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ================= ТВОИ ПАРЫ И ЭКСПИРАЦИИ =================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 5, 10]

# ================= ГЛАВНЫЙ СЛОВАРЬ ИНТЕРФЕЙСА =================
LEXICON = {
    "ru": {
        "start": "🇷🇺 Выберите язык интерфейса:",
        "instr": "📘 **ИНСТРУКЦИЯ KURUT TRADE**\n\n1️⃣ Нажмите **Регистрация**.\n2️⃣ Создайте аккаунт.\n3️⃣ Пополните баланс от **$20**.\n4️⃣ Доступ откроется автоматически!",
        "reg_btn": "🔗 Регистрация", "check_btn": "✅ Проверить доступ",
        "menu": "🏠 Главное меню", "pairs_btn": "📈 Валютные пары", "news_btn": "📰 Новости рынка",
        "access_ok": "✅ Доступ подтвержден!", "access_no": "❌ Депозит не найден ($20).",
        "sig_title": "СИГНАЛ"
    },
    "en": {
        "start": "🇺🇸 Choose language:", "reg_btn": "🔗 Registration", "check_btn": "✅ Check",
        "menu": "🏠 Main Menu", "pairs_btn": "📈 Pairs", "news_btn": "📰 News", "sig_title": "SIGNAL"
    },
    "tj": {
        "start": "🇹🇯 Забонро интихоб кунед:", "reg_btn": "🔗 Бақайдгирӣ", "check_btn": "✅ Санҷиш",
        "menu": "🏠 Меню", "pairs_btn": "📈 Ҷуфтҳо", "news_btn": "📰 Хабарҳо", "sig_title": "СИГНАЛ"
    },
    "uz": {
        "start": "🇺🇿 Tilni tanlang:", "reg_btn": "🔗 Ro'yxatdan o'tish", "check_btn": "✅ Tekshirish",
        "menu": "🏠 Menyu", "pairs_btn": "📈 Juftliklar", "news_btn": "📰 Yangiliklar", "sig_title": "SIGNAL"
    },
    "kg": {
        "start": "🇰🇬 Тилди тандаңыз:", "reg_btn": "🔗 Каттоо", "check_btn": "✅ Текшерүү",
        "menu": "🏠 Меню", "pairs_btn": "📈 Жуптар", "news_btn": "📰 Жаңылыктар", "sig_title": "СИГНАЛ"
    },
    "kz": {
        "start": "🇰🇿 Тілді таңдаңыз:", "reg_btn": "🔗 Тіркелу", "check_btn": "✅ Тексеру",
        "menu": "🏠 Мәзір", "pairs_btn": "📈 Жұптар", "news_btn": "📰 Жаңалықтар", "sig_title": "СИГНАЛ"
    }
}
# Настройка фолбэка для LEXICON
for lang in LEXICON:
    if lang != "ru":
        for k, v in LEXICON["ru"].items():
            if k not in LEXICON[lang]: LEXICON[lang][k] = v

# ================= ГЛОБАЛЬНЫЙ СЛОВАРЬ ДЛЯ СИГНАЛОВ =================
SIGNAL_LEXICON = {
    "ru": {
        "up": "ВВЕРХ 📈", "down": "ВНИЗ 📉",
        "strong": "СИЛЬНЫЙ 🔥🔥🔥", "medium": "СРЕДНИЙ ⚡⚡", "weak": "СЛАБЫЙ ⚠️",
        "dir": "Направление", "str": "Сила сигнала", "target": "Цель (Уровни)", "pattern_label": "Паттерн",
        "standard": "Стандартный", "bull_eng": "Бычье поглощение 🐂", "bear_eng": "Медвежье поглощение 🐻"
    },
    "en": {
        "up": "UP 📈", "down": "DOWN 📉", "strong": "STRONG 🔥🔥🔥", "medium": "MEDIUM ⚡⚡", "weak": "WEAK ⚠️",
        "dir": "Direction", "str": "Signal Strength", "target": "Target (Levels)", "pattern_label": "Pattern",
        "standard": "Standard", "bull_eng": "Bullish Engulfing 🐂", "bear_eng": "Bearish Engulfing 🐻"
    },
    "tj": {
        "up": "БОЛО 📈", "down": "ПОЁН 📉", "strong": "ҚАВӢ 🔥🔥🔥", "medium": "МИЁНА ⚡⚡", "weak": "ЗАИФ ⚠️",
        "dir": "Самт", "str": "Қувваи сигнал", "target": "Ҳадаф (Сатҳҳо)", "pattern_label": "Паттерн",
        "standard": "Стандартӣ", "bull_eng": "Фурӯбарии говӣ 🐂", "bear_eng": "Фурӯбарии хирсӣ 🐻"
    },
    "uz": {
        "up": "YUQORI 📈", "down": "PAST 📉", "strong": "KUCHLI 🔥🔥🔥", "medium": "O'RTA ⚡⚡", "weak": "KUCHSIZ ⚠️",
        "dir": "Yo'nalish", "str": "Signal kuchi", "target": "Maqsad (Darajalar)", "pattern_label": "Shakl",
        "standard": "Standart", "bull_eng": "Buqa yutilishi 🐂", "bear_eng": "Ayiq yutilishi 🐻"
    },
    "kg": {
        "up": "ЖОГОРУ 📈", "down": "ТӨМӨН 📉", "strong": "КҮЧТҮҮ 🔥🔥🔥", "medium": "ОРТО ⚡⚡", "weak": "АЛСЫЗ ⚠️",
        "dir": "Багыты", "str": "Сигнал күчү", "target": "Максат (Деңгээлдер)", "pattern_label": "Паттерн",
        "standard": "Стандарттык", "bull_eng": "Бука жутуусу 🐂", "bear_eng": "Аюу жутуусу 🐻"
    },
    "kz": {
        "up": "ЖОҒАРЫ 📈", "down": "ТӨМЕН 📉", "strong": "КҮШТІ 🔥🔥🔥", "medium": "ОРАША ⚡⚡", "weak": "ӘЛСІЗ ⚠️",
        "dir": "Бағыты", "str": "Сигнал қуаты", "target": "Мақсат (Деңгейлер)", "pattern_label": "Паттерн",
        "standard": "Стандартты", "bull_eng": "Бұқа жұтылуы 🐂", "bear_eng": "Аю жұтылуы 🐻"
    }
}

# ================= BOT INITIALIZATION =================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.Pool | None = None

async def get_user_lang(uid: int):
    if uid in AUTHORS: return "ru"
    async with DB_POOL.acquire() as conn:
        res = await conn.fetchval("SELECT language FROM users WHERE user_id=$1", uid)
        return res or "ru"

async def has_access(uid: int):
    if uid in AUTHORS: return True
    async with DB_POOL.acquire() as conn:
        res = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", uid)
        return (res or 0) >= MIN_DEPOSIT

# ================= УЛУЧШЕННАЯ ФУНКЦИЯ СИГНАЛА =================
async def get_advanced_signal(pair: str, exp: int, lang: str):
    try:
        s_lang = lang if lang in SIGNAL_LEXICON else "ru"
        sl = SIGNAL_LEXICON[s_lang]
        
        interval = "1m" if exp == 1 else "5m" if exp == 5 else "15m"
        df = yf.download(pair, period="1d", interval=interval, progress=False)
        if len(df) < 20: return "⚠️ Error: No market data"
        
        close = df['Close']
        rsi = ta.rsi(close, length=14).iloc[-1]
        ema21 = ta.ema(close, length=21).iloc[-1]
        sup = df['Low'].rolling(20).min().iloc[-1]
        res = df['High'].rolling(20).max().iloc[-1]
        
        score = 0
        if close.iloc[-1] > ema21: score += 1
        if rsi < 35: score += 2
        if rsi > 65: score -= 2
        
        direction = sl["up"] if score >= 0 else sl["down"]
        strength = sl["strong"] if abs(score) >= 2 else sl["medium"]
        
        pattern = sl["standard"]
        if close.iloc[-1] > df['Open'].iloc[-1] and close.iloc[-2] < df['Open'].iloc[-2]:
            pattern = sl["bull_eng"]
        elif close.iloc[-1] < df['Open'].iloc[-1] and close.iloc[-2] > df['Open'].iloc[-2]:
            pattern = sl["bear_eng"]
            
        text = (
            f"📊 **{LEXICON[s_lang]['sig_title']}**: {pair.replace('=X','')}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"⏰ {s_lang.upper()} | EXP: **{exp} MIN**\n\n"
            f"🚀 **{sl['dir']}**: {direction}\n"
            f"💪 **{sl['str']}**: {strength}\n"
            f"📍 **{sl['target']}**: `{sup:.5f} - {res:.5f}`\n"
            f"🕯 **{sl['pattern_label']}**: {pattern}\n"
            f"📈 **RSI**: `{rsi:.1f}`\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        return text
    except Exception as e:
        return f"⚠️ Analysis Error: {e}"

# ================= HANDLERS =================
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", msg.from_user.id)
    kb = InlineKeyboardBuilder()
    for n, c in [("🇷🇺 RU","ru"),("🇺🇸 EN","en"),("🇹🇯 TJ","tj"),("🇺🇿 UZ","uz"),("🇰🇬 KG","kg"),("🇰🇿 KZ","kz")]:
        kb.button(text=n, callback_data=f"sl:{c}")
    kb.adjust(2)
    await msg.answer(LEXICON["ru"]["start"], reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sl:"))
async def set_lang(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    if cb.from_user.id in AUTHORS: lang = "ru"
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET language=$1 WHERE user_id=$2", lang, cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text=LEXICON[lang]["reg_btn"], url=f"{REF_LINK}&click_id={cb.from_user.id}")
    kb.button(text=LEXICON[lang]["check_btn"], callback_data="check_acc")
    await cb.message.edit_text(LEXICON[lang]["instr"], reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "check_acc")
async def check_acc(cb: types.CallbackQuery):
    l = await get_user_lang(cb.from_user.id)
    if await has_access(cb.from_user.id):
        kb = InlineKeyboardBuilder()
        kb.button(text=LEXICON[l]["pairs_btn"], callback_data="p_list:0")
        kb.button(text=LEXICON[l]["news_btn"], callback_data="m_news")
        kb.adjust(1)
        await cb.message.edit_text(LEXICON[l]["menu"], reply_markup=kb.as_markup())
    else:
        await cb.answer(LEXICON[l]["access_no"], show_alert=True)

@dp.callback_query(F.data.startswith("p_list:"))
async def p_list(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1]); l = await get_user_lang(cb.from_user.id)
    kb = InlineKeyboardBuilder()
    start = page * 8; end = start + 8
    for p in PAIRS[start:end]: kb.button(text=p.replace("=X",""), callback_data=f"sel:{p}")
    kb.adjust(2)
    if page > 0: kb.row(types.InlineKeyboardButton(text="⬅️", callback_data=f"p_list:{page-1}"))
    if end < len(PAIRS): kb.row(types.InlineKeyboardButton(text="➡️", callback_data=f"p_list:{page+1}"))
    kb.row(types.InlineKeyboardButton(text=LEXICON[l]["menu"], callback_data="check_acc"))
    await cb.message.edit_text(LEXICON[l]["pairs_btn"], reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sel:"))
async def sel_exp(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS: kb.button(text=f"{e} MIN", callback_data=f"sg:{pair}:{e}")
    kb.adjust(1)
    await cb.message.edit_text(f"💎 Asset: {pair.replace('=X','')}", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sg:"))
async def get_sig(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":"); l = await get_user_lang(cb.from_user.id)
    await cb.answer("⚡ Analysis...")
    res = await get_advanced_signal(pair, int(exp), l)
    kb = InlineKeyboardBuilder().button(text="⬅️ Back", callback_data="p_list:0")
    await cb.message.edit_text(res, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "m_news")
async def m_news(cb: types.CallbackQuery):
    import random
    l = await get_user_lang(cb.from_user.id)
    pair = random.choice(PAIRS); res = await get_advanced_signal(pair, 5, l)
    kb = InlineKeyboardBuilder().button(text="⬅️ Back", callback_data="check_acc")
    await cb.message.edit_text(f"🔥 **VIP NEWS**\n\n{res}", reply_markup=kb.as_markup(), parse_mode="Markdown")

# ================= SERVER =================
async def postback(request:web.Request):
    uid = request.query.get("click_id"); amt = request.query.get("amount", "0")
    if uid and uid.isdigit():
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", float(amt), int(uid))
    return web.Response(text="OK")

async def main():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, balance FLOAT DEFAULT 0, language TEXT DEFAULT 'ru')")
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
