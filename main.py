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
EXPIRATIONS = [1,5,10]
PAIRS_PER_PAGE = 6
INTERVAL_MAP = {1:"1m",5:"5m",10:"15m"}

# ================= MESSAGES =================
MESSAGES = {
    "start": {
        "ru":"📘 ИНСТРУКЦИЯ KURUT TRADE\nБот анализирует рынок\nИспользует профессиональные индикаторы\nПодходит для новичков и профи",
        "en":"📘 KURUT TRADE INSTRUCTION\nBot analyzes the market\nUses professional indicators\nSuitable for beginners and pros",
        "tj":"📘 KURUT TRADE ДАСТУР\nБот бозорро таҳлил мекунад\nИндикаторҳои касбӣ истифода мекунад\nБарои навкорон ва мутахассисон мувофиқ",
        "uz":"📘 KURUT TRADE KO'RSATMA\nBot bozorni tahlil qiladi\nProfessional indikatorlardan foydalanadi\nYangi boshlovchilar va mutaxassislar uchun",
        "kg":"📘 KURUT TRADE ИНСТРУКЦИЯ\nБот базарды талдайт\nКесипкөй индикаторлорду колдонуу\nЖаңылар жана адистер үчүн ылайыктуу",
        "kz":"📘 KURUT TRADE НҰСҚАУЛЫҚ\nБот нарықты талдайды\nКәсіби индикаторларды пайдаланады\nЖаңадан бастағандар мен мамандарға қолайлы"
    },
    "main_menu": {
        "ru":"Главное меню:",
        "en":"Main menu:",
        "tj":"Менюи асосӣ:",
        "uz":"Asosiy menyu:",
        "kg":"Башкы меню:",
        "kz":"Басты меню:"
    },
    "pairs": {
        "ru":"Выберите валютную пару:",
        "en":"Select currency pair:",
        "tj":"Ҷуфти асъорро интихоб кунед:",
        "uz":"Valyuta juftligini tanlang:",
        "kg":"Валюталык жупту тандаңыз:",
        "kz":"Валюта жұбын таңдаңыз:"
    },
    "news": {
        "ru":"НОВОСТНОЙ СИГНАЛ",
        "en":"NEWS SIGNAL",
        "tj":"СИГНАЛИ ХАБАР",
        "uz":"YANGILIK SIGNALI",
        "kg":"ЖАУП БЕРҮҮ СИГНАЛЫ",
        "kz":"ЖАҢАЛЫҚ СИГНАЛЫ"
    },
    "choose_language": {
        "ru":"Выберите язык:",
        "en":"Choose language:",
        "tj":"Забониро интихоб кунед:",
        "uz":"Tilni tanlang:",
        "kg":"Тилди тандаңыз:",
        "kz":"Тілді таңдаңыз:"
    }
}

# ================= DATABASE =================
async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(DATABASE_URL)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance FLOAT DEFAULT 0,
            language TEXT DEFAULT 'ru'
        );
        """)

async def upsert_user(user_id:int):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)

async def get_user(user_id:int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def update_balance(user_id:int, amount:float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, user_id)

async def set_language(user_id:int, lang:str):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET language=$1 WHERE user_id=$2", lang, user_id)

async def get_language(user_id:int) -> str:
    if user_id in AUTHORS:
        return "ru"
    user = await get_user(user_id)
    return user["language"] if user else "ru"

async def has_access(user_id:int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"]>=MIN_DEPOSIT)

# ================= SIGNAL CORE (10 indicators + свечные паттерны) =================
def last(v): return float(v.iloc[-1])

async def get_signal(pair:str, exp:int) -> tuple[str,str]:
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="5d", interval=interval, progress=False)
        if df.empty or len(df)<50: return "ВНИЗ 📉","Слабый рынок"

        close = df["Close"]
        high = df["High"]
        low = df["Low"]

        # EMA, SMA
        ema10 = close.ewm(span=10).mean()
        ema50 = close.ewm(span=50).mean()
        sma20 = close.rolling(20).mean()

        # RSI
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100-(100/(1+gain/loss))

        # MACD
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12-ema26
        signal = macd.ewm(span=9).mean()

        # Bollinger Bands
        sma20_bb = close.rolling(20).mean()
        std = close.rolling(20).std()
        upper = sma20_bb+2*std
        lower = sma20_bb-2*std

        # ATR
        tr = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()

        # ADX
        plus_dm = high.diff()
        minus_dm = low.diff()
        plus_dm[plus_dm<0]=0
        minus_dm[minus_dm<0]=0
        tr_adx = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
        plus_di = 100*(plus_dm.rolling(14).sum()/tr_adx.rolling(14).sum())
        minus_di = 100*(minus_dm.rolling(14).sum()/tr_adx.rolling(14).sum())
        adx = abs(plus_di-minus_di)

        # Считаем "силу" покупки и продажи
        buy=sell=0
        if last(close)>last(ema50): buy+=2
        else: sell+=2
        if last(rsi)>55: buy+=2
        elif last(rsi)<45: sell+=2
        if last(macd)>last(signal): buy+=2
        else: sell+=2
        if last(close)>last(upper): buy+=1
        elif last(close)<last(lower): sell+=1
        if last(plus_di)>last(minus_di): buy+=1
        else: sell+=1

        # Определяем направление и силу
        direction = "ВВЕРХ 📈" if buy>sell else "ВНИЗ 📉"
        strength_diff = abs(buy-sell)
        if strength_diff>=6: level="🔥 СИЛЬНЫЙ сигнал"
        elif strength_diff>=3: level="⚡ СРЕДНИЙ сигнал"
        else: level="⚠️ СЛАБЫЙ рынок (риск)"

        return direction, level

    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "ВНИЗ 📉","⚠️ Ошибка данных"

# ================= KEYBOARDS =================
def main_menu_kb(lang:str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 "+MESSAGES["pairs"][lang], callback_data="pairs")
    kb.button(text="📰 "+MESSAGES["news"][lang], callback_data="news")
    kb.button(text="🌐 "+MESSAGES["choose_language"][lang], callback_data="change_lang")
    kb.adjust(1)
    return kb.as_markup()

def back_menu_kb(lang:str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ "+MESSAGES["main_menu"][lang], callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page*PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page>0: kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start+PAIRS_PER_PAGE<len(PAIRS): kb.button(text="➡️ Вперёд", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair:str):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

# ================= CALLBACK HANDLERS =================
# ================= CALLBACK HANDLERS =================
@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    await cb.message.edit_text(MESSAGES["main_menu"][lang], reply_markup=main_menu_kb(lang))

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    lang = await get_language(cb.from_user.id)
    await cb.message.edit_text(MESSAGES["pairs"][lang], reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text(MESSAGES["pairs"][await get_language(cb.from_user.id)], reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию:", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level = await get_signal(pair,int(exp))
    lang = await get_language(cb.from_user.id)
    text = f"📊 СИГНАЛ KURUT TRADE\n\nПара: {pair.replace('=X','')}\nЭкспирация: {exp} мин\nНаправление: {direction}\nКачество: {level}"
    await cb.message.edit_text(text, reply_markup=back_menu_kb(lang))

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    pair = random.choice(PAIRS)
    exp = random.choice(EXPIRATIONS)
    direction, level = await get_signal(pair,exp)
    lang = await get_language(cb.from_user.id)
    text = f"📰 {MESSAGES['news'][lang]}\n\n{pair.replace('=X','')} — {exp} мин\n{direction}\n{level}"
    await cb.message.edit_text(text, reply_markup=back_menu_kb(lang))

@dp.callback_query(lambda c: c.data=="change_lang")
async def change_lang(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    langs = [("Русский","ru"),("English","en"),("Тоҷикӣ","tj"),("O'zbek","uz"),("Кыргызча","kg"),("Қазақша","kz")]
    for name, code in langs:
        kb.button(text=name, callback_data=f"set_lang:{code}")
    kb.adjust(2)
    await cb.message.edit_text(MESSAGES["choose_language"]["ru"], reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data.startswith("set_lang:"))
async def set_lang(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    if cb.from_user.id in AUTHORS:
        await cb.answer("Авторы всегда используют русский язык", show_alert=True)
        return
    await set_language(cb.from_user.id, lang)
    await cb.answer("Язык успешно изменён")
    await cb.message.edit_text(MESSAGES["main_menu"][lang], reply_markup=main_menu_kb(lang))

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text("Доступ к боту:", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    lang = await get_language(cb.from_user.id)

    if cb.from_user.id in AUTHORS:
        await cb.message.edit_text("👑 Авторский доступ открыт", reply_markup=main_menu_kb(lang))
        return

    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu_kb(lang))
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить пополнение", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text("⏳ Ожидаем пополнение от 20$", reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_balance")
async def check_balance(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    lang = await get_language(cb.from_user.id)
    if cb.from_user.id in AUTHORS or (user and user["balance"]>=MIN_DEPOSIT):
        await cb.message.edit_text("✅ Доступ открыт", reply_markup=main_menu_kb(lang))
    else:
        await cb.answer("❌ Баланс меньше 20$", show_alert=True)

# ================= POSTBACK =================
async def postback(request:web.Request):
    click_id = request.query.get("click_id","").strip()
    amount = request.query.get("amount","0")
    if not click_id.isdigit(): return web.Response(text="NO CLICK_ID")
    await upsert_user(int(click_id))
    await update_balance(int(click_id), float(amount))
    logging.info(f"POSTBACK: user {click_id} amount {amount}")
    return web.Response(text="OK")

# ================= START SERVER =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))
    app = web.Application()
    SimpleRequestHandler(dp, bot).register(app, WEBHOOK_PATH)
    app.router.add_get("/postback", postback)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()
    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())
