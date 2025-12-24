import os
import sys
import asyncio
import logging
import asyncpg
import pandas as pd
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

EXPIRATIONS = [1,5,15]
PAIRS_PER_PAGE = 6

INTERVAL_MAP = {1:"1m",5:"5m",15:"15m"}

# ================= MESSAGES =================
MESSAGES = {
    "start": {
        "ru":"📘 ИНСТРУКЦИЯ KURUT TRADE\nБот анализирует рынок\nИспользует профессиональные индикаторы\nПодходит для новичков и профи",
        "en":"📘 KURUT TRADE INSTRUCTION\nBot analyzes the market\nUses professional indicators\nSuitable for beginners and pros",
        "tj":"📘 ДАСТУРИ KURUT TRADE\nБот бозорро таҳлил мекунад\nИндикаторҳои касбиро истифода мебарад\nБарои навомӯзон ва мутахассисон мувофиқ",
        "uz":"📘 KURUT TRADE KO‘RSATMALARI\nBot bozorni tahlil qiladi\nProfessional indikatorlardan foydalanadi\nYangi boshlovchi va professionallar uchun",
        "kg":"📘 KURUT TRADE KO‘RSÖTMÖLÖR\nБазарды талдайт\nКесиптик индикаторлорду колдонуп, жаңы баштагандар жана профессионалдар үчүн",
        "kz":"📘 KURUT TRADE НҰСҚАМАЛАРЫ\nБот нарықты талдайды\nКәсіби индикаторларды қолданады\nБастаушылар мен кәсіби үшін"
    },
    "main_menu":{
        "ru":"Главное меню", "en":"Main menu", "tj":"Менюи асосӣ",
        "uz":"Asosiy menyu","kg":"Башкы меню","kz":"Басты меню"
    },
    "choose_language":{
        "ru":"Выберите язык:", "en":"Choose language:", "tj":"Забонро интихоб кунед:",
        "uz":"Tilni tanlang:","kg":"Тилди тандаңыз:","kz":"Тілді таңдаңыз:"
    }
}

SIGNAL_TEXT = {
    "direction":{
        "up":{"ru":"ВВЕРХ 📈","en":"UP 📈","tj":"БОЛО 📈","uz":"YUQORI 📈","kg":"ЖОГОРУ 📈","kz":"ЖОҒАРЫ 📈"},
        "down":{"ru":"ВНИЗ 📉","en":"DOWN 📉","tj":"ПОЁН 📉","uz":"PAST 📉","kg":"ТӨМӨН 📉","kz":"ТӨМЕН 📉"}
    },
    "strength":{
        "strong":{"ru":"🔥 СИЛЬНЫЙ сигнал","en":"🔥 STRONG signal","tj":"🔥 СИГНАЛИ ҚАВИ","uz":"🔥 KUCHLI signal","kg":"🔥 КҮЧТҮ сигнал","kz":"🔥 КҮШТІ сигнал"},
        "medium":{"ru":"⚡ СРЕДНИЙ сигнал","en":"⚡ MEDIUM signal","tj":"⚡ МУТАДИЛ СИГНАЛ","uz":"⚡ O‘RTA signal","kg":"⚡ Орточо сигнал","kz":"⚡ Орта сигнал"},
        "weak":{"ru":"⚠️ СЛАБЫЙ рынок (риск)","en":"⚠️ WEAK market (risk)","tj":"⚠️ БОЗОР КУВАТНОК НЕСТ (Хавф)","uz":"⚠️ Zaif bozor (xavf)","kg":"⚠️ АЗЫРКЫ рынок (кооптуу)","kz":"⚠️ Әлсіз нарық (тәуекел)"}
    },
    "weak_note":{
        "ru":"⚠️ Рынок слабый, риск повышен","en":"⚠️ Market is weak, high risk","tj":"⚠️ Бозор заиф, хавф баланд",
        "uz":"⚠️ Bozor zaif, xavf yuqori","kg":"⚠️ Рынок азык, коркунуч жогору","kz":"⚠️ Нарық әлсіз, тәуекел жоғары"
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
        await conn.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING",user_id
        )

async def get_user(user_id:int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1",user_id)

async def update_balance(user_id:int,amount:float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2",amount,user_id)

async def set_language(user_id:int,lang:str):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET language=$1 WHERE user_id=$2",lang,user_id)

async def get_language(user_id:int):
    if user_id in AUTHORS:
        return "ru"
    user = await get_user(user_id)
    return user["language"] if user else "ru"

async def has_access(user_id:int)->bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"]>=MIN_DEPOSIT)

# ================= SIGNAL CORE =================
def last(v): return float(v.iloc[-1])

async def get_signal(pair:str,exp:int):
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)
        if df.empty or len(df)<50:
            return "DOWN","weak"
        close = df["Close"]
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100-(100/(1+gain/loss))
        buy = sell = 0
        if last(ema20)>last(ema50): buy+=2
        else: sell+=2
        if last(rsi)>55: buy+=2
        elif last(rsi)<45: sell+=2
        direction = "UP" if buy>sell else "DOWN"
        strength = abs(buy-sell)
        if strength>=3: level="strong"
        elif strength==2: level="medium"
        else: level="weak"
        return direction,level
    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "DOWN","weak"

# ================= KEYBOARDS =================
def main_menu_kb(lang:str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 "+MESSAGES["main_menu"][lang], callback_data="pairs")
    kb.button(text="📰 Новости", callback_data="news")
    kb.button(text="🌐 Сменить язык", callback_data="change_lang")
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
        kb.button(text=p.replace("=X",""),callback_data=f"pair:{p}")
    if page>0: kb.button(text="⬅️ Назад",callback_data=f"page:{page-1}")
    if start+PAIRS_PER_PAGE<len(PAIRS): kb.button(text="➡️ Вперёд",callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин",callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg:types.Message):
    await upsert_user(msg.from_user.id)
    lang = await get_language(msg.from_user.id)
    await msg.answer(MESSAGES["start"][lang],reply_markup=main_menu_kb(lang))

# ---------- Меню и пары ----------
@dp.callback_query(lambda c:c.data=="main_menu")
async def main_menu_cb(cb:types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    await cb.message.edit_text(MESSAGES["main_menu"][lang],reply_markup=main_menu_kb(lang))

@dp.callback_query(lambda c:c.data=="pairs")
async def pairs_cb(cb:types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        await cb.answer("Нет доступа",show_alert=True)
        return
    await cb.message.edit_text("Выберите пару",reply_markup=pairs_kb())

@dp.callback_query(lambda c:c.data.startswith("page:"))
async def page_cb(cb:types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите пару",reply_markup=pairs_kb(page))

@dp.callback_query(lambda c:c.data.startswith("pair:"))
async def pair_cb(cb:types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию",reply_markup=exp_kb(pair))

# ---------- Сигналы ----------
@dp.callback_query(lambda c:c.data.startswith("exp:"))
async def exp_cb(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    direction, level = await get_signal(pair,int(exp))
    lang = await get_language(cb.from_user.id)
    dir_text = SIGNAL_TEXT["direction"]["up"][lang] if direction=="UP" else SIGNAL_TEXT["direction"]["down"][lang]
    level_text = SIGNAL_TEXT["strength"][level][lang]
    note = SIGNAL_TEXT["weak_note"][lang] if level=="weak" else ""
    text = f"📊 KURUT TRADE SIGNAL\n\n💱 Пара: {pair.replace('=X','')}\n⏱ Экспирация: {exp} мин\n📈 Направление: {dir_text}\n{level_text}{note}"
    await cb.message.edit_text(text,reply_markup=back_menu_kb(lang))

# ---------- Новости ----------
@dp.callback_query(lambda c:c.data=="news")
async def news_cb(cb: types.CallbackQuery):
    import random
    pair=random.choice(PAIRS)
    exp=random.choice(EXPIRATIONS)
    direction,level = await get_signal(pair,exp)
    lang = await get_language(cb.from_user.id)
    dir_text = SIGNAL_TEXT["direction"]["up"][lang] if direction=="UP" else SIGNAL_TEXT["direction"]["down"][lang]
    level_text = SIGNAL_TEXT["strength"][level][lang]
    note = SIGNAL_TEXT["weak_note"][lang] if level=="weak" else ""
    text = f"📰 KURUT TRADE NEWS\n\n💱 Пара: {pair.replace('=X','')} — {exp} мин\n📈 Направление: {dir_text}\n{level_text}{note}"
    await cb.message.edit_text(text,reply_markup=back_menu_kb(lang))

# ---------- Смена языка ----------
@dp.callback_query(lambda c:c.data=="change_lang")
async def change_lang_cb(cb: types.CallbackQuery):
    kb=InlineKeyboardBuilder()
    langs=[("Русский","ru"),("English","en"),("Тоҷикӣ","tj"),("O'zbek","uz"),("Кыргызча","kg"),("Қазақша","kz")]
    for name,code in langs: kb.button(text=name,callback_data=f"set_lang:{code}")
    kb.adjust(2)
    await cb.message.edit_text(MESSAGES["choose_language"]["ru"],reply_markup=kb.as_markup())

@dp.callback_query(lambda c:c.data.startswith("set_lang:"))
async def set_lang_cb(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    if cb.from_user.id in AUTHORS:
        await cb.answer("Авторы всегда используют русский язык",show_alert=True)
        return
    await set_language(cb.from_user.id,lang)
    await cb.answer("Язык успешно изменён")
    await cb.message.edit_text(MESSAGES["main_menu"][lang],reply_markup=main_menu_kb(lang))

# ================= POSTBACK =================
async def postback(request:web.Request):
    click_id=request.query.get("click_id","").strip()
    amount=request.query.get("amount","0")
    if not click_id.isdigit(): return web.Response(text="NO CLICK_ID")
    await upsert_user(int(click_id))
    await update_balance(int(click_id),float(amount))
    logging.info(f"POSTBACK: user {click_id} amount {amount}")
    return web.Response(text="OK")

# ================= START SERVER =================
async def main():
    await init_db()
    await bot(DeleteWebhook(drop_pending_updates=True))
    await bot(SetWebhook(url=WEBHOOK_URL))

    app=web.Application()
    SimpleRequestHandler(dp,bot).register(app,WEBHOOK_PATH)
    app.router.add_get("/postback",postback)

    runner=web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())
