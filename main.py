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
EXPIRATIONS = [1,5,10]
PAIRS_PER_PAGE = 6
INTERVAL_MAP = {1:"1m",5:"5m",10:"15m"}

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
    user = await get_user(user_id)
    return user["language"] if user and "language" in user else "ru"

async def has_access(user_id:int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= MESSAGES =================
MESSAGES = {
    "start": {
        "ru":"📘 ИНСТРУКЦИЯ KURUT TRADE\n\nБот анализирует рынок\nИспользует профессиональные индикаторы\nПодходит для новичков и профи",
        "en":"📘 KURUT TRADE INSTRUCTION\n\nBot analyzes the market\nUses professional indicators\nSuitable for beginners and pros",
        "tj":"📘 ДАСТУРИ KURUT TRADE\n\nБот бозорро таҳлил мекунад\nИндикаторҳои касбӣ истифода мебарад\nБарои навҷавон ва профессионалҳо мувофиқ аст",
        "uz":"📘 KURUT TRADE KO‘RSATMALARI\n\nBot bozorni tahlil qiladi\nProfessional indikatorlardan foydalanadi\nBoshlovchi va mutaxassislar uchun mos",
        "kg":"📘 KURUT TRADE НУСХАСЫ\n\nБот базарды талдайт\nКесиптик индикаторлорду колдонуу\nЖаңы баштагандар жана профессионалдар үчүн ылайыктуу",
        "kz":"📘 KURUT TRADE НҰСҚАУЛЫҒЫ\n\nБот нарықты талдайды\nКәсіби индикаторларды қолданады\nБастаушылар мен мамандарға арналған"
    },
    "main_menu": {
        "ru":"Главное меню:",
        "en":"Main menu:",
        "tj":"Менюи асосӣ:",
        "uz":"Asosiy menyu:",
        "kg":"Башкы меню:",
        "kz":"Негізгі меню:"
    },
    "choose_language": {
        "ru":"Выберите язык:",
        "en":"Choose language:",
        "tj":"Забонро интихоб кунед:",
        "uz":"Tilni tanlang:",
        "kg":"Тилди тандаңыз:",
        "kz":"Тілді таңдаңыз:"
    },
    "registration": {
        "ru":"🔗 Для доступа зарегистрируйтесь по ссылке и пополните баланс от 20$",
        "en":"🔗 To get access, register via link and deposit at least $20",
        "tj":"🔗 Барои дастрасӣ, тавассути пайванд сабти ном кунед ва камаш $20 гузаронед",
        "uz":"🔗 Kirish uchun havola orqali ro‘yxatdan o‘ting va kamida $20 to‘lang",
        "kg":"🔗 Кирүү үчүн шилтеме аркылуу катталып, кеминде $20 толтуруңуз",
        "kz":"🔗 Қол жеткізу үшін сілтеме арқылы тіркеліп, кемінде $20 салыңыз"
    },
    "balance_wait": {
        "ru":"⏳ Ожидаем пополнение от 20$",
        "en":"⏳ Waiting for deposit of at least $20",
        "tj":"⏳ Мунтазири гузаронидани $20",
        "uz":"⏳ Kamida $20 to‘lovini kutamiz",
        "kg":"⏳ Кеминде $20 толтурууну күтөбүз",
        "kz":"⏳ Кемінде $20 депозит күтеміз"
    },
    "balance_ok": {
        "ru":"✅ Доступ открыт",
        "en":"✅ Access granted",
        "tj":"✅ Дастрасӣ кушода шуд",
        "uz":"✅ Kirish ochildi",
        "kg":"✅ Кирүү ачык",
        "kz":"✅ Қол жеткізу ашық"
    }
}

# ================= SIGNALS =================
SIGNAL_TEXT = {
    "direction": {
        "ru": {"UP":"ВВЕРХ 📈", "DOWN":"ВНИЗ 📉"},
        "en": {"UP":"UP 📈", "DOWN":"DOWN 📉"},
        "tj": {"UP":"БОЛО 📈", "DOWN":"ПОЁН 📉"},
        "uz": {"UP":"YUQORI 📈", "DOWN":"PAST 📉"},
        "kg": {"UP":"ЖОГОРУ 📈", "DOWN":"ТӨМӨН 📉"},
        "kz": {"UP":"ЖОҒАРЫ 📈", "DOWN":"ТӨМЕН 📉"}
    },
    "strength": {
        "ru": { "STRONG":"🔥 СИЛЬНЫЙ", "MEDIUM":"⚡ СРЕДНИЙ", "WEAK":"⚠️ СЛАБЫЙ РЫНОК"},
        "en": { "STRONG":"🔥 STRONG", "MEDIUM":"⚡ MEDIUM", "WEAK":"⚠️ WEAK MARKET"},
        "tj": { "STRONG":"🔥 ҚУВВАТЛИ", "MEDIUM":"⚡ ЎРТА", "WEAK":"⚠️ ЗАИФ БОЗОР"},
        "uz": { "STRONG":"🔥 KUCHLI", "MEDIUM":"⚡ O‘RTA", "WEAK":"⚠️ ZAIF BOZOR"},
        "kg": { "STRONG":"🔥 КҮЧҮҮ", "MEDIUM":"⚡ ORТО", "WEAK":"⚠️ АЛСЫЗ БАЗАР"},
        "kz": { "STRONG":"🔥 КҮШТІ", "MEDIUM":"⚡ ОРТАША", "WEAK":"⚠️ Әлсіз НАРЫҚ"}
    }
}

def last(v): return float(v.iloc[-1])

async def get_signal(pair:str, exp:int) -> tuple[str,str]:
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)
        if df.empty or len(df)<50:
            return "DOWN","WEAK"
        close = df["Close"]
        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100/(1+gain/loss))
        buy=sell=0
        if last(ema20)>last(ema50): buy+=2
        else: sell+=2
        if last(rsi)>55: buy+=2
        elif last(rsi)<45: sell+=2
        direction="UP" if buy>sell else "DOWN"
        strength_code = "STRONG" if abs(buy-sell)>=3 else "MEDIUM" if abs(buy-sell)==2 else "WEAK"
        return direction,strength_code
    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "DOWN","WEAK"

# ================= KEYBOARDS =================
def main_menu_kb(lang:str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
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
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page>0: kb.button(text="⬅️ Back", callback_data=f"page:{page-1}")
    if start+PAIRS_PER_PAGE<len(PAIRS): kb.button(text="➡️ Next", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS: kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

def access_kb(lang:str):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    return kb.as_markup()

# ================= HANDLERS =================
@dp.message(Command("start"))
async def start(msg:types.Message):
    await upsert_user(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer("👑 Авторский доступ", reply_markup=main_menu_kb("ru"))
        return
    lang = await get_language(msg.from_user.id)
    await msg.answer(MESSAGES["start"][lang], reply_markup=access_kb(lang))

# ========= обработка сигналов и новостей =========
@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb:types.CallbackQuery):
    _,pair,exp_val=cb.data.split(":")
    direction_code,strength_code=await get_signal(pair,int(exp_val))
    lang = await get_language(cb.from_user.id)
    direction_text = SIGNAL_TEXT["direction"][lang][direction_code]
    strength_text = SIGNAL_TEXT["strength"][lang][strength_code]
    text=f"📊 СИГНАЛ KURUT TRADE\n\nПара: {pair.replace('=X','')}\nЭкспирация: {exp_val} мин\nНаправление: {direction_text}\nКачество: {strength_text}"
    await cb.message.edit_text(text, reply_markup=back_menu_kb(lang))

@dp.callback_query(lambda c: c.data=="news")
async def news(cb:types.CallbackQuery):
    import random
    pair=random.choice(PAIRS)
    exp_val=random.choice(EXPIRATIONS)
    direction_code,strength_code=await get_signal(pair,exp_val)
    lang=await get_language(cb.from_user.id)
    direction_text = SIGNAL_TEXT["direction"][lang][direction_code]
    strength_text = SIGNAL_TEXT["strength"][lang][strength_code]
    text=f"📰 НОВОСТНОЙ СИГНАЛ\n\n{pair.replace('=X','')} — {exp_val} мин\n{direction_text}\n{strength_text}"
    await cb.message.edit_text(text, reply_markup=back_menu_kb(lang))

# ================= POSTBACK =================
async def postback(request:web.Request):
    click_id=request.query.get("click_id","").strip()
    amount=request.query.get("amount","0")
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
    app=web.Application()
    SimpleRequestHandler(dp,bot).register(app,WEBHOOK_PATH)
    app.router.add_get("/postback", postback)
    runner=web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()
    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__=="__main__":
    asyncio.run(main())
