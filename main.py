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

EXPIRATIONS = [1, 5, 10]
PAIRS_PER_PAGE = 6

INTERVAL_MAP = {
    1: "1m",
    5: "5m",
    10: "15m"
}

LANGUAGES = {
    "ru": "Русский",
    "en": "English",
    "tj": "Тоҷикӣ",
    "uz": "O'zbek",
    "kg": "Кыргызча",
    "kz": "Қазақша"
}

MESSAGES = {
    "start": {
        "ru": "📘 ИНСТРУКЦИЯ KURUT TRADE\n\nБот анализирует рынок\nИспользует профессиональные индикаторы\nПодходит для новичков и профи",
        "en": "📘 KURUT TRADE INSTRUCTION\n\nBot analyzes the market\nUses professional indicators\nSuitable for beginners and pros",
        "tj": "📘 ДАСТУРИ KURUT TRADE\n\nБот бозорро таҳлил мекунад\nИндикаторҳои касбиро истифода мебарад\nБарои наврасон ва мутахассисон",
        "uz": "📘 KURUT TRADE KO'RSATMALARI\n\nBot bozorni tahlil qiladi\nProfessional indikatorlardan foydalanadi\nYangi boshlovchilar va mutaxassislar uchun",
        "kg": "📘 KURUT TRADE КООРДИНАТОР\n\nБот базарды талдайт\nКесиптик индикаторлорду колдонуп,\nЖаңы баштагандар жана адистер үчүн",
        "kz": "📘 KURUT TRADE НҰСҚАУЛЫҚ\n\nБот нарықты талдайды\nКәсіби индикаторларды қолданады\nЖаңадан бастаушылар мен мамандарға"
    },
    "author_access": {
        "ru": "👑 Авторский доступ",
        "en": "👑 Author access",
        "tj": "👑 Дастрасии муаллиф",
        "uz": "👑 Muallif kirish",
        "kg": "👑 Автордук кирүү",
        "kz": "👑 Авторлық қолжетімділік"
    },
    "get_access": {
        "ru": "Доступ к боту:",
        "en": "Bot access:",
        "tj": "Дастрасӣ ба бот:",
        "uz": "Botga kirish:",
        "kg": "Ботко кирүү:",
        "kz": "Ботқа қолжетімділік:"
    },
    "check_balance": {
        "ru": "⏳ Ожидаем пополнение от 20$",
        "en": "⏳ Waiting for deposit of 20$",
        "tj": "⏳ Муттозири пардохт аз 20$",
        "uz": "⏳ 20$ depozitni kutmoqda",
        "kg": "⏳ 20$ толомоону күтүп жатабыз",
        "kz": "⏳ 20$ депозитін күтеміз"
    },
    "access_open": {
        "ru": "✅ Доступ открыт",
        "en": "✅ Access granted",
        "tj": "✅ Дастрасӣ кушода шуд",
        "uz": "✅ Kirish ochildi",
        "kg": "✅ Кирүү ачылды",
        "kz": "✅ Қолжетімділік ашылды"
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

async def upsert_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def get_user(user_id: int):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET balance=$1 WHERE user_id=$2", amount, user_id)

async def set_language(user_id: int, lang: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET language=$1 WHERE user_id=$2", lang, user_id)

async def get_language(user_id: int) -> str:
    user = await get_user(user_id)
    return user["language"] if user else "ru"

async def has_access(user_id: int) -> bool:
    if user_id in AUTHORS:
        return True
    user = await get_user(user_id)
    return bool(user and user["balance"] >= MIN_DEPOSIT)

# ================= SIGNAL CORE =================

def last(v):
    return float(v.iloc[-1])

async def get_signal(pair: str, exp: int, lang: str) -> tuple[str, str]:
    try:
        interval = INTERVAL_MAP[exp]
        df = yf.download(pair, period="2d", interval=interval, progress=False)

        if df.empty or len(df) < 50:
            messages = {
                "ru": "Слабый рынок", "en": "Weak market", "tj": "Бозори суст",
                "uz": "Bozor zaif", "kg": "Сырт базар", "kz": "Нашар нарық"
            }
            return "ВНИЗ 📉", messages.get(lang, "Слабый рынок")

        close = df["Close"]

        ema20 = close.ewm(span=20).mean()
        ema50 = close.ewm(span=50).mean()

        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))

        buy = 0
        sell = 0

        if last(ema20) > last(ema50):
            buy += 2
        else:
            sell += 2

        if last(rsi) > 55:
            buy += 2
        elif last(rsi) < 45:
            sell += 2

        if buy > sell:
            direction = "ВВЕРХ 📈" if lang=="ru" else "UP 📈"
        else:
            direction = "ВНИЗ 📉" if lang=="ru" else "DOWN 📉"

        strength_map = {
            3: {"ru":"🔥 СИЛЬНЫЙ сигнал","en":"🔥 STRONG signal"},
            2: {"ru":"⚡ СРЕДНИЙ сигнал","en":"⚡ MEDIUM signal"},
            1: {"ru":"⚠️ СЛАБЫЙ рынок (риск)","en":"⚠️ WEAK market (risk)"},
            0: {"ru":"⚠️ СЛАБЫЙ рынок (риск)","en":"⚠️ WEAK market (risk)"}
        }
        level = strength_map.get(abs(buy - sell), strength_map[0]).get(lang, "⚠️ СЛАБЫЙ рынок (риск)")

        return direction, level

    except Exception as e:
        logging.error(f"get_signal error: {e}")
        return "ВНИЗ 📉", "⚠️ Ошибка данных"

# ================= KEYBOARDS =================

def main_menu(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары" if lang=="ru" else "📈 Pairs", callback_data="pairs")
    kb.button(text="📰 Новости" if lang=="ru" else "📰 News", callback_data="news")
    kb.button(text="🌐 Сменить язык" if lang=="ru" else "🌐 Change Language", callback_data="change_lang")
    kb.adjust(1)
    return kb.as_markup()

def back_menu_kb(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Главное меню" if lang=="ru" else "⬅️ Main Menu", callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def exp_kb(pair):
    kb = InlineKeyboardBuilder()
    for e in EXPIRATIONS:
        kb.button(text=f"{e} мин", callback_data=f"exp:{pair}:{e}")
    kb.adjust(2)
    return kb.as_markup()

def language_kb():
    kb = InlineKeyboardBuilder()
    for code, name in LANGUAGES.items():
        kb.button(text=name, callback_data=f"set_lang:{code}")
    kb.adjust(2)
    return kb.as_markup()

# ================= HANDLERS =================

@dp.message(Command("start"))
async def start(msg: types.Message):
    await upsert_user(msg.from_user.id)
    lang = await get_language(msg.from_user.id)
    if msg.from_user.id in AUTHORS:
        await msg.answer(MESSAGES["author_access"][lang], reply_markup=main_menu(lang))
        return
    await msg.answer(MESSAGES["start"][lang], reply_markup=InlineKeyboardBuilder().button(text="➡️ Далее", callback_data="instr2").as_markup())

@dp.callback_query(lambda c: c.data=="instr2")
async def instr2(cb: types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Получить доступ", callback_data="get_access")
    await cb.message.edit_text(
        f"Как получить доступ:\n1️⃣ Регистрация по ссылке\n2️⃣ Пополнение от {MIN_DEPOSIT}$\n3️⃣ Проверка ID",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data=="get_access")
async def get_access(cb: types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Регистрация", url=REF_LINK)
    kb.button(text="✅ Проверить ID", callback_data="check_id")
    kb.adjust(1)
    await cb.message.edit_text(MESSAGES["get_access"][lang], reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_id")
async def check_id(cb: types.CallbackQuery):
    await upsert_user(cb.from_user.id)
    user = await get_user(cb.from_user.id)
    lang = await get_language(cb.from_user.id)

    if cb.from_user.id in AUTHORS:
        await cb.message.edit_text(MESSAGES["author_access"][lang], reply_markup=main_menu(lang))
        return

    if user and user["balance"] >= MIN_DEPOSIT:
        await cb.message.edit_text(MESSAGES["access_open"][lang], reply_markup=main_menu(lang))
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="💰 Пополнить баланс", url=REF_LINK)
        kb.button(text="🔄 Проверить пополнение", callback_data="check_balance")
        kb.adjust(1)
        await cb.message.edit_text(MESSAGES["check_balance"][lang], reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data=="check_balance")
async def check_balance(cb: types.CallbackQuery):
    user = await get_user(cb.from_user.id)
    lang = await get_language(cb.from_user.id)
    if cb.from_user.id in AUTHORS or (user and user["balance"] >= MIN_DEPOSIT):
        await cb.message.edit_text(MESSAGES["access_open"][lang], reply_markup=main_menu(lang))
    else:
        await cb.answer(f"❌ Баланс меньше {MIN_DEPOSIT}$", show_alert=True)

@dp.callback_query(lambda c: c.data=="main_menu")
async def main_menu_cb(cb: types.CallbackQuery):
    lang = await get_language(cb.from_user.id)
    await cb.message.edit_text("Главное меню:" if lang=="ru" else "Main menu:", reply_markup=main_menu(lang))

@dp.callback_query(lambda c: c.data=="pairs")
async def pairs(cb: types.CallbackQuery):
    if not await has_access(cb.from_user.id):
        lang = await get_language(cb.from_user.id)
        await cb.answer("Нет доступа" if lang=="ru" else "No access", show_alert=True)
        return
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb())

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("Выберите пару", reply_markup=pairs_kb(page))

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text("Выберите экспирацию", reply_markup=exp_kb(pair))

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def exp(cb: types.CallbackQuery):
    _, pair, exp_time = cb.data.split(":")
    lang = await get_language(cb.from_user.id)
    direction, level = await get_signal(pair, int(exp_time), lang)
    await cb.message.edit_text(
        f"📊 СИГНАЛ KURUT TRADE\n\nПара: {pair.replace('=X','')}\nЭкспирация: {exp_time} мин\nНаправление: {direction}\nКачество: {level}",
        reply_markup=back_menu_kb(lang)
    )

@dp.callback_query(lambda c: c.data=="news")
async def news(cb: types.CallbackQuery):
    import random
    lang = await get_language(cb.from_user.id)
    pair = random.choice(PAIRS)
    exp_time = random.choice(EXPIRATIONS)
    direction, level = await get_signal(pair, exp_time, lang)
    await cb.message.edit_text(
        f"📰 НОВОСТНОЙ СИГНАЛ\n\n{pair.replace('=X','')} — {exp_time} мин\n{direction}\n{level}",
        reply_markup=back_menu_kb(lang)
    )

@dp.callback_query(lambda c: c.data=="change_lang")
async def change_lang(cb: types.CallbackQuery):
    await cb.message.edit_text("Выберите язык:" if await get_language(cb.from_user.id)=="ru" else "Choose language:", reply_markup=language_kb())

@dp.callback_query(lambda c: c.data.startswith("set_lang:"))
async def set_lang(cb: types.CallbackQuery):
    lang_code = cb.data.split(":")[1]
    await set_language(cb.from_user.id, lang_code)
    await cb.message.edit_text(f"Язык установлен: {LANGUAGES.get(lang_code, 'Русский')}", reply_markup=main_menu(lang_code))

# ================= POSTBACK =================

async def postback(request: web.Request):
    click_id = request.query.get("click_id","").strip()
    amount = request.query.get("amount","0")
    if not click_id.isdigit():
        logging.warning(f"Invalid click_id: {click_id}")
        return web.Response(text="NO CLICK_ID")
    try:
        await upsert_user(int(click_id))
        await update_balance(int(click_id), float(amount))
        logging.info(f"Postback success: user {click_id}, amount {amount}")
        return web.Response(text="OK")
    except Exception as e:
        logging.error(f"Postback error: {e}")
        return web.Response(text="ERROR")

# ================= START =================

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

    logging.info("BOT STARTED")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
