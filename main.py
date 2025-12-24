import os
import asyncio
import logging
import asyncpg
import pandas_ta as ta
import yfinance as yf
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.methods import DeleteWebhook, SetWebhook

# 1. КОНФИГ
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

# 2. ИНИЦИАЛИЗАЦИЯ (Важно: сначала создаем bot и dp!)
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL = None

# 3. ДАННЫЕ И СЛОВАРЬ
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]

LEXICON = {
    "ru": {
        "instr": "📘 **ИНСТРУКЦИЯ**\n\n1️⃣ Нажмите **Регистрация**.\n2️⃣ Создайте новый аккаунт.\n3️⃣ Пополните баланс от **$20**.\n4️⃣ Доступ откроется автоматически!",
        "reg_btn": "🔗 Регистрация", "check_btn": "✅ Проверить доступ", "pairs_btn": "📈 Валютные пары", "news_btn": "📰 Новости рынка",
        "up": "ВВЕРХ 📈", "down": "ВНИЗ 📉", "dir": "Направление", "str": "Сила", "target": "Цель", "wait": "⌛ Анализируем...", "sig_title": "СИГНАЛ", "strong": "Высокая 🔥"
    },
    "en": {
        "instr": "📘 **INSTRUCTION**\n\n1️⃣ Click **Registration**.\n2️⃣ Create account.\n3️⃣ Deposit **$20**+.\n4️⃣ Access opens automatically!",
        "reg_btn": "🔗 Registration", "check_btn": "✅ Check Access", "pairs_btn": "📈 Currency Pairs", "news_btn": "📰 Market News",
        "up": "UP 📈", "down": "DOWN 📉", "dir": "Direction", "str": "Strength", "target": "Target", "wait": "⌛ Analyzing...", "sig_title": "SIGNAL", "strong": "High 🔥"
    },
    "tj": {
        "instr": "📘 **ДАСТУРАМАЛ**\n\n1️⃣ Тугмаи **Бақайдгирӣ**-ро пахш кунед.\n2️⃣ Аккаунти нав созед.\n3️⃣ Амонат аз **$20** пур кунед.\n4️⃣ Дастрасӣ худкор кушода мешавад!",
        "reg_btn": "🔗 Бақайдгирӣ", "check_btn": "✅ Санҷиш", "pairs_btn": "📈 Ҷуфтҳои асъор", "news_btn": "📰 Хабарҳо",
        "up": "БОЛО 📈", "down": "ПОЁН 📉", "dir": "Самт", "str": "Қувва", "target": "Ҳадаф", "wait": "⌛ Таҳлил...", "sig_title": "СИГНАЛ", "strong": "Баланд 🔥"
    },
    "uz": {
        "instr": "📘 **YO'RIQNOMA**\n\n1️⃣ **Ro'yxatdan o'tish**ни босинг.\n2️⃣ Yangi hisob yarating.\n3️⃣ Balansni **$20** то'лдиринг.\n4️⃣ Kirish avtomatik ochiladi!",
        "reg_btn": "🔗 Ro'yxatdan o'tish", "check_btn": "✅ Tekshirish", "pairs_btn": "📈 Juftliklar", "news_btn": "📰 Yangiliklar",
        "up": "YUQORI 📈", "down": "PAST 📉", "dir": "Yo'nalish", "str": "Kuch", "target": "Maqsad", "wait": "⌛ Tahlil...", "sig_title": "SIGNAL", "strong": "Yuqori 🔥"
    },
    "kg": {
        "instr": "📘 **ИНСТРУКЦИЯ**\n\n1️⃣ **Каттоо** баскычын басыңыз.\n2️⃣ Жаңы аккаунт түзүңүз.\n3️⃣ Балансты **$20** толтуруңуз.\n4️⃣ Кирүү автоматтык түрдө ачылат!",
        "reg_btn": "🔗 Каттоо", "check_btn": "✅ Текшерүү", "pairs_btn": "📈 Жуптар", "news_btn": "📰 Жаңылыктар",
        "up": "ЖОГОРУ 📈", "down": "ТӨМӨН 📉", "dir": "Багыты", "str": "Күчү", "target": "Максат", "wait": "⌛ Анализ...", "sig_title": "СИГНАЛ", "strong": "Жогору 🔥"
    },
    "kz": {
        "instr": "📘 **НҰСҚАУЛЫҚ**\n\n1️⃣ **Тіркелу** түймесін басыңыз.\n2️⃣ Жаңа аккаунт ашыңыз.\n3️⃣ Депозит **$20** салыңыз.\n4️⃣ Кіру автоматты түрде ашылады!",
        "reg_btn": "🔗 Тіркелу", "check_btn": "✅ Тексеру", "pairs_btn": "📈 Жұптар", "news_btn": "📰 Жаңалықтар",
        "up": "ЖОҒАРЫ 📈", "down": "ТӨМЕН 📉", "dir": "Бағыты", "str": "Қуаты", "target": "Мақсат", "wait": "⌛ Талдау...", "sig_title": "СИГНАЛ", "strong": "Жоғары 🔥"
    }
}

# 4. ФУНКЦИИ ЛОГИКИ
async def get_lang(uid: int):
    if uid in AUTHORS: return "ru"
    async with DB_POOL.acquire() as conn:
        res = await conn.fetchval("SELECT language FROM users WHERE user_id=$1", uid)
        return res or "ru"

async def check_access(uid: int):
    if uid in AUTHORS: return True
    async with DB_POOL.acquire() as conn:
        bal = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", uid)
        return (bal or 0) >= MIN_DEPOSIT

async def get_signal(pair: str, exp: int, lang: str):
    try:
        data = yf.download(pair, period="1d", interval="1m", progress=False)
        if data.empty: return "❌ No Market Data"
        close = data['Close']
        rsi = ta.rsi(close, length=14).iloc[-1]
        sup = data['Low'].rolling(20).min().iloc[-1]
        res_p = data['High'].rolling(20).max().iloc[-1]
        l = LEXICON[lang]
        direction = l["up"] if rsi < 50 else l["down"]
        return (f"📊 **{l['sig_title']}: {pair.replace('=X','')}**\n━━━━━━━━━━━━━━\n"
                f"⏰ Time: **{exp} MIN**\n🚀 {l['dir']}: **{direction}**\n💪 {l['str']}: {l['strong']}\n"
                f"📍 {l['target']}: `{sup:.5f}-{res_p:.5f}`\n📈 RSI: `{rsi:.1f}`\n━━━━━━━━━━━━━━")
    except: return "❌ Analysis Error"

# 5. ХЕНДЛЕРЫ (Теперь dp уже создан выше)
@dp.message(Command("start"))
async def cmd_start(msg: types.Message):
    async with DB_POOL.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", msg.from_user.id)
    kb = InlineKeyboardBuilder()
    for n, c in [("🇷🇺 RU","ru"),("🇺🇸 EN","en"),("🇹🇯 TJ","tj"),("🇺🇿 UZ","uz"),("🇰🇬 KG","kg"),("🇰🇿 KZ","kz")]:
        kb.button(text=n, callback_data=f"sl:{c}")
    kb.adjust(2)
    await msg.answer("Выберите язык / Select language:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sl:"))
async def set_lang(cb: types.CallbackQuery):
    lang = cb.data.split(":")[1]
    async with DB_POOL.acquire() as conn:
        await conn.execute("UPDATE users SET language=$1 WHERE user_id=$2", lang, cb.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text=LEXICON[lang]["reg_btn"], url=f"{REF_LINK}&click_id={cb.from_user.id}")
    kb.button(text=LEXICON[lang]["check_btn"], callback_data="verify")
    kb.adjust(1)
    await cb.message.edit_text(LEXICON[lang]["instr"], reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "verify")
async def verify(cb: types.CallbackQuery):
    l = await get_lang(cb.from_user.id)
    if await check_access(cb.from_user.id):
        kb = InlineKeyboardBuilder()
        kb.button(text=LEXICON[l]["pairs_btn"], callback_data="plist:0")
        kb.button(text=LEXICON[l]["news_btn"], callback_data="vip_news")
        kb.adjust(1)
        await cb.message.edit_text("🏠 Главное меню / Main Menu", reply_markup=kb.as_markup())
    else:
        await cb.answer("❌ Deposit $20 first!", show_alert=True)

@dp.callback_query(F.data.startswith("plist:"))
async def plist(cb: types.CallbackQuery):
    if not await check_access(cb.from_user.id): return
    page = int(cb.data.split(":")[1])
    kb = InlineKeyboardBuilder()
    start = page * 8
    for p in PAIRS[start:start+8]: kb.button(text=p.replace("=X",""), callback_data=f"sel:{p}")
    kb.adjust(2)
    if start + 8 < len(PAIRS): kb.row(types.InlineKeyboardButton(text="➡️ Next", callback_data=f"plist:{page+1}"))
    kb.row(types.InlineKeyboardButton(text="🏠 Menu", callback_data="verify"))
    await cb.message.edit_text("Select Pair:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sel:"))
async def sel_exp(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    kb = InlineKeyboardBuilder()
    for e in [1, 5, 10]: kb.button(text=f"{e} MIN", callback_data=f"sg:{pair}:{e}")
    kb.adjust(1)
    await cb.message.edit_text(f"Asset: {pair.replace('=X','')}\nTime:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("sg:"))
async def sg(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    l = await get_lang(cb.from_user.id)
    await cb.answer(LEXICON[l]["wait"])
    res = await get_signal(pair, int(exp), l)
    kb = InlineKeyboardBuilder().button(text="⬅️ Back", callback_data="plist:0")
    await cb.message.edit_text(res, reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "vip_news")
async def vip_news(cb: types.CallbackQuery):
    import random
    l = await get_lang(cb.from_user.id)
    p = random.choice(PAIRS)
    res = await get_signal(p, 5, l)
    kb = InlineKeyboardBuilder().button(text="⬅️ Back", callback_data="verify")
    await cb.message.edit_text(f"🔥 **VIP NEWS**\n\n{res}", reply_markup=kb.as_markup(), parse_mode="Markdown")

# 6. ВЕБ-СЕРВЕР И ЗАПУСК
async def postback(request):
    uid = request.query.get("click_id")
    amt = request.query.get("amount", "0")
    if uid and uid.isdigit():
        async with DB_POOL.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", float(amt), int(uid))
            try: await bot.send_message(int(uid), "✅ Deposit Received! Access Open.")
            except: pass
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
    
    # Исправленный запуск для Render
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"Bot started on port {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
