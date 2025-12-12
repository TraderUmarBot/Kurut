# main.py - V4-FIXED: БОЕВАЯ ВЕРСИЯ (PostgreSQL + Async API + СТАБИЛЬНЫЙ WEBHOOK)

import os
import asyncio
import pandas as pd
import pandas_ta as ta
import logging
import sys 
from typing import Dict, Any, List, Union
from datetime import datetime
import time

# --- Добавлены библиотеки для стабильности ---
import asyncpg 
from functools import lru_cache 

# --- Импорты для aiogram V3 и aiohttp V2 запуска ---
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder 
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import setup_application
from aiohttp import web 
from aiogram.utils.markdown import link

# --- ВРЕМЕННЫЙ ИМПОРТ: Заглушка для API ---
import yfinance as yf 

# -------------------- Конфиг и Ключи --------------------
TG_TOKEN = os.environ.get("TG_TOKEN") 
DATABASE_URL = os.environ.get("DATABASE_URL") 
API_KEY = os.environ.get("API_KEY") 
SECRET_KEY = os.environ.get("SECRET_KEY") 

PO_REFERRAL_LINK = "https://m.po-tck.com/ru/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START" 

# НАСТРОЙКИ WEBHOOK
WEB_SERVER_PORT = int(os.environ.get("PORT", 10000)) 
WEB_SERVER_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0") 
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME") 

# Проверка критических переменных
if not all([TG_TOKEN, RENDER_EXTERNAL_HOSTNAME]):
    logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задан TG_TOKEN или RENDER_EXTERNAL_HOSTNAME. Выход.")
    sys.exit(1)

# --- КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ WEBHOOK PATH ---
# 1. Путь для установки в Telegram API (С ТОКЕНОМ)
WEBHOOK_PATH = f"/webhook/{TG_TOKEN}" 
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

# 2. Путь для aiohttp роутера (ДОЛЖЕН СОВПАДАТЬ С ПУНКТ 1, чтобы избежать 404)
WEBHOOK_BASE_PATH = WEBHOOK_PATH # ИСПРАВЛЕНО!

# ОСТАЛЬНЫЕ КОНСТАНТЫ
PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
]
TIMEFRAMES = [1, 3, 5, 10]
PAIRS_PER_PAGE = 6

# -------------------- Бот и диспетчер --------------------
bot = Bot(token=TG_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_POOL: Union[asyncpg.Pool, None] = None 


# -------------------- PostgreSQL Логика (ОБЯЗАТЕЛЬНАЯ) --------------------

async def init_db_pool():
    """Инициализирует пул подключений к PostgreSQL."""
    global DB_POOL
    if not DATABASE_URL:
        logging.warning("⚠️ DATABASE_URL не задан. Бот будет работать без сохранения истории и авторизации (In-Memory).")
        return
    try:
        DB_POOL = await asyncpg.create_pool(DATABASE_URL)
        logging.info("✅ Пул PostgreSQL успешно создан.")
        await init_db_tables()
    except Exception as e:
        logging.error(f"❌ Ошибка подключения или создания пула PostgreSQL: {e}")
        # Не выходим, чтобы бот работал хотя бы в In-Memory режиме, если DB упала.

async def init_db_tables():
    """Создает необходимые таблицы (users и trades)."""
    if not DB_POOL: return
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pair TEXT NOT NULL,
                timeframe INTEGER NOT NULL,
                result TEXT, 
                direction TEXT
            );
        """)
    logging.info("✅ Таблицы users и trades успешно созданы/проверены.")


# In-Memory заглушка, если DB не работает
AUTHORIZED_USERS: Dict[int, bool] = {}

async def save_user_db(user_id: int):
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            try:
                await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)
            except Exception as e:
                logging.error(f"Ошибка сохранения пользователя {user_id} в DB: {e}")
    else:
        AUTHORIZED_USERS[user_id] = True

async def is_user_authorized_db(user_id: int) -> bool:
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            result = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id)
            return result is not None
    else:
        return user_id in AUTHORIZED_USERS

async def save_trade_db(user_id: int, pair: str, timeframe: int, direction: str) -> int:
    # Используем заглушку, так как без DB нельзя вернуть ID сделки для дальнейшего обновления.
    if not DB_POOL: 
        logging.warning("⚠️ DB недоступна. Сделка не сохранена.")
        return int(time.time()) 

    async with DB_POOL.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO trades (user_id, pair, timeframe, direction) 
            VALUES ($1, $2, $3, $4)
            RETURNING id
        """, user_id, pair, timeframe, direction)

async def update_trade_result_db(trade_id: int, result: str):
    if not DB_POOL: return
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            UPDATE trades SET result = $1 WHERE id = $2
        """, result, trade_id)

async def get_user_stats_db(user_id: int) -> Dict[str, Any]:
    if not DB_POOL:
        return {'total_plus': 0, 'total_minus': 0, 'pair_stats': {}}

    async with DB_POOL.acquire() as conn:
        stats_rows = await conn.fetch("""
            SELECT result, COUNT(*) FROM trades 
            WHERE user_id = $1 AND result IS NOT NULL 
            GROUP BY result
        """, user_id)
        stats = dict(stats_rows)

        pair_rows = await conn.fetch("""
            SELECT pair, result, COUNT(*) FROM trades 
            WHERE user_id = $1 AND result IS NOT NULL 
            GROUP BY pair, result
        """, user_id)
    
    formatted_pair_stats: Dict[str, Dict[str, int]] = {}
    for pair, result, count in pair_rows:
        if pair not in formatted_pair_stats:
            formatted_pair_stats[pair] = {'PLUS': 0, 'MINUS': 0}
        if result in formatted_pair_stats[pair]:
            formatted_pair_stats[pair][result] = count

    return {
        'total_plus': stats.get('PLUS', 0),
        'total_minus': stats.get('MINUS', 0),
        'pair_stats': formatted_pair_stats
    }


# -------------------- FSM и Клавиатуры --------------------
class Form(StatesGroup):
    waiting_for_referral = State() 
    choosing_pair = State()
    choosing_timeframe = State()

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📈 Выбрать пару", callback_data="start_trade")
    builder.button(text="📜 История сделок", callback_data="show_history")
    builder.adjust(1)
    return builder.as_markup()

def get_trade_result_keyboard(trade_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ ПЛЮС", callback_data=f"result:{trade_id}:PLUS")
    builder.button(text="❌ МИНУС", callback_data=f"result:{trade_id}:MINUS")
    builder.adjust(2)
    return builder.as_markup()

def get_pairs_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    builder = InlineKeyboardBuilder() 
    for pair in PAIRS[start:end]:
        builder.button(text=pair, callback_data=f"pair:{pair}")
    
    builder.adjust(2) 
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page:{page-1}"))
    if end < len(PAIRS):
        nav_buttons.append(InlineKeyboardButton(text="➡️ Вперед", callback_data=f"page:{page+1}"))
    
    if nav_buttons:
        builder.row(*nav_buttons) 
    
    builder.row(InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu"))
    
    return builder.as_markup()

def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        builder.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    builder.adjust(2) 
    
    builder.row(InlineKeyboardButton(text="◀️ Назад к парам", callback_data="start_trade"))
    
    return builder.as_markup()


# -------------------- Обработчики (Хендлеры) --------------------

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if await is_user_authorized_db(user_id):
        await state.clear()
        await message.answer(
            "🏠 **Главное меню**\n\nВыберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await state.set_state(Form.waiting_for_referral)
        referral_link = link("НАША РЕФЕРАЛЬНАЯ ССЫЛКА", PO_REFERRAL_LINK)
        referral_text = (
            "🚀 **Привет! Для получения торговых сигналов тебе необходимо зарегистрироваться "
            "по нашей реферальной ссылке Pocket Option!**\n\n"
            f"1. Перейди по ссылке: {referral_link}\n"
            "2. Зарегистрируйся.\n"
            "3. **После регистрации** скопируй свой **ID аккаунта** (только цифры) "
            "и **отправь его в этот чат** для активации бота."
        )
        await message.answer(referral_text)


@dp.callback_query(lambda c: c.data in ["main_menu", "start_trade"])
async def main_menu_handler(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    if not await is_user_authorized_db(user_id):
        await query.answer("Сначала активируйте бота, отправив свой ID.", show_alert=True)
        return
        
    await state.clear()
    
    if query.data == "main_menu":
        if query.message:
            await query.message.edit_text(
                "🏠 **Главное меню**\n\nВыберите действие:",
                reply_markup=get_main_menu_keyboard()
            )
        
    elif query.data == "start_trade":
        await state.set_state(Form.choosing_pair)
        await query.message.edit_text(
            "📈 Выбери валютную пару:",
            reply_markup=get_pairs_keyboard(0)
        )
        
    await query.answer()

@dp.callback_query(lambda c: c.data == "show_history")
async def show_history_handler(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    if not await is_user_authorized_db(user_id):
        await query.answer("Сначала активируйте бота, отправив свой ID.", show_alert=True)
        return
        
    stats = await get_user_stats_db(user_id) 
    
    total_trades = stats['total_plus'] + stats['total_minus']
    
    if total_trades == 0:
        text = "📜 **История сделок**\n\nУ вас пока нет закрытых сделок."
    else:
        win_rate = (stats['total_plus'] / total_trades) * 100 if total_trades > 0 else 0
        
        text = (
            "📜 **История сделок**\n\n"
            f"Общее количество сделок: **{total_trades}**\n"
            f"✅ Плюсовых: **{stats['total_plus']}**\n"
            f"❌ Минусовых: **{stats['total_minus']}**\n"
            f"🎯 Процент побед (Win Rate): **{win_rate:.2f}%**\n\n"
            "--- Статистика по парам ---"
        )
        
        for pair, data in stats['pair_stats'].items():
            plus = data.get('PLUS', 0)
            minus = data.get('MINUS', 0)
            total = plus + minus
            pair_win_rate = (plus / total) * 100 if total > 0 else 0
            text += (
                f"\n**{pair}**: {plus} ✅ / {minus} ❌ ({pair_win_rate:.1f}%)"
            )
        
        if not DB_POOL:
            text += "\n\n⚠️ **Примечание:** История сохраняется только до перезапуска, так как DATABASE_URL не задан или не работает."

    await query.message.edit_text(
        text,
        reply_markup=get_main_menu_keyboard()
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("result:"))
async def trade_result_handler(query: types.CallbackQuery, state: FSMContext):
    _, trade_id_str, result = query.data.split(":")
    trade_id = int(trade_id_str)
    
    await update_trade_result_db(trade_id, result)
    
    icon = "✅" if result == "PLUS" else "❌"
    
    await query.message.edit_reply_markup(reply_markup=None)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🏠 Главное меню", callback_data="main_menu")
    
    text = f"{icon} **Результат сделки сохранен: {result}**\n\nВыберите следующее действие:"
    if not DB_POOL:
        text += "\n\n⚠️ **ВНИМАНИЕ:** История не сохранена навсегда (нет DB)."
    
    await query.message.answer(
        text,
        reply_markup=keyboard.as_markup()
    )

    await query.answer(f"Результат {result} сохранен!")

@dp.message(Form.waiting_for_referral)
async def process_referral_check(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    user_id = message.from_user.id
    is_valid = user_input.isdigit() and len(user_input) > 4

    if is_valid:
        await save_user_db(user_id) 
        await state.clear()
        
        await message.answer(
            "✅ **Активация успешна!**\n\n"
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await message.answer(
            "❌ **Ошибка активации.**\nПожалуйста, убедитесь, что вы прислали свой **ID аккаунта** (только цифры)."
        )

@dp.callback_query(lambda c: c.data.startswith("page:"))
async def page_handler(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    if not await is_user_authorized_db(user_id):
        await query.answer("Сначала активируйте бота.", show_alert=True)
        return
        
    page = int(query.data.split(":")[1])
    await query.message.edit_text(
        "Выбери валютную пару:",
        reply_markup=get_pairs_keyboard(page)
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_handler(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    if not await is_user_authorized_db(user_id):
        await query.answer("Сначала активируйте бота.", show_alert=True)
        return
        
    pair = query.data.split(":")[1]
    await state.update_data(selected_pair=pair)
    
    await state.set_state(Form.choosing_timeframe) 
    
    await query.message.edit_text(
        f"Выбрана пара {pair}. Теперь выбери таймфрейм:",
        reply_markup=get_timeframes_keyboard(pair)
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf_handler(query: types.CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    if not await is_user_authorized_db(user_id):
        await query.answer("Сначала активируйте бота.", show_alert=True)
        return
        
    current_state = await state.get_state()
    if current_state != Form.choosing_timeframe:
        await query.answer("⏳ Дождитесь завершения предыдущего запроса или выберите пару снова.", show_alert=False)
        return
        
    await state.set_state(None) 

    _, pair, tf = query.data.split(":")
    tf = int(tf)
    
    await query.answer("Идет загрузка сигнала...", show_alert=False) 
    message_to_edit = await query.message.edit_text(f"Выбраны {pair} и {tf} мин. Идет загрузка сигнала...")

    try:
        await send_signal(pair, tf, query.from_user.id, message_to_edit.chat.id, message_to_edit.message_id)
    except Exception as e:
        error_text = f"❌ **Критическая ошибка.** Не удалось обработать сигнал. Попробуйте позже."
        await bot.edit_message_text(
            chat_id=message_to_edit.chat.id, 
            message_id=message_to_edit.message_id, 
            text=error_text
        )
        logging.error(f"Критическая ошибка в tf_handler: {e}")
        
    
# -------------------- Получение свечей и Индикаторы --------------------

# Кэш сбрасывается каждую минуту
@lru_cache(maxsize=128)
def get_cache_key(symbol: str, exp_minutes: int, current_minute: int) -> str:
    return f"{symbol}_{exp_minutes}_{current_minute}"


async def async_fetch_ohlcv(symbol: str, exp_minutes: int) -> pd.DataFrame:
    current_minute = datetime.now().minute
    cache_key = get_cache_key(symbol, exp_minutes, current_minute)
    
    def sync_fetch_data():
        try:
            # Используем yfinance как заглушку, так как ключи Alpaca не предоставлены.
            df = yf.download(f"{symbol}=X", period="5d", interval="1m", progress=False, show_errors=False) 
        except Exception as e:
            logging.error(f"Ошибка загрузки данных YFinance для {symbol}: {e}")
            return pd.DataFrame() 

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            return pd.DataFrame()

        df = df[required_cols].copy() 
        df.columns = [col.lower() for col in required_cols]
        
        if exp_minutes > 1 and not df.empty:
            df = df.resample(f"{exp_minutes}min").agg({
                'open':'first','high':'max','low':'min','close':'last','volume':'sum'
            }).dropna()
            
        if len(df) < 50:
            logging.warning(f"Недостаточно данных ({len(df)}) для {symbol} {exp_minutes}min.")
            return pd.DataFrame()
            
        return df

    return await asyncio.to_thread(sync_fetch_data)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df['ema9'] = ta.ema(df['close'], length=9)
    df['ema21'] = ta.ema(df['close'], length=21)
    df['sma50'] = ta.sma(df['close'], length=50)
    
    macd = ta.macd(df['close'])
    df['macd'] = macd['MACD_12_26_9']
    df['macd_signal'] = macd['MACDs_12_26_9']
    
    df['rsi14'] = ta.rsi(df['close'], length=14)
    
    stoch = ta.stoch(df['high'], df['low'], df['close'])
    df['stoch_k'] = stoch['STOCHk_14_3_3']
    df['stoch_d'] = stoch['STOCHd_14_3_3']

    df['cci20'] = ta.cci(df['high'], df['low'], df['close'], length=20)
    
    critical_cols = ['ema9', 'ema21', 'macd', 'rsi14', 'stoch_k', 'sma50']
    df_cleaned = df.dropna(subset=critical_cols)
    
    return df_cleaned.tail(50)

def support_resistance(df: pd.DataFrame) -> Dict[str, float]:
    levels = {'support': float('nan'), 'resistance': float('nan')}
    df_sr = df.tail(10) 
    if not df_sr.empty:
        levels['support'] = df_sr['low'].min()
        levels['resistance'] = df_sr['high'].max()
    return levels

def indicator_vote(latest: pd.Series) -> Dict[str, Union[str, float]]:
    score = 0
    
    if latest['ema9'] > latest['ema21'] and latest['close'] > latest['ema21']:
        score += 1 
    elif latest['ema9'] < latest['ema21'] and latest['close'] < latest['ema21']:
        score -= 1 
    
    if latest['rsi14'] < 30: score += 1 
    if latest['rsi14'] > 70: score -= 1 

    if latest['macd'] > latest['macd_signal'] and latest['macd'] < 0:
        score += 1 
    elif latest['macd'] < latest['macd_signal'] and latest['macd'] > 0:
        score -= 1 
    
    if latest['stoch_k'] < 20 and latest['stoch_k'] > latest['stoch_d']:
        score += 1 
    if latest['stoch_k'] > 80 and latest['stoch_k'] < latest['stoch_d']:
        score -= 1 
            
    if score >= 2:
        direction = "BUY"
    elif score <= -2:
        direction = "SELL"
    else:
        direction = "HOLD" 

    confidence = min(100, abs(score) * 20 + 30)
    
    return {"direction": direction, "confidence": confidence, "score": score}


async def send_signal(pair: str, timeframe: int, user_id: int, chat_id: int, message_id: int):
    
    df = await async_fetch_ohlcv(pair, timeframe)
    
    if df.empty: 
        error_text = f"❌ **Ошибка.** Не удалось загрузить достаточно данных для {pair} {timeframe} мин. Попробуйте позже."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text)
        return
        
    df_ind = compute_indicators(df)
    
    if df_ind.empty:
        error_text = f"❌ **Ошибка.** Не удалось рассчитать индикаторы. Данные слишком неполные."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text)
        return
        
    latest = df_ind.iloc[-1]
    
    res = indicator_vote(latest)
    sr = support_resistance(df_ind)
    
    # Сохраняем сделку в PostgreSQL (или получаем временный ID)
    trade_id = await save_trade_db(user_id, pair, timeframe, res['direction'])

    dir_map = {"BUY":"🔺 ПОКУПКА","SELL":"🔻 ПРОДАЖА","HOLD":"⚠️ НЕОДНОЗНАЧНО"}
    text = (
        f"📊 **Сигнал #{trade_id}**\n\n"
        f"Пара: {pair}\n"
        f"Таймфрейм: {timeframe} мин\n\n"
        f"Направление: **{dir_map[res['direction']]}**\n"
        f"Уверенность: {res['confidence']:.0f}%\n\n"
        f"Поддержка: {sr['support']:.5f}\n"
        f"Сопротивление: {sr['resistance']:.5f}\n\n"
        f"**Нажмите кнопку ниже после закрытия сделки:**"
    )
    
    try:
        await bot.edit_message_text(
            chat_id=chat_id, 
            message_id=message_id, 
            text=text, 
            reply_markup=get_trade_result_keyboard(trade_id)
        )
    except Exception as e:
        logging.error(f"Ошибка при редактировании сообщения пользователю {chat_id}: {e}")

# -------------------- БЛОК ЗАПУСКА WEBHOOK (ФИНАЛЬНЫЙ С FIX) --------------------

async def on_startup_webhook(bot: Bot):
    await init_db_pool() # Инициализация пула DB при старте
    
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
        if WEBHOOK_URL:
            # Устанавливаем Webhook с полным путем (с токеном)
            await bot(SetWebhook(url=WEBHOOK_URL)) 
            logging.info(f"✅ Webhook успешно переустановлен: {WEBHOOK_URL}")
        else:
            logging.error("❌ Webhook URL не определен.")
    except Exception as e:
        logging.error(f"Ошибка в on_startup_webhook: {e}")

async def on_shutdown_webhook(bot: Bot):
    try:
        if DB_POOL:
            await DB_POOL.close()
            logging.info("❌ Пул PostgreSQL закрыт.")
        await bot(DeleteWebhook(drop_pending_updates=True))
    except Exception as e:
        logging.error(f"Ошибка при удалении Webhook/закрытии DB: {e}")
    logging.info("❌ Webhook удален.")


async def start_webhook():
    logging.info(f"--- ЗАПУСК WEBHOOK СЕРВЕРА V4-FIXED: {WEBHOOK_URL} ---")
    
    dp.startup.register(on_startup_webhook)
    dp.shutdown.register(on_shutdown_webhook)
    
    app = web.Application()
    
    # Используем WEBHOOK_BASE_PATH, который теперь включает токен!
    setup_application(app, dp, bot=bot, path=WEBHOOK_BASE_PATH) 
    
    try:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        await site.start()
        logging.info(f"🌐 Сервер запущен на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
        
        await asyncio.Event().wait() 

    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА WEBHOOK-СЕРВЕРА: {e}")
        sys.exit(1) 

def main():
    try:
        asyncio.run(start_webhook())
    except Exception as e:
        logging.error(f"Непредвиденная ошибка в main(): {e}")


if __name__ == "__main__":
    main()

