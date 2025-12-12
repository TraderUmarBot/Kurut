# main.py - ОКОНЧАТЕЛЬНАЯ ВЕРСИЯ (ИСПРАВЛЕННЫЙ WEBHOOK)

import os
import asyncio
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import logging
import sqlite3 
import sys 

# --- Импорты aiogram ---
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder 
from aiogram.methods import DeleteWebhook
from aiogram.client.default import DefaultBotProperties
from aiogram.methods.set_webhook import SetWebhook 

# -------------------- Конфиг (WEBHOOK) --------------------
# Переменные читаются из Env Vars. URL формируется автоматически.
TG_TOKEN = os.environ.get("TG_TOKEN") 
PO_REFERRAL_LINK = "https://m.po-tck.com/ru/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START" 

# НАСТРОЙКИ WEBHOOK (Используем стандартные переменные Render)
WEB_SERVER_PORT = int(os.environ.get("PORT", 10000)) 
WEB_SERVER_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0") 
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME") 

# Формирование URL для Webhook
WEBHOOK_PATH = f"/webhook/{TG_TOKEN}" if TG_TOKEN else "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}" if RENDER_EXTERNAL_HOSTNAME else None

# ОСТАЛЬНЫЕ КОНСТАНТЫ
PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
]

TIMEFRAMES = [1, 3, 5, 10]
PAIRS_PER_PAGE = 6

USERS_FILE = "users.txt"
DB_FILE = "trades.db" 

# -------------------- Бот и диспетчер --------------------
bot = Bot(token=TG_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher(storage=MemoryStorage())

# -------------------- FSM (Состояния) --------------------
class Form(StatesGroup):
    waiting_for_referral = State() 
    choosing_pair = State()
    choosing_timeframe = State()

# -------------------- База данных (SQLite) --------------------

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            pair TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            result TEXT, -- 'PLUS' или 'MINUS'
            direction TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_trade(user_id: int, pair: str, timeframe: int, direction: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trades (user_id, pair, timeframe, direction) 
        VALUES (?, ?, ?, ?)
    """, (user_id, pair, timeframe, direction))
    trade_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return trade_id

def update_trade_result(trade_id: int, result: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE trades SET result = ? WHERE id = ?
    """, (result, trade_id))
    conn.commit()
    conn.close()

def get_user_stats(user_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT result, COUNT(*) FROM trades WHERE user_id = ? AND result IS NOT NULL GROUP BY result", (user_id,))
    stats = dict(cursor.fetchall())
    
    cursor.execute("SELECT pair, result, COUNT(*) FROM trades WHERE user_id = ? AND result IS NOT NULL GROUP BY pair, result", (user_id,))
    pair_stats = cursor.fetchall()

    conn.close()
    
    formatted_pair_stats = {}
    for pair, result, count in pair_stats:
        if pair not in formatted_pair_stats:
            formatted_pair_stats[pair] = {'PLUS': 0, 'MINUS': 0}
        formatted_pair_stats[pair][result] = count

    return {
        'total_plus': stats.get('PLUS', 0),
        'total_minus': stats.get('MINUS', 0),
        'pair_stats': formatted_pair_stats
    }

# -------------------- Пользователи (для проверки реферала) --------------------
def load_users():
    try:
        with open(USERS_FILE, "r") as f:
            return set(int(line.strip()) for line in f.readlines())
    except:
        return set()

def save_user(user_id):
    users = load_users()
    if user_id not in users:
        users.add(user_id)
        with open(USERS_FILE, "a") as f: 
            f.write(f"{user_id}\n")

# -------------------- Клавиатуры --------------------

def get_main_menu_keyboard():
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
    
    if user_id in load_users():
        await state.clear()
        await message.answer(
            "🏠 **Главное меню**\n\nВыберите действие:",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await state.set_state(Form.waiting_for_referral)
        referral_text = (
            "🚀 **Привет! Для получения торговых сигналов тебе необходимо зарегистрироваться "
            "по нашей реферальной ссылке Pocket Option!**\n\n"
            f"1. Перейди по ссылке: [НАША РЕФЕРАЛЬНАЯ ССЫЛКА]({PO_REFERRAL_LINK})\n"
            "2. Зарегистрируйся.\n"
            "3. **После регистрации** скопируй свой **ID аккаунта** (только цифры) "
            "и **отправь его в этот чат** для активации бота."
        )
        await message.answer(referral_text)


@dp.callback_query(lambda c: c.data in ["main_menu", "start_trade"])
async def main_menu_handler(query: types.CallbackQuery, state: FSMContext):
    await state.clear()
    
    if query.data == "main_menu":
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
    stats = get_user_stats(user_id)
    
    total_trades = stats['total_plus'] + stats['total_minus']
    
    if total_trades == 0:
        text = "📜 **История сделок**\n\nУ вас пока нет закрытых сделок."
    else:
        # Устойчивый расчет Win Rate
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
            plus = data['PLUS']
            minus = data['MINUS']
            total = plus + minus
            pair_win_rate = (plus / total) * 100 if total > 0 else 0
            text += (
                f"\n**{pair}**: {plus} ✅ / {minus} ❌ ({pair_win_rate:.1f}%)"
            )

    await query.message.edit_text(
        text,
        reply_markup=get_main_menu_keyboard()
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("result:"))
async def trade_result_handler(query: types.CallbackQuery, state: FSMContext):
    _, trade_id_str, result = query.data.split(":")
    trade_id = int(trade_id_str)
    
    update_trade_result(trade_id, result)
    
    icon = "✅" if result == "PLUS" else "❌"
    
    # Редактируем сообщение, чтобы убрать кнопки и показать результат
    await query.message.edit_reply_markup(reply_markup=None)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🏠 Главное меню", callback_data="main_menu")
    
    await query.message.answer(
        f"{icon} **Результат сделки сохранен: {result}**\n\n"
        "Выберите следующее действие:",
        reply_markup=keyboard.as_markup()
    )

    await query.answer(f"Результат {result} сохранен!")
    await state.clear()
    await state.set_state(None)


@dp.message(Form.waiting_for_referral)
async def process_referral_check(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    user_id = message.from_user.id
    is_valid = user_input.isdigit() and len(user_input) > 4

    if is_valid:
        save_user(user_id) 
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
    page = int(query.data.split(":")[1])
    await query.message.edit_text(
        "Выбери валютную пару:",
        reply_markup=get_pairs_keyboard(page)
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair_handler(query: types.CallbackQuery, state: FSMContext):
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
    _, pair, tf = query.data.split(":")
    tf = int(tf)
    
    await query.answer("Идет загрузка сигнала...", show_alert=False) 
    message_to_edit = await query.message.edit_text(f"Выбраны {pair} и {tf} мин. Идет загрузка сигнала...")

    try:
        await send_signal(pair, tf, query.from_user.id, message_to_edit.chat.id, message_to_edit.message_id)
    except Exception as e:
        error_text = f"❌ **Критическая ошибка.** Не удалось обработать сигнал. Пожалуйста, попробуйте снова или выберите другую пару."
        await bot.edit_message_text(
            chat_id=message_to_edit.chat.id, 
            message_id=message_to_edit.message_id, 
            text=error_text
        )
        logging.error(f"Критическая ошибка в tf_handler: {e}")
        
    await state.clear() 

# -------------------- Получение свечей и Индикаторы --------------------
# Используется yfinance, pandas и pandas-ta (они должны быть в requirements.txt)

def fetch_ohlcv(symbol: str, exp_minutes: int) -> pd.DataFrame:
    interval = "1m"
    try:
        # Добавляем '=X' для корректной работы с yfinance для Forex
        df = yf.download(f"{symbol}=X", period="5d", interval=interval, progress=False) 
    except Exception as e:
        logging.error(f"Ошибка загрузки данных YFinance для {symbol}: {e}")
        return pd.DataFrame() 

    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame()

    df = df[required_cols] 
    df.columns = [col.lower() for col in required_cols]
    
    if exp_minutes > 1 and not df.empty:
        # Пересчет на нужный таймфрейм
        df = df.resample(f"{exp_minutes}min").agg({
            'open':'first','high':'max','low':'min','close':'last','volume':'sum'
        }).dropna()
        
    return df

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df['ema9'] = ta.ema(df['close'], length=9)
    df['ema21'] = ta.ema(df['close'], length=21)
    df['sma50'] = ta.sma(df['close'], length=50)
    
    # MACD (Устойчивый расчет)
    macd = ta.macd(df['close'])
    df['macd'] = macd['MACD_12_26_9']
    df['macd_signal'] = macd['MACDs_12_26_9']
    
    df['rsi14'] = ta.rsi(df['close'], length=14)
    
    # Stochastic (Устойчивый расчет)
    stoch = ta.stoch(df['high'], df['low'], df['close'])
    df['stoch_k'] = stoch['STOCHk_14_3_3']
    df['stoch_d'] = stoch['STOCHd_14_3_3']

    df['cci20'] = ta.cci(df['high'], df['low'], df['close'], length=20)
    
    # Bollinger Bands
    bb = ta.bbands(df['close'])
    df['bb_upper'] = bb['BBU_20_2.0']
    df['bb_lower'] = bb['BBL_20_2.0']
        
    # ADX/ATR
    adx_df = ta.adx(df['high'], df['low'], df['close'])
    df['atr14'] = ta.atr(df['high'], df['low'], df['close'])
    df['adx14'] = adx_df['ADX_14']
    
    # Паттерны свечей
    df['hammer'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['close']-df['low'])/(.001+df['high']-df['low'])>0.6)
    df['shooting_star'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['high']-df['close'])/(.001+df['high']-df['low'])>0.6)
    
    # Чистим от NaN и берем только последние данные
    critical_cols = ['ema9', 'ema21', 'macd', 'rsi14', 'stoch_k', 'adx14']
    df_cleaned = df.dropna(subset=critical_cols)
    
    return df_cleaned.tail(100)

def support_resistance(df: pd.DataFrame) -> dict:
    levels = {}
    df_sr = df.tail(20) 
    if not df_sr.empty:
        levels['support'] = df_sr['low'].min()
        levels['resistance'] = df_sr['high'].max()
    else:
        levels['support'] = float('nan')
        levels['resistance'] = float('nan')
    return levels

def indicator_vote(latest: pd.Series) -> dict:
    score = 0
    
    is_trending = latest['adx14'] > 25
    
    # Трендовая стратегия
    if is_trending:
        if latest['ema9'] > latest['ema21'] and latest['close'] > latest['sma50']:
            score += 2 # Сильный BUY
        elif latest['ema9'] < latest['ema21'] and latest['close'] < latest['sma50']:
            score -= 2 # Сильный SELL
    
    # Контртрендовые условия (Перекупленность/Перепроданность)
    is_oversold = (latest['rsi14'] < 30) and (latest['stoch_k'] < 20)
    is_overbought = (latest['rsi14'] > 70) and (latest['stoch_k'] > 80)
    
    if is_oversold: score += 1
    if is_overbought: score -= 1

    # Паттерны свечей
    if latest['hammer']: score += 1
    if latest['shooting_star']: score -= 1
            
    if score >= 2:
        direction = "BUY"
    elif score <= -2:
        direction = "SELL"
    else:
        direction = "HOLD" # Если нет явного сигнала

    confidence = min(100, abs(score) * 20 + 40) # Расчет уверенности
    
    return {"direction": direction, "confidence": confidence, "score": score}

async def send_signal(pair: str, timeframe: int, user_id: int, chat_id: int, message_id: int):
    
    df = fetch_ohlcv(pair, timeframe)
    
    if df.empty or len(df) < 50: 
        error_text = f"❌ **Ошибка.** Не удалось загрузить достаточно свечей (нужно >50) для {pair} {timeframe} мин. Попробуйте позже."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text)
        return
        
    df_ind = compute_indicators(df)
    
    if df_ind.empty:
        error_text = f"❌ **Ошибка.** Индикаторы не рассчитаны (недостаточно полных данных после очистки). Попробуйте меньший таймфрейм."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text)
        return
        
    latest = df_ind.iloc[-1]
    
    res = indicator_vote(latest)
    sr = support_resistance(df_ind)
    
    # Сохраняем сделку в базу данных
    trade_id = save_trade(user_id, pair, timeframe, res['direction'])

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

# -------------------- БЛОК ЗАПУСКА WEBHOOK (УСТОЙЧИВЫЙ И ИСПРАВЛЕННЫЙ) --------------------

async def on_startup_webhook(bot: Bot):
    """
    Принудительно устанавливает Webhook перед запуском сервера.
    """
    if WEBHOOK_URL:
        await bot(DeleteWebhook(drop_pending_updates=True))
        await bot(SetWebhook(url=WEBHOOK_URL))
        logging.info(f"✅ Webhook успешно переустановлен: {WEBHOOK_URL}")
    else:
        logging.error("❌ Webhook URL не определен. Невозможно установить Webhook.")

async def on_shutdown_webhook(bot: Bot):
    """Удаляет webhook URL при завершении работы."""
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
    except Exception as e:
        logging.error(f"Ошибка при удалении Webhook: {e}")
    logging.info("❌ Webhook удален.")


async def start_webhook():
    """Главная асинхронная функция, явно запускающая Webhook-сервер."""
    
    # Проверка обязательных переменных
    if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
        logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задан TG_TOKEN или RENDER_EXTERNAL_HOSTNAME. Выход.")
        sys.exit(1)
        
    init_db() 
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(f"--- ЗАПУСК WEBHOOK СЕРВЕРА: {WEBHOOK_URL} ---")
    
    dp.startup.register(on_startup_webhook)
    dp.shutdown.register(on_shutdown_webhook)
    
    # Явный запуск aiohttp Web Server - ИСПРАВЛЕНО НА run_webhook
    try:
        await dp.run_webhook( # <--- ИСПРАВЛЕНО
            bot=bot,
            webhook_url=WEBHOOK_URL,
            host=WEB_SERVER_HOST,
            port=WEB_SERVER_PORT,
            path=WEBHOOK_PATH,
            allowed_updates=dp.resolve_used_update_types()
        )
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ЗАПУСКА WEBHOOK-СЕРВЕРА: {e}")
        sys.exit(1) 

def main():
    """Точка входа в программу."""
    try:
        asyncio.run(start_webhook())
    except Exception as e:
        logging.error(f"Непредвиденная ошибка в main(): {e}")


if __name__ == "__main__":
    main()

