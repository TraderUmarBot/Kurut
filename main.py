# main.py - V3: УЛУЧШЕННАЯ СТАБИЛЬНОСТЬ И АРХИТЕКТУРА (WEBHOOK V2)

import os
import asyncio
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import logging
import sys 
from typing import Dict, Any, List, Union
from threading import Lock # Для защиты данных в памяти

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
import time

# -------------------- Конфиг (WEBHOOK) --------------------
# Переменные читаются из Env Vars.
TG_TOKEN = os.environ.get("TG_TOKEN") 
PO_REFERRAL_LINK = "https://m.po-tck.com/ru/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START" 

# НАСТРОЙКИ WEBHOOK (Используем стандартные переменные Render)
WEB_SERVER_PORT = int(os.environ.get("PORT", 10000)) 
WEB_SERVER_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0") 
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME") 

# --- КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ ПУТИ WEBHOOK ---
if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME:
    logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задан TG_TOKEN или RENDER_EXTERNAL_HOSTNAME. Выход.")
    sys.exit(1)

# WEBHOOK_PATH для установки в Telegram API (с токеном)
WEBHOOK_PATH = f"/webhook/{TG_TOKEN}" 
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"
# WEBHOOK_BASE_PATH для aiohttp роутера (без токена)
WEBHOOK_BASE_PATH = "/webhook"

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

# -------------------- In-Memory База данных (ЗАМЕНА SQLite и users.txt) --------------------
# Используем словари в памяти, так как Render не гарантирует сохранение файлов.

# {user_id: True}
AUTHORIZED_USERS: Dict[int, bool] = {}

# {trade_id: {user_id: 123, pair: 'EURUSD', timeframe: 5, direction: 'BUY', result: None}}
ACTIVE_TRADES: Dict[int, Dict[str, Any]] = {}
trade_id_counter: int = 1
data_lock = Lock() # Мьютекс для защиты общих данных

def get_next_trade_id() -> int:
    global trade_id_counter
    with data_lock:
        trade_id_counter += 1
        return trade_id_counter - 1

def save_user(user_id: int):
    with data_lock:
        AUTHORIZED_USERS[user_id] = True

def is_user_authorized(user_id: int) -> bool:
    with data_lock:
        return user_id in AUTHORIZED_USERS

def save_trade(user_id: int, pair: str, timeframe: int, direction: str) -> int:
    trade_id = get_next_trade_id()
    with data_lock:
        ACTIVE_TRADES[trade_id] = {
            'user_id': user_id,
            'timestamp': time.time(),
            'pair': pair,
            'timeframe': timeframe,
            'direction': direction,
            'result': None
        }
    return trade_id

def update_trade_result(trade_id: int, result: str):
    with data_lock:
        if trade_id in ACTIVE_TRADES:
            ACTIVE_TRADES[trade_id]['result'] = result

def get_user_stats(user_id: int) -> Dict[str, Any]:
    with data_lock:
        user_trades = [trade for trade in ACTIVE_TRADES.values() 
                       if trade['user_id'] == user_id and trade['result'] is not None]

    total_plus = sum(1 for trade in user_trades if trade['result'] == 'PLUS')
    total_minus = sum(1 for trade in user_trades if trade['result'] == 'MINUS')
    
    pair_stats: Dict[str, Dict[str, int]] = {}
    for trade in user_trades:
        pair = trade['pair']
        result = trade['result']
        if pair not in pair_stats:
            pair_stats[pair] = {'PLUS': 0, 'MINUS': 0}
        pair_stats[pair][result] += 1

    return {
        'total_plus': total_plus,
        'total_minus': total_minus,
        'pair_stats': pair_stats
    }


# -------------------- FSM (Состояния) --------------------
class Form(StatesGroup):
    waiting_for_referral = State() 
    choosing_pair = State()
    choosing_timeframe = State()
    
# -------------------- Клавиатуры (БЕЗ ИЗМЕНЕНИЙ) --------------------
# [Код клавиатур из предыдущей версии]

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
    
    if is_user_authorized(user_id):
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
    user_id = query.from_user.id
    if not is_user_authorized(user_id):
        await query.answer("Сначала активируйте бота, отправив свой ID.", show_alert=True)
        return
        
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
    if not is_user_authorized(user_id):
        await query.answer("Сначала активируйте бота, отправив свой ID.", show_alert=True)
        return
        
    stats = get_user_stats(user_id)
    total_trades = stats['total_plus'] + stats['total_minus']
    
    # ... [Код статистики без изменений]
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
    
    # Удаляем кнопки после выбора
    await query.message.edit_reply_markup(reply_markup=None)
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🏠 Главное меню", callback_data="main_menu")
    
    await query.message.answer(
        f"{icon} **Результат сделки сохранен: {result}**\n\n"
        "Выберите следующее действие:",
        reply_markup=keyboard.as_markup()
    )

    await query.answer(f"Результат {result} сохранен!")
    # State остается чистым

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
    user_id = query.from_user.id
    if not is_user_authorized(user_id):
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
    if not is_user_authorized(user_id):
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
    if not is_user_authorized(user_id):
        await query.answer("Сначала активируйте бота.", show_alert=True)
        return
        
    # Блокировка concurrency: проверяем, не ждет ли бот сейчас ввода
    current_state = await state.get_state()
    if current_state == Form.choosing_timeframe:
        await state.set_state(None) # Снимаем состояние, чтобы разрешить следующие команды
    else:
        # Если вдруг пришел еще один callback, пока предыдущий обрабатывается
        await query.answer("⏳ Дождитесь завершения предыдущего запроса.", show_alert=False)
        return
        
    _, pair, tf = query.data.split(":")
    tf = int(tf)
    
    # 1. Изменяем сообщение на "Загрузка"
    await query.answer("Идет загрузка сигнала...", show_alert=False) 
    message_to_edit = await query.message.edit_text(f"Выбраны {pair} и {tf} мин. Идет загрузка сигнала...")

    try:
        # 2. Вызов асинхронной функции
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

# Оборачиваем синхронную функцию YFinance в асинхронный поток
async def async_fetch_ohlcv(symbol: str, exp_minutes: int) -> pd.DataFrame:
    def sync_fetch():
        interval = "1m"
        try:
            # yfinance не очень надежен для форекс, но оставим его для простоты.
            df = yf.download(f"{symbol}=X", period="5d", interval=interval, progress=False, show_errors=False) 
        except Exception as e:
            logging.error(f"Ошибка загрузки данных YFinance для {symbol}: {e}")
            return pd.DataFrame() 

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            return pd.DataFrame()

        df = df[required_cols] 
        df.columns = [col.lower() for col in required_cols]
        
        if exp_minutes > 1 and not df.empty:
            df = df.resample(f"{exp_minutes}min").agg({
                'open':'first','high':'max','low':'min','close':'last','volume':'sum'
            }).dropna()
            
        # Усиленная проверка данных: нужно минимум 50 свечей
        if len(df) < 50:
            logging.warning(f"Недостаточно данных ({len(df)}) для {symbol} {exp_minutes}min.")
            return pd.DataFrame()
            
        return df

    # Запускаем синхронную операцию в отдельном потоке, чтобы не блокировать event loop
    return await asyncio.to_thread(sync_fetch)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Расчет индикаторов
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
    
    # Проверка на наличие критических колонок после расчета
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
    # Проверка на тренд (ADX не используется, так как он иногда требует слишком много данных)
    
    # 1. EMA (Trend)
    if latest['ema9'] > latest['ema21'] and latest['close'] > latest['ema21']:
        score += 1 # UP Trend
    elif latest['ema9'] < latest['ema21'] and latest['close'] < latest['ema21']:
        score -= 1 # DOWN Trend
    
    # 2. RSI (Momentum/Overbought/Oversold)
    if latest['rsi14'] < 30: score += 1 # Oversold -> Buy
    if latest['rsi14'] > 70: score -= 1 # Overbought -> Sell

    # 3. MACD (Momentum cross)
    if latest['macd'] > latest['macd_signal'] and latest['macd'] < 0:
        score += 1 # Buy signal from momentum shift
    elif latest['macd'] < latest['macd_signal'] and latest['macd'] > 0:
        score -= 1 # Sell signal from momentum shift
    
    # 4. Stochastics (Reversal/Overbought/Oversold)
    if latest['stoch_k'] < 20 and latest['stoch_k'] > latest['stoch_d']:
        score += 1 # Strong Buy
    if latest['stoch_k'] > 80 and latest['stoch_k'] < latest['stoch_d']:
        score -= 1 # Strong Sell
            
    if score >= 2:
        direction = "BUY"
    elif score <= -2:
        direction = "SELL"
    else:
        direction = "HOLD" 

    # Простая уверенность, основанная на силе скоринга
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
    
    # Сохраняем сделку в In-Memory DB
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

# -------------------- БЛОК ЗАПУСКА WEBHOOK (УЛУЧШЕННЫЙ) --------------------

async def on_startup_webhook(bot: Bot):
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
        if WEBHOOK_URL:
            # Устанавливаем полный путь с токеном
            await bot(SetWebhook(url=WEBHOOK_URL)) 
            logging.info(f"✅ Webhook успешно переустановлен: {WEBHOOK_URL}")
        else:
            logging.error("❌ Webhook URL не определен.")
    except Exception as e:
        logging.error(f"Ошибка в on_startup_webhook: {e}")

async def on_shutdown_webhook(bot: Bot):
    try:
        await bot(DeleteWebhook(drop_pending_updates=True))
    except Exception as e:
        logging.error(f"Ошибка при удалении Webhook: {e}")
    logging.info("❌ Webhook удален.")


async def start_webhook():
    dp.startup.register(on_startup_webhook)
    dp.shutdown.register(on_shutdown_webhook)
    
    app = web.Application()
    
    # Используем WEBHOOK_BASE_PATH = "/webhook" для aiohttp роутера
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
