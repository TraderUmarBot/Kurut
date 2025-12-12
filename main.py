# main.py
import os
import io
import asyncio
from datetime import datetime
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import mplfinance as mpf

# --- Импорты aiogram и aiohttp ---
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder # <-- Исправление Pydantic ошибки
# from aiohttp import web # aiogram 3.x может запустить Aiohttp через dp.run_app

# -------------------- Конфиг --------------------
# Используйте переменные окружения Render
TG_TOKEN = os.getenv("TG_TOKEN") or "ВАШ_TELEGRAM_TOKEN"
CANDLES_LIMIT = 500

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
]

TIMEFRAMES = [1, 3, 5, 10]  # минуты
PAIRS_PER_PAGE = 6

USERS_FILE = "users.txt"

# -------------------- Конфиг Webhook для Render --------------------

# Render автоматически устанавливает PORT. Нам нужно его использовать.
WEB_SERVER_HOST = "0.0.0.0"
WEB_SERVER_PORT = int(os.environ.get("PORT", 8080))

# ВАЖНО: Установите эту переменную окружения (WEBHOOK_URL) на Render
# Пример: https://your-service-name.onrender.com
BASE_WEBHOOK_URL = os.environ.get("WEBHOOK_URL") 
if not BASE_WEBHOOK_URL:
    print("!!! ВНИМАНИЕ: Переменная WEBHOOK_URL не установлена. Замените заглушку в коде. !!!")
    BASE_WEBHOOK_URL = "https://<ЗДЕСЬ_ВАШ_URL_RENDER>.onrender.com" 

WEBHOOK_PATH = f"/webhook/{TG_TOKEN}"
WEBHOOK_URL = BASE_WEBHOOK_URL + WEBHOOK_PATH

# -------------------- Бот и диспетчер --------------------
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# -------------------- FSM --------------------
class Form(StatesGroup):
    choosing_pair = State()
    choosing_timeframe = State()

# -------------------- Пользователи --------------------
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
        with open(USERS_FILE, "w") as f:
            for u in users:
                f.write(f"{u}\n")

# -------------------- Клавиатуры (Исправлено) --------------------
def get_pairs_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    
    # Используем Builder для корректной валидации Pydantic
    builder = InlineKeyboardBuilder() 
    
    # Добавляем кнопки пар, разбивая их на ряды по 2
    for pair in PAIRS[start:end]:
        builder.button(text=pair, callback_data=f"pair:{pair}")
    
    builder.adjust(2) # Устанавливаем макет: 2 кнопки в ряд
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page:{page-1}"))
    if end < len(PAIRS):
        nav_buttons.append(InlineKeyboardButton(text="➡️ Вперед", callback_data=f"page:{page+1}"))
    
    if nav_buttons:
        builder.row(*nav_buttons) 
    
    return builder.as_markup() # Возвращаем готовый объект InlineKeyboardMarkup

def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    # Используем Builder для корректной валидации Pydantic
    builder = InlineKeyboardBuilder()
    
    # Добавляем кнопки таймфреймов
    for tf in TIMEFRAMES:
        builder.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    
    builder.adjust(2) # Разбиваем на ряды по 2 кнопки

    return builder.as_markup()

# -------------------- Обработчики --------------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    save_user(message.from_user.id)
    await state.set_state(Form.choosing_pair)
    await message.answer(
        "Привет! Выбери валютную пару:",
        reply_markup=get_pairs_keyboard(0)
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
    await Form.choosing_timeframe.set()
    await query.message.edit_text(
        f"Выбрана пара {pair}. Теперь выбери таймфрейм:",
        reply_markup=get_timeframes_keyboard(pair)
    )
    await query.answer()

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def tf_handler(query: types.CallbackQuery, state: FSMContext):
    _, pair, tf = query.data.split(":")
    tf = int(tf)
    await query.message.edit_text(f"Выбраны {pair} и {tf} мин. Идет загрузка сигнала...")
    
    # Передаем chat_id, чтобы функция send_signal могла редактировать сообщение после загрузки
    await send_signal(pair, tf, query.message.chat.id, query.message.message_id)
    await state.clear()
    await query.answer()

# -------------------- Получение свечей --------------------
def fetch_ohlcv(symbol: str, exp_minutes: int, limit=CANDLES_LIMIT) -> pd.DataFrame:
    interval = "1m"
    df = yf.download(f"{symbol}=X", period="2d", interval=interval, progress=False)
    df = df.rename(columns=str.lower)[['open','high','low','close','volume']]
    if exp_minutes > 1:
        df = df.resample(f"{exp_minutes}min").agg({
            'open':'first','high':'max','low':'min','close':'last','volume':'sum'
        })
    return df.tail(limit)

# -------------------- Индикаторы --------------------
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
    bb = ta.bbands(df['close'])
    df['bb_upper'] = bb['BBU_20_2.0']
    df['bb_lower'] = bb['BBL_20_2.0']
    df['atr14'] = ta.atr(df['high'], df['low'], df['close'])
    df['adx14'] = ta.adx(df['high'], df['low'], df['close'])['ADX_14']
    df['cci20'] = ta.cci(df['high'], df['low'], df['close'], length=20)
    df['obv'] = ta.obv(df['close'], df['volume'])
    df['mom10'] = ta.mom(df['close'], length=10)
    # свечные паттерны
    df['hammer'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['close']-df['low'])/(.001+df['high']-df['low'])>0.6)
    df['shooting_star'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['high']-df['close'])/(.001+df['high']-df['low'])>0.6)
    return df

# -------------------- Поддержка/Сопротивление --------------------
def support_resistance(df: pd.DataFrame) -> dict:
    levels = {}
    levels['support'] = df['low'].rolling(20).min().iloc[-1]
    levels['resistance'] = df['high'].rolling(20).max().iloc[-1]
    return levels

# -------------------- Голосование индикаторов --------------------
def indicator_vote(latest: pd.Series) -> dict:
    score = 0
    if latest['ema9'] > latest['ema21']: score += 1
    else: score -=1
    if latest['rsi14'] < 30: score += 1
    elif latest['rsi14'] > 70: score -=1
    if latest['hammer']: score += 1
    if latest['shooting_star']: score -=1
    direction = "BUY" if score > 0 else ("SELL" if score < 0 else "HOLD")
    confidence = min(100, abs(score)*20 + 40)
    return {"direction": direction, "confidence": confidence}

# -------------------- График --------------------
def plot_chart(df: pd.DataFrame) -> io.BytesIO:
    plot_df = df[['open','high','low','close','volume']].tail(150)
    addplots = [mpf.make_addplot(df['ema9'].tail(150)), mpf.make_addplot(df['ema21'].tail(150))]
    buf = io.BytesIO()
    # Обратите внимание на аргументы savefig для корректного сохранения в буфер
    mpf.plot(plot_df, type='candle', style='yahoo', volume=True, addplot=addplots, savefig=dict(fname=buf, dpi=100))
    buf.seek(0)
    return buf

# -------------------- Отправка сигнала (Изменено) --------------------
async def send_signal(pair: str, timeframe: int, chat_id: int, message_id: int):
    # 1. Загрузка данных
    df = fetch_ohlcv(pair, timeframe)
    df_ind = compute_indicators(df)
    latest = df_ind.iloc[-1]
    
    # 2. Анализ
    res = indicator_vote(latest)
    sr = support_resistance(df_ind)
    chart_buf = plot_chart(df_ind)
    
    # 3. Формирование текста
    dir_map = {"BUY":"🔺 ПОКУПКА","SELL":"🔻 ПРОДАЖА","HOLD":"⚠️ НЕОДНОЗНАЧНО"}
    text = (
        f"📊 Сигнал\nПара: {pair}\nТаймфрейм: {timeframe} мин\n"
        f"Направление: {dir_map[res['direction']]}\nУверенность: {res['confidence']}%\n"
        f"Поддержка: {sr['support']:.5f}\nСопротивление: {sr['resistance']:.5f}"
    )
    
    # 4. Отправка пользователю, который запросил сигнал (редактируем предыдущее сообщение)
    try:
        await bot.edit_message_caption(
            chat_id=chat_id, 
            message_id=message_id, 
            caption="Готовлю график..." # Сначала редактируем текст, так как нельзя редактировать фото
        )
        # Отправляем новое сообщение с фото, т.к. edit_message_photo сложнее
        await bot.send_photo(
            chat_id=chat_id, 
            photo=chart_buf, 
            caption=text
        )
        await bot.delete_message(chat_id=chat_id, message_id=message_id) # Удаляем сообщение "Идет загрузка сигнала..."
    except Exception as e:
        print(f"Ошибка при отправке сигнала пользователю {chat_id}: {e}")

    # 5. Отправка всем остальным подписчикам (если нужно)
    users = load_users()
    for user_id in users:
        if user_id != chat_id:
            try:
                # Сброс буфера для каждого отправляемого сообщения
                chart_buf.seek(0) 
                await bot.send_photo(chat_id=user_id, photo=chart_buf, caption=text)
            except Exception as e:
                print(f"Ошибка отправки пользователю {user_id}: {e}")


# -------------------- Запуск Webhook --------------------

async def on_startup_webhook(bot: Bot):
    """
    Вызывается при старте Aiohttp сервера. Устанавливает Webhook URL в Telegram.
    """
    print("--- ЗАПУСК WEBHOOK ---")
    if not BASE_WEBHOOK_URL or 'your-service-name' in BASE_WEBHOOK_URL:
        raise ValueError("Ошибка: WEBHOOK_URL не настроен корректно. Проверьте переменную окружения на Render.")
    
    print(f"Установка Webhook URL: {WEBHOOK_URL}")
    await bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)

async def on_shutdown_webhook(bot: Bot):
    """
    Вызывается при остановке Aiohttp сервера. Удаляет Webhook URL.
    """
    print("--- ОСТАНОВКА WEBHOOK ---")
    await bot.delete_webhook()

def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    # 1. Добавление функций старта/завершения к диспетчеру
    dp.startup.register(on_startup_webhook)
    dp.shutdown.register(on_shutdown_webhook)

    # 2. Запуск Aiohttp сервера через aiogram
    # aiogram 3.x автоматически создает web.Application и привязывает к нему диспетчер
    print(f"Сервер запускается на {WEB_SERVER_HOST}:{WEB_SERVER_PORT} с путем {WEBHOOK_PATH}")
    dp.run_app(
        host=WEB_SERVER_HOST,
        port=WEB_SERVER_PORT,
        path=WEBHOOK_PATH,
        # session=AiohttpSession() # Можно добавить, если нужна кастомная aiohttp сессия
    )

if __name__ == "__main__":
    main()
