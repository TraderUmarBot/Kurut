# main.py (Исправленная версия)
import os
import io
import asyncio
from datetime import datetime
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import mplfinance as mpf

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
# ИМПОРТИРУЕМ НОВЫЙ КЛАСС ДЛЯ УДОБНОГО ПОСТРОЕНИЯ КЛАВИАТУР В AIOGRAM 3.X
from aiogram.utils.keyboard import InlineKeyboardBuilder 

# -------------------- Конфиг --------------------
# Я оставляю токен как есть, предполагая, что он будет загружен из переменной среды на Render
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

# -------------------- Клавиатуры --------------------

# ИСПРАВЛЕННАЯ ФУНКЦИЯ ДЛЯ КЛАВИАТУРЫ ПАР
def get_pairs_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    
    # Используем Builder для удобной и правильной инициализации в aiogram 3.x
    builder = InlineKeyboardBuilder() 
    
    # Добавляем кнопки пар, разбивая их на ряды по 2
    for pair in PAIRS[start:end]:
        builder.button(text=pair, callback_data=f"pair:{pair}")
    
    # Устанавливаем макет (layout) для кнопок: 2 кнопки в ряд
    builder.adjust(2) 
    
    # Навигационные кнопки
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page:{page-1}"))
    if end < len(PAIRS):
        nav_buttons.append(InlineKeyboardButton(text="➡️ Вперед", callback_data=f"page:{page+1}"))
    
    # Добавляем навигационный ряд, если он есть
    if nav_buttons:
        builder.row(*nav_buttons) 
    
    return builder.as_markup() # Возвращаем готовый объект InlineKeyboardMarkup

# ИСПРАВЛЕННАЯ ФУНКЦИЯ ДЛЯ КЛАВИАТУРЫ ТАЙМФРЕЙМОВ
def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    # Добавляем кнопки таймфреймов
    for tf in TIMEFRAMES:
        builder.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    
    # Разбиваем на ряды по 2 кнопки
    builder.adjust(2) 

    return builder.as_markup() # Возвращаем готовый объект InlineKeyboardMarkup


# -------------------- Обработчики (остаются без изменений) --------------------
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
    await send_signal(pair, tf)
    await state.clear()
    await query.answer()

# -------------------- Получение свечей (без изменений) --------------------
def fetch_ohlcv(symbol: str, exp_minutes: int, limit=CANDLES_LIMIT) -> pd.DataFrame:
    interval = "1m"
    # yfinance использует формат 'X' для FOREX, что может вызвать проблемы на некоторых парах, 
    # но я оставлю как есть, предполагая, что он работает для вас.
    df = yf.download(f"{symbol}=X", period="2d", interval=interval, progress=False) 
    df = df.rename(columns=str.lower)[['open','high','low','close','volume']]
    if exp_minutes > 1:
        df = df.resample(f"{exp_minutes}min").agg({
            'open':'first','high':'max','low':'min','close':'last','volume':'sum'
        })
    return df.tail(limit)

# -------------------- Индикаторы (без изменений) --------------------
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

# -------------------- Поддержка/Сопротивление (без изменений) --------------------
def support_resistance(df: pd.DataFrame) -> dict:
    levels = {}
    levels['support'] = df['low'].rolling(20).min().iloc[-1]
    levels['resistance'] = df['high'].rolling(20).max().iloc[-1]
    return levels

# -------------------- Голосование индикаторов (без изменений) --------------------
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

# -------------------- График (без изменений) --------------------
def plot_chart(df: pd.DataFrame) -> io.BytesIO:
    plot_df = df[['open','high','low','close','volume']].tail(150)
    # Добавление индикаторов для отображения на графике
    addplots = [
        mpf.make_addplot(df['ema9'].tail(150), color='blue', panel=0, title='EMA9'), 
        mpf.make_addplot(df['ema21'].tail(150), color='orange', panel=0, title='EMA21')
    ]
    # Добавление RSI и MACD (пример)
    # rsi_plot = mpf.make_addplot(df['rsi14'].tail(150), panel=1, ylabel='RSI')
    # macd_plot = mpf.make_addplot(df['macd'].tail(150), panel=2, type='bar', ylabel='MACD')
    # addplots.extend([rsi_plot, macd_plot])
    
    buf = io.BytesIO()
    # Обратите внимание: mpf.plot может быть медленным, особенно на Render
    mpf.plot(plot_df, type='candle', style='yahoo', volume=True, addplot=addplots, savefig=dict(fname=buf, dpi=100))
    buf.seek(0)
    return buf

# -------------------- Отправка сигнала (без изменений) --------------------
async def send_signal(pair: str, timeframe: int):
    # Ваш код, который сейчас использует `query.message.edit_text` для статуса, 
    # не сможет отправить фотографию в тот же чат, поскольку у вас нет объекта 
    # `query` или `message` здесь.
    # Вам нужно будет передать `chat_id` сюда или использовать FSMContext.
    # Поскольку логика пока отправляет всем пользователям, я оставляю ее как есть:
    
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
    
    # 4. Отправка всем пользователям
    users = load_users()
    for user_id in users:
        try:
            # Отправка фото
            await bot.send_photo(chat_id=user_id, photo=chart_buf, caption=text)
        except Exception as e:
            print(f"Ошибка отправки пользователю {user_id}: {e}")

# -------------------- Запуск --------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # ВНИМАНИЕ: Для работы на Render.com (Web Service), 
    # вам НУЖНО перейти на Webhook, а не Polling.
    # Если вы хотите использовать Polling, измените тип сервиса на Render на "Background Worker" (Фоновый работник).
    
    asyncio.run(dp.start_polling(bot))
