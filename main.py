# main.py
import os
import io
import asyncio
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import mplfinance as mpf
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor

# -------------------- Конфиг --------------------
TG_TOKEN = os.getenv("TG_TOKEN") or "ВАШ_TELEGRAM_TOKEN"
CANDLES_LIMIT = 500

PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
TIMEFRAMES = [1, 3, 5, 10]  # минуты

USERS_FILE = "users.txt"

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(bot)

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

# -------------------- Telegram Handlers --------------------
@dp.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    save_user(message.from_user.id)
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    for pair in PAIRS:
        keyboard.add(KeyboardButton(pair))
    for tf in TIMEFRAMES:
        keyboard.add(KeyboardButton(f"{tf} мин"))
    await message.reply(
        "Привет! Я твой торговый помощник 🤖\nВыбери валютную пару и таймфрейм, и я пришлю торговый сигнал.",
        reply_markup=keyboard
    )

# -------------------- Получение свечей --------------------
def fetch_ohlcv(symbol: str, minutes: int, limit=CANDLES_LIMIT) -> pd.DataFrame:
    df = yf.download(symbol, period="2d", interval="1m", progress=False)
    df = df.rename(columns=str.lower)[['open','high','low','close','volume']]
    if minutes > 1:
        rule = f"{minutes}min"
        df = df.resample(rule).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
    return df.tail(limit)

# -------------------- Индикаторы --------------------
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Трендовые
    df['ema9'] = ta.ema(df['close'], length=9)
    df['ema21'] = ta.ema(df['close'], length=21)
    df['sma50'] = ta.sma(df['close'], length=50)
    macd = ta.macd(df['close'])
    df['macd'] = macd['MACD_12_26_9']
    df['macd_signal'] = macd['MACDs_12_26_9']
    # Осцилляторы
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
    # Свечные паттерны
    df['hammer'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['close']-df['low'])/(.001+df['high']-df['low'])>0.6)
    df['shooting_star'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['high']-df['close'])/(.001+df['high']-df['low'])>0.6)
    df['doji'] = abs(df['close']-df['open']) < 0.1*(df['high']-df['low'])
    return df

# -------------------- Поддержка/Сопротивление --------------------
def support_resistance(df: pd.DataFrame) -> dict:
    return {
        'support': df['low'].rolling(20).min().iloc[-1],
        'resistance': df['high'].rolling(20).max().iloc[-1]
    }

# -------------------- Голосование индикаторов --------------------
def indicator_vote(latest: pd.Series) -> dict:
    score = 0
    if latest['ema9'] > latest['ema21']:
        score += 1
    else:
        score -=1
    if latest['rsi14'] < 30: score += 1
    elif latest['rsi14'] > 70: score -=1
    if latest['hammer']: score += 1
    if latest['shooting_star']: score -=1
    if latest['doji']: score += 0  # нейтрально
    direction = "BUY" if score > 0 else ("SELL" if score < 0 else "HOLD")
    confidence = min(100, abs(score)*20 + 40)
    return {"direction": direction, "confidence": confidence}

# -------------------- График --------------------
def plot_chart(df: pd.DataFrame) -> io.BytesIO:
    plot_df = df[['open','high','low','close','volume']].tail(150)
    addplots = [mpf.make_addplot(df['ema9'].tail(150)), mpf.make_addplot(df['ema21'].tail(150))]
    buf = io.BytesIO()
    mpf.plot(plot_df, type='candle', style='yahoo', volume=True, addplot=addplots, savefig=buf)
    buf.seek(0)
    return buf

# -------------------- Отправка сигналов --------------------
async def send_signal(pair, timeframe, chat_id):
    df = fetch_ohlcv(pair, timeframe)
    df_ind = compute_indicators(df)
    latest = df_ind.iloc[-1]
    vote = indicator_vote(latest)
    sr = support_resistance(df_ind)
    chart_buf = plot_chart(df_ind)
    dir_map = {"BUY":"🔺 ПОКУПКА","SELL":"🔻 ПРОДАЖА","HOLD":"⚠️ НЕОДНОЗНАЧНО"}
    text = (
        f"📊 Сигнал\nПара: {pair}\nТаймфрейм: {timeframe} мин\n"
        f"Направление: {dir_map[vote['direction']]}\nУверенность: {vote['confidence']}%\n"
        f"Поддержка: {sr['support']:.5f}\nСопротивление: {sr['resistance']:.5f}"
    )
    await bot.send_photo(chat_id=chat_id, photo=chart_buf, caption=text)

# -------------------- Обработка выбора пользователя --------------------
user_selection = {}

@dp.message_handler()
async def handle_selection(message: types.Message):
    user_id = message.from_user.id
    text = message.text.strip()
    if text in PAIRS:
        user_selection[user_id] = {"pair": text}
        await message.reply("Вы выбрали пару. Теперь выберите таймфрейм (1,3,5,10 мин).")
    elif text.replace(" мин","").isdigit():
        if user_id in user_selection:
            tf = int(text.replace(" мин",""))
            user_selection[user_id]["timeframe"] = tf
            pair = user_selection[user_id]["pair"]
            await message.reply(f"Отправляю сигнал для {pair} на {tf} мин...")
            await send_signal(pair, tf, user_id)
        else:
            await message.reply("Сначала выберите валютную пару.")
    else:
        await message.reply("Выберите валютную пару или таймфрейм с клавиатуры.")

# -------------------- Запуск --------------------
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
