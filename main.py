# advanced_trading_bot.py
import os
import io
import asyncio
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import mplfinance as mpf
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

TG_TOKEN = os.getenv("TG_TOKEN") or "ВАШ_TELEGRAM_TOKEN"

PAIRS = [
    "EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF",
    "EURJPY","GBPJPY","AUDJPY","EURGBP","EURAUD","GBPAUD",
    "CADJPY","CHFJPY","EURCAD","GBPCAD","AUDCAD","AUDCHF","CADCHF"
]
PAIRS_PER_PAGE = 6
TIMEFRAMES = [1,3,5,10]
CANDLES_LIMIT = 500

bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# -------------------- Хранилище выбранных пар --------------------
USER_SELECTIONS = {}  # user_id: {"pairs": [], "timeframes": []}

# -------------------- Кнопки --------------------
def get_pairs_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    start = page * PAIRS_PER_PAGE
    end = start + PAIRS_PER_PAGE
    for pair in PAIRS[start:end]:
        kb.add(InlineKeyboardButton(pair, callback_data=f"pair:{pair}"))
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"page:{page-1}"))
    if end < len(PAIRS):
        buttons.append(InlineKeyboardButton("➡️ Вперед", callback_data=f"page:{page+1}"))
    if buttons:
        kb.row(*buttons)
    return kb

def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for tf in TIMEFRAMES:
        kb.add(InlineKeyboardButton(f"{tf} мин", callback_data=f"tf:{pair}:{tf}"))
    return kb

# -------------------- Получение свечей --------------------
def fetch_ohlcv(symbol: str, exp_minutes: int) -> pd.DataFrame:
    df = yf.download(symbol+"=X", period="2d", interval="1m", progress=False)
    df = df.rename(columns=str.lower)[['open','high','low','close','volume']]
    if exp_minutes > 1:
        rule = f"{exp_minutes}min"
        df = df.resample(rule).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
    return df.tail(CANDLES_LIMIT)

# -------------------- Индикаторы и свечные паттерны --------------------
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
    return {
        'support': df['low'].rolling(20).min().iloc[-1],
        'resistance': df['high'].rolling(20).max().iloc[-1]
    }

# -------------------- Голосование индикаторов --------------------
def indicator_vote(latest: pd.Series) -> dict:
    score = 0
    # EMA
    score += 1 if latest['ema9']>latest['ema21'] else -1
    # RSI
    score += 1 if latest['rsi14']<30 else (-1 if latest['rsi14']>70 else 0)
    # MACD
    score += 1 if latest['macd']>latest['macd_signal'] else -1
    # Свечные паттерны
    score += 1 if latest['hammer'] else 0
    score -= 1 if latest['shooting_star'] else 0
    direction = "BUY" if score>0 else ("SELL" if score<0 else "HOLD")
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

# -------------------- Обработчики --------------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Выбери валютную пару:", reply_markup=get_pairs_keyboard(0))

@dp.callback_query(lambda c: c.data.startswith("page:") or c.data.startswith("pair:"))
async def choose_pair(callback: types.CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id
    if user_id not in USER_SELECTIONS:
        USER_SELECTIONS[user_id] = {"pairs": [], "timeframes": []}

    if data.startswith("page:"):
        page = int(data.split(":")[1])
        await callback.message.edit_reply_markup(reply_markup=get_pairs_keyboard(page))
    elif data.startswith("pair:"):
        pair = data.split(":")[1]
        USER_SELECTIONS[user_id]["pairs"].append(pair)
        await callback.message.edit_text(f"Выбрана пара: {pair}\nВыбери таймфрейм:", reply_markup=get_timeframes_keyboard(pair))

@dp.callback_query(lambda c: c.data.startswith("tf:"))
async def choose_timeframe(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    _, pair, tf = callback.data.split(":")
    tf = int(tf)
    USER_SELECTIONS[user_id]["timeframes"].append((pair, tf))
    await callback.message.edit_text(f"Добавлено отслеживание {pair} {tf} мин.\nБот будет присылать сигналы автоматически.")

# -------------------- Авто-обновление сигналов --------------------
async def auto_signals():
    while True:
        for user_id, sel in USER_SELECTIONS.items():
            for pair, tf in sel["timeframes"]:
                try:
                    df = fetch_ohlcv(pair, tf)
                    df_ind = compute_indicators(df)
                    latest = df_ind.iloc[-1]
                    vote = indicator_vote(latest)
                    sr = support_resistance(df_ind)
                    chart = plot_chart(df_ind)
                    dir_map = {"BUY":"🔺 ПОКУПКА","SELL":"🔻 ПРОДАЖА","HOLD":"⚠️ НЕОДНОЗНАЧНО"}
                    text = (
                        f"📊 Сигнал\nПара: {pair}\nТаймфрейм: {tf} мин\n"
                        f"Направление: {dir_map[vote['direction']]}\nУверенность: {vote['confidence']}%\n"
                        f"Поддержка: {sr['support']:.5f}\nСопротивление: {sr['resistance']:.5f}"
                    )
                    await bot.send_photo(user_id, photo=chart, caption=text)
                except Exception as e:
                    print(f"Ошибка при отправке сигналов {pair} {tf} для {user_id}: {e}")
        await asyncio.sleep(60)  # проверка раз в минуту

# -------------------- Запуск --------------------
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(auto_signals())
    loop.run_until_complete(dp.start_polling(bot))
