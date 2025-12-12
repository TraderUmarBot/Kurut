# main.py - ФИНАЛЬНЫЙ, УСТОЙЧИВЫЙ КОД (Polling Mode)

import os
import asyncio
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import logging

# --- Импорты aiogram ---
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder 

# -------------------- Конфиг --------------------
TG_TOKEN = os.getenv("TG_TOKEN") or "ВАШ_ТЕЛЕГРАМ_ТОКЕН" 
# CANDLES_LIMIT удален для получения максимального количества данных за 5d.

PO_REFERRAL_LINK = "https://m.po-tck.com/ru/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START" 

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF",
    "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD", "GBPAUD",
    "CADJPY", "CHFJPY", "EURCAD", "GBPCAD", "AUDCAD", "AUDCHF", "CADCHF"
]

TIMEFRAMES = [1, 3, 5, 10]
PAIRS_PER_PAGE = 6

USERS_FILE = "users.txt"

# -------------------- Бот и диспетчер --------------------
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# -------------------- FSM --------------------
class Form(StatesGroup):
    waiting_for_referral = State() 
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
        with open(USERS_FILE, "a") as f: 
            f.write(f"{user_id}\n")

# -------------------- Клавиатуры (без изменений) --------------------
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
    
    return builder.as_markup()

def get_timeframes_keyboard(pair: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for tf in TIMEFRAMES:
        builder.button(text=f"{tf} мин", callback_data=f"tf:{pair}:{tf}")
    builder.adjust(2) 
    return builder.as_markup()


# -------------------- Обработчики (без изменений) --------------------

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id in load_users():
        await state.set_state(Form.choosing_pair)
        await message.answer(
            "С возвращением! Выбери валютную пару:",
            reply_markup=get_pairs_keyboard(0)
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
        
        await message.answer(
            referral_text,
            parse_mode="Markdown"
        )


@dp.message(Form.waiting_for_referral)
async def process_referral_check(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    user_id = message.from_user.id

    is_valid = False
    
    if user_input.isdigit() and len(user_input) > 4:
        is_valid = True
    
    
    if is_valid:
        save_user(user_id) 
        await state.set_state(Form.choosing_pair)
        
        await message.answer(
            "✅ **Активация успешна!**\nСпасибо за регистрацию. Теперь вы можете получать торговые сигналы.\n\n"
            "Выбери валютную пару:",
            reply_markup=get_pairs_keyboard(0),
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "❌ **Ошибка активации.**\n"
            "Пожалуйста, убедитесь, что вы прислали свой **ID аккаунта** (только цифры), "
            "или попробуйте снова."
        )


@dp.callback_query(Form.choosing_pair, lambda c: c.data.startswith("page:"))
async def page_handler(query: types.CallbackQuery, state: FSMContext):
    page = int(query.data.split(":")[1])
    await query.message.edit_text(
        "Выбери валютную пару:",
        reply_markup=get_pairs_keyboard(page)
    )
    await query.answer()

@dp.callback_query(Form.choosing_pair, lambda c: c.data.startswith("pair:"))
async def pair_handler(query: types.CallbackQuery, state: FSMContext):
    pair = query.data.split(":")[1]
    await state.update_data(selected_pair=pair)
    
    await state.set_state(Form.choosing_timeframe) 
    
    await query.message.edit_text(
        f"Выбрана пара {pair}. Теперь выбери таймфрейм:",
        reply_markup=get_timeframes_keyboard(pair)
    )
    await query.answer()

@dp.callback_query(Form.choosing_timeframe, lambda c: c.data.startswith("tf:"))
async def tf_handler(query: types.CallbackQuery, state: FSMContext):
    _, pair, tf = query.data.split(":")
    tf = int(tf)
    
    await query.answer("Идет загрузка сигнала...", show_alert=False) 
    message_to_edit = await query.message.edit_text(f"Выбраны {pair} и {tf} мин. Идет загрузка сигнала...")

    try:
        await send_signal(pair, tf, message_to_edit.chat.id, message_to_edit.message_id)
    except Exception as e:
        error_text = f"❌ **Критическая ошибка.** Не удалось обработать сигнал. Пожалуйста, попробуйте снова или выберите другую пару."
        await bot.edit_message_text(
            chat_id=message_to_edit.chat.id, 
            message_id=message_to_edit.message_id, 
            text=error_text, 
            parse_mode="Markdown"
        )
        logging.error(f"Критическая ошибка в tf_handler: {e}")
        
    await state.clear()


# -------------------- Получение свечей --------------------
def fetch_ohlcv(symbol: str, exp_minutes: int) -> pd.DataFrame:
    interval = "1m"
    try:
        # Увеличен период до 5 дней, CANDLES_LIMIT удален
        df = yf.download(f"{symbol}=X", period="5d", interval=interval, progress=False) 
    except Exception as e:
        logging.error(f"Ошибка загрузки данных YFinance для {symbol}: {e}")
        return pd.DataFrame() 

    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required_cols):
        logging.warning(f"Не все OHLCV столбцы найдены для {symbol}.")
        return pd.DataFrame()

    df = df[required_cols] 
    df.columns = [col.lower() for col in required_cols]
    
    if exp_minutes > 1 and not df.empty:
        df = df.resample(f"{exp_minutes}min").agg({
            'open':'first','high':'max','low':'min','close':'last','volume':'sum'
        }).dropna()
        
    return df

# -------------------- Индикаторы (УСТОЙЧИВАЯ ВЕРСИЯ) --------------------
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Трендовые индикаторы
    df['ema9'] = ta.ema(df['close'], length=9)
    df['ema21'] = ta.ema(df['close'], length=21)
    df['sma50'] = ta.sma(df['close'], length=50)
    
    # MACD 
    macd = ta.macd(df['close'])
    if macd is not None and 'MACD_12_26_9' in macd.columns:
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
    else:
        df['macd'] = float('nan')
        df['macd_signal'] = float('nan')
    
    # Осцилляторы
    df['rsi14'] = ta.rsi(df['close'], length=14)
    stoch = ta.stoch(df['high'], df['low'], df['close'])
    if stoch is not None and 'STOCHk_14_3_3' in stoch.columns:
        df['stoch_k'] = stoch['STOCHk_14_3_3']
        df['stoch_d'] = stoch['STOCHd_14_3_3']
    else:
        df['stoch_k'] = float('nan')
        df['stoch_d'] = float('nan')

    df['cci20'] = ta.cci(df['high'], df['low'], df['close'], length=20)
    df['mom10'] = ta.mom(df['close'], length=10)
    
    # Волатильность 
    bb = ta.bbands(df['close'])
    if bb is not None and 'BBU_20_2.0' in bb.columns: 
        df['bb_upper'] = bb['BBU_20_2.0']
        df['bb_lower'] = bb['BBL_20_2.0']
    else:
        df['bb_upper'] = float('nan')
        df['bb_lower'] = float('nan')
        
    # ATR и ADX 
    try:
        adx_df = ta.adx(df['high'], df['low'], df['close'])
        df['atr14'] = ta.atr(df['high'], df['low'], df['close'])
        df['adx14'] = adx_df['ADX_14'] if 'ADX_14' in adx_df.columns else float('nan')
    except Exception:
        df['atr14'] = float('nan')
        df['adx14'] = float('nan')

    df['obv'] = ta.obv(df['close'], df['volume'])
    
    # Свечные паттерны
    df['hammer'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['close']-df['low'])/(.001+df['high']-df['low'])>0.6)
    df['shooting_star'] = ((df['high']-df['low'])>3*(df['open']-df['close'])) & ((df['high']-df['close'])/(.001+df['high']-df['low'])>0.6)
    
    # ИСПРАВЛЕНИЕ: Вместо df.dropna() удаляем NaN только в ключевых столбцах.
    critical_cols = ['ema9', 'ema21', 'macd', 'rsi14', 'stoch_k', 'adx14']
    df_cleaned = df.dropna(subset=critical_cols)
    
    # Возвращаем последние 100 строк для анализа
    return df_cleaned.tail(100)

# -------------------- Поддержка/Сопротивление (без изменений) --------------------
def support_resistance(df: pd.DataFrame) -> dict:
    levels = {}
    # Используем последние 20 свечей для расчета S/R
    df_sr = df.tail(20) 
    if not df_sr.empty:
        levels['support'] = df_sr['low'].min()
        levels['resistance'] = df_sr['high'].max()
    else:
        levels['support'] = float('nan')
        levels['resistance'] = float('nan')
    return levels

# -------------------- Голосование индикаторов (Улучшенная точность) --------------------
def indicator_vote(latest: pd.Series) -> dict:
    score = 0
    
    # --- 1. Трендовая фильтрация ---
    
    # ADX > 25 для подтверждения сильного тренда
    is_trending = latest['adx14'] > 25
    
    if is_trending:
        # Трендовое подтверждение (EMA)
        if latest['ema9'] > latest['ema21'] and latest['close'] > latest['sma50']:
            score += 2
        elif latest['ema9'] < latest['ema21'] and latest['close'] < latest['sma50']:
            score -= 2
    
    # --- 2. Комплексное подтверждение осцилляторов (RSI + Stoch) ---
    
    # СТРОГОЕ ПРАВИЛО BUY: RSI < 30 (перепроданность) И Stoch K < 20
    is_oversold = (latest['rsi14'] < 30) and (latest['stoch_k'] < 20)
    # СТРОГОЕ ПРАВИЛО SELL: RSI > 70 (перекупленность) И Stoch K > 80
    is_overbought = (latest['rsi14'] > 70) and (latest['stoch_k'] > 80)
    
    if is_oversold:
        score += 1
    
    if is_overbought:
        score -= 1


    # --- 3. Свечные паттерны ---
    
    if latest['hammer']: 
        score += 1
    if latest['shooting_star']: 
        score -= 1
            
    # --- Вывод ---
    
    # Требуем минимальный счет >= 2 или <= -2 для сигнала
    if score >= 2:
        direction = "BUY"
    elif score <= -2:
        direction = "SELL"
    else:
        direction = "HOLD"

    confidence = min(100, abs(score) * 20 + 40)
    
    return {"direction": direction, "confidence": confidence, "score": score}

# -------------------- Отправка сигнала (без изменений) --------------------
async def send_signal(pair: str, timeframe: int, chat_id: int, message_id: int):
    
    df = fetch_ohlcv(pair, timeframe)
    
    # Проверка 1: Получили ли мы достаточно данных? (После агрегации)
    if df.empty or len(df) < 50: 
        error_text = f"❌ **Ошибка.** Не удалось загрузить достаточно свечей (нужно >50) для {pair} {timeframe} мин. Попробуйте позже."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text, parse_mode="Markdown")
        return
        
    df_ind = compute_indicators(df)
    
    # Проверка 2: Получили ли мы рассчитанные индикаторы?
    if df_ind.empty:
        # Эта ошибка теперь должна возникать КРАЙНЕ РЕДКО
        error_text = f"❌ **Ошибка.** Индикаторы не рассчитаны (недостаточно полных данных после очистки). Попробуйте меньший таймфрейм."
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=error_text, parse_mode="Markdown")
        return
        
    latest = df_ind.iloc[-1]
    
    res = indicator_vote(latest)
    sr = support_resistance(df_ind)
    
    dir_map = {"BUY":"🔺 ПОКУПКА","SELL":"🔻 ПРОДАЖА","HOLD":"⚠️ НЕОДНОЗНАЧНО"}
    text = (
        f"📊 **Сигнал**\n\n"
        f"Пара: {pair}\n"
        f"Таймфрейм: {timeframe} мин\n\n"
        f"Направление: **{dir_map[res['direction']]}**\n"
        f"Уверенность: {res['confidence']}%\n\n"
        f"Поддержка: {sr['support']:.5f}\n"
        f"Сопротивление: {sr['resistance']:.5f}"
    )
    
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Ошибка при редактировании сообщения пользователю {chat_id}: {e}")

    # Отправка всем остальным подписчикам 
    users = load_users()
    for user_id in users:
        if user_id != chat_id:
            try:
                await bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown")
            except Exception as e:
                logging.error(f"Ошибка отправки пользователю {user_id}: {e}")


# -------------------- Запуск (ПОЛЛИНГ) --------------------

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("--- ЗАПУСК В РЕЖИМЕ ПОЛЛИНГА ---")
    
    if not TG_TOKEN or TG_TOKEN == "ВАШ_ТЕЛЕГРАМ_ТОКЕН":
        logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задан TG_TOKEN.")
        return

    try:
        asyncio.run(dp.start_polling(bot, drop_pending_updates=True))
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную.")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка запуска: {e}")


if __name__ == "__main__":
    main()
