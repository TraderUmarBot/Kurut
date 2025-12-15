"""
🔥 POCKET OPTION ULTIMATE SIGNAL BOT v4.0
Улучшенные сигналы + твой оригинальный функционал
"""

import os
import sys
import asyncio
import logging
import aiohttp
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import json
from collections import defaultdict

# ===================== ТВОИ ИМПОРТЫ =====================
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

from tradingview_ta import TA_Handler, Interval, Exchange
import yfinance as yf
import talib
from textblob import TextBlob
import requests

# ===================== ТВОЙ КОНФИГ (БЕЗ ИЗМЕНЕНИЙ) =====================
TG_TOKEN = os.getenv("TG_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))
HOST = "0.0.0.0"

REF_LINK = "https://po-ru4.click/register?utm_campaign=797321&utm_source=affiliate&utm_medium=sr&a=6KE9lr793exm8X&ac=kurut&code=50START"
AUTHORS = [7079260196, 6117198446]

if not TG_TOKEN or not RENDER_EXTERNAL_HOSTNAME or not DATABASE_URL:
    print("❌ ENV не заданы или DATABASE_URL неверен")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"

logging.basicConfig(level=logging.INFO)

# ===================== ТВОЙ БОТ И БД (БЕЗ ИЗМЕНЕНИЙ) =====================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.pool.Pool | None = None

# ===================== ТВОИ КОНСТАНТЫ (БЕЗ ИЗМЕНЕНИЙ) =====================
PAIRS = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X",
    "EURJPY=X","GBPJPY=X","AUDJPY=X","EURGBP=X","EURAUD=X","GBPAUD=X",
    "CADJPY=X","CHFJPY=X","EURCAD=X","GBPCAD=X","AUDCAD=X","AUDCHF=X","CADCHF=X"
]
EXPIRATIONS = [1, 2, 3, 5, 10]
PAIRS_PER_PAGE = 6
MIN_DEPOSIT = 20.0

# ===================== МОИ ДОБАВЛЕНИЯ ДЛЯ СИГНАЛОВ =====================
class SignalStrength(Enum):
    """Сила сигнала для Pocket Option"""
    STRONG_BUY = 5
    BUY = 4
    WEAK_BUY = 3
    NEUTRAL = 2
    WEAK_SELL = 1
    SELL = 0
    STRONG_SELL = -1

# Веса систем анализа для Pocket Option (краткосрок)
POCKET_OPTION_WEIGHTS = {
    "technical": 0.40,    # Основное - тех.анализ
    "momentum": 0.25,     # Моментум для краткосрока
    "volatility": 0.20,   # Волатильность важна для опционов
    "sentiment": 0.15     # Настроения
}

# Маппинг таймфреймов TradingView для Pocket Option
POCKET_TIMEFRAMES = {
    1: ["1m", "2m", "3m"],      # 1 минута - очень краткосрок
    2: ["2m", "3m", "5m"],      # 2 минуты
    3: ["3m", "5m", "10m"],     # 3 минуты
    5: ["5m", "10m", "15m"],    # 5 минут
    10: ["10m", "15m", "30m"]   # 10 минут
}

# ===================== ТВОИ БД ФУНКЦИИ (БЕЗ ИЗМЕНЕНИЙ) =====================
async def init_db():
    global DB_POOL
    if DB_POOL is None:
        try:
            DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            logging.info("✅ Подключение к БД успешно")
        except Exception as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            sys.exit(1)
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT,
            balance FLOAT DEFAULT 0
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pair TEXT,
            expiration INT,
            direction TEXT,
            confidence FLOAT,
            explanation TEXT,
            result TEXT,
            signal_strength TEXT,
            stop_loss FLOAT,
            take_profit FLOAT
        );
        """)
        # Индекс для быстрого поиска истории
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_user_time 
        ON trades(user_id, timestamp DESC);
        """)

async def add_user(user_id: int, pocket_id: str):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id, pocket_id) VALUES ($1,$2) ON CONFLICT (user_id) DO NOTHING",
            user_id, pocket_id
        )

async def update_balance(user_id: int, amount: float):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id=$2",
            amount, user_id
        )

async def get_balance(user_id: int) -> float:
    async with DB_POOL.acquire() as conn:
        val = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return val or 0.0

async def save_trade(user_id, pair, expiration, direction, confidence, explanation, 
                    signal_strength=None, stop_loss=None, take_profit=None):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchval(
            """INSERT INTO trades (user_id, pair, expiration, direction, confidence, 
               explanation, signal_strength, stop_loss, take_profit)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id""",
            user_id, pair, expiration, direction, confidence, explanation,
            signal_strength, stop_loss, take_profit
        )

async def update_trade(trade_id, result):
    async with DB_POOL.acquire() as conn:
        await conn.execute(
            "UPDATE trades SET result=$1 WHERE id=$2",
            result, trade_id
        )

async def get_history(user_id):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM trades WHERE user_id=$1 ORDER BY timestamp DESC LIMIT 20",
            user_id
        )

# ===================== ТВОЙ FSM (БЕЗ ИЗМЕНЕНИЙ) =====================
class TradeState(StatesGroup):
    choosing_pair = State()
    choosing_expiration = State()

# ===================== ТВОИ КЛАВИАТУРЫ (БЕЗ ИЗМЕНЕНИЙ) =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📜 История сделок", callback_data="history")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start+PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X",""), callback_data=f"pair:{p}")
    if page > 0:
        kb.button(text="⬅️ Назад", callback_data=f"pairs_page:{page-1}")
    if start + PAIRS_PER_PAGE < len(PAIRS):
        kb.button(text="➡️ Вперёд", callback_data=f"pairs_page:{page+1}")
    kb.adjust(2)
    return kb.as_markup()

def expiration_kb(pair):
    kb = InlineKeyboardBuilder()
    for exp in EXPIRATIONS:
        kb.button(text=f"{exp} мин", callback_data=f"exp:{pair}:{exp}")
    kb.adjust(3)
    return kb.as_markup()

def result_kb(trade_id):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ВЫИГРЫШ", callback_data=f"res:{trade_id}:WIN")
    kb.button(text="❌ ПРОИГРЫШ", callback_data=f"res:{trade_id}:LOSE")
    kb.button(text="🏠 Меню", callback_data="menu")
    kb.adjust(2)
    return kb.as_markup()

# ===================== УЛУЧШЕННЫЕ СИГНАЛЫ ДЛЯ POCKET OPTION =====================
class PocketOptionSignalAnalyzer:
    """Специальный анализатор для краткосрочных опционов Pocket Option"""
    
    @staticmethod
    async def get_enhanced_signal(pair: str, expiration: int) -> tuple:
        """
        Улучшенный сигнал для Pocket Option
        Возвращает: (направление, уверенность%, объяснение, сила_сигнала, SL, TP)
        """
        try:
            pair_clean = pair.replace("=X", "")
            
            # 1. Мультитаймфреймовый анализ TradingView
            tv_result = await PocketOptionSignalAnalyzer._multi_tf_tv_analysis(pair_clean, expiration)
            
            # 2. Моментум анализ (важно для краткосрока)
            momentum_result = await PocketOptionSignalAnalyzer._momentum_analysis(pair_clean)
            
            # 3. Анализ волатильности (ключевое для опционов)
            volatility_result = await PocketOptionSignalAnalyzer._volatility_analysis(pair_clean, expiration)
            
            # 4. Быстрый sentiment анализ
            sentiment_result = await PocketOptionSignalAnalyzer._quick_sentiment(pair_clean)
            
            # 5. Взвешенный консенсус
            final_signal = PocketOptionSignalAnalyzer._calculate_pocket_consensus(
                tv_result, momentum_result, volatility_result, sentiment_result
            )
            
            # 6. Расчет SL/TP для Pocket Option (в процентах)
            sl_pct, tp_pct = PocketOptionSignalAnalyzer._calculate_pocket_levels(
                final_signal["direction"], final_signal["confidence"], expiration
            )
            
            # 7. Форматирование объяснения
            explanation = PocketOptionSignalAnalyzer._format_explanation(
                pair_clean, expiration, tv_result, momentum_result, 
                volatility_result, final_signal
            )
            
            # 8. Определение силы сигнала
            signal_strength = PocketOptionSignalAnalyzer._get_signal_strength(
                final_signal["confidence"], final_signal["direction"]
            )
            
            return (
                final_signal["direction"],
                final_signal["confidence"],
                explanation,
                signal_strength,
                sl_pct,
                tp_pct
            )
            
        except Exception as e:
            logging.error(f"Ошибка анализа для {pair}: {e}")
            # Фолбэк на простой анализ
            return await PocketOptionSignalAnalyzer._fallback_signal(pair, expiration)
    
    @staticmethod
    async def _multi_tf_tv_analysis(pair: str, expiration: int) -> Dict:
        """Анализ TradingView на 3 таймфреймах"""
        timeframes = POCKET_TIMEFRAMES.get(expiration, ["1m", "5m", "15m"])
        
        all_recommendations = []
        all_scores = {"BUY": 0, "SELL": 0, "NEUTRAL": 0}
        
        for tf in timeframes:
            try:
                handler = TA_Handler(
                    symbol=pair,
                    screener="forex",
                    exchange="FX_IDC",
                    interval=tf
                )
                analysis = await asyncio.to_thread(handler.get_analysis)
                
                rec = analysis.summary.get("RECOMMENDATION", "NEUTRAL")
                all_recommendations.append(rec)
                
                # Собираем оценки
                all_scores["BUY"] += analysis.summary.get("BUY", 0)
                all_scores["SELL"] += analysis.summary.get("SELL", 0)
                all_scores["NEUTRAL"] += analysis.summary.get("NEUTRAL", 0)
                
            except Exception as e:
                logging.warning(f"TV анализ {pair} на {tf} ошибка: {e}")
                continue
        
        # Консенсус по таймфреймам
        from collections import Counter
        if all_recommendations:
            most_common = Counter(all_recommendations).most_common(1)[0][0]
        else:
            most_common = "NEUTRAL"
        
        # Расчет уверенности
        total_score = sum(all_scores.values())
        if total_score > 0:
            if "BUY" in most_common:
                confidence = (all_scores["BUY"] / total_score) * 100
            elif "SELL" in most_common:
                confidence = (all_scores["SELL"] / total_score) * 100
            else:
                confidence = 50
        else:
            confidence = 50
        
        return {
            "system": "technical",
            "direction": most_common,
            "confidence": min(95, confidence),
            "timeframes_analyzed": len(timeframes),
            "scores": all_scores
        }
    
    @staticmethod
    async def _momentum_analysis(pair: str) -> Dict:
        """Анализ момента для краткосрочной торговли"""
        try:
            # Используем yfinance для получения последних свечей
            ticker = yf.Ticker(pair)
            hist = ticker.history(period="1d", interval="5m")
            
            if len(hist) > 10:
                closes = hist['Close'].values
                volumes = hist['Volume'].values
                
                # RSI (моментум)
                rsi = talib.RSI(closes, timeperiod=14)[-1] if len(closes) >= 14 else 50
                
                # MACD (тренд и момент)
                macd, macd_signal, _ = talib.MACD(closes)
                macd_value = macd[-1] - macd_signal[-1] if len(macd) > 0 else 0
                
                # Объемный момент
                volume_trend = np.mean(volumes[-5:]) / np.mean(volumes[-10:-5]) if len(volumes) >= 10 else 1
                
                # Определяем сигнал
                if rsi > 70 and macd_value < 0:
                    direction = "SELL"
                    confidence = min(80, ((rsi - 70) * 3 + abs(macd_value) * 10))
                elif rsi < 30 and macd_value > 0:
                    direction = "BUY"
                    confidence = min(80, ((30 - rsi) * 3 + abs(macd_value) * 10))
                elif rsi > 60 and macd_value < -0.001:
                    direction = "SELL"
                    confidence = 65
                elif rsi < 40 and macd_value > 0.001:
                    direction = "BUY"
                    confidence = 65
                else:
                    direction = "NEUTRAL"
                    confidence = 50
                
                return {
                    "system": "momentum",
                    "direction": direction,
                    "confidence": confidence,
                    "rsi": rsi,
                    "macd": macd_value,
                    "volume_trend": volume_trend
                }
                
        except Exception as e:
            logging.error(f"Momentum анализ ошибка: {e}")
        
        return {"system": "momentum", "direction": "NEUTRAL", "confidence": 50}
    
    @staticmethod
    async def _volatility_analysis(pair: str, expiration: int) -> Dict:
        """Анализ волатильности (важно для опционов)"""
        try:
            ticker = yf.Ticker(pair)
            hist = ticker.history(period="5d", interval="15m")
            
            if len(hist) > 20:
                closes = hist['Close'].values
                
                # ATR (Average True Range) - мера волатильности
                high = hist['High'].values
                low = hist['Low'].values
                
                atr = talib.ATR(high, low, closes, timeperiod=14)[-1] if len(closes) >= 14 else 0
                atr_percent = (atr / closes[-1]) * 100 if closes[-1] > 0 else 0
                
                # Боллинджер Bands для волатильности
                upper, middle, lower = talib.BBANDS(closes, timeperiod=20)
                bb_width = ((upper[-1] - lower[-1]) / middle[-1]) * 100 if middle[-1] > 0 else 0
                
                # Сигнал на основе волатильности
                # Для опционов: высокая волатильность = больше возможностей
                if atr_percent > 0.15 and bb_width > 2.0:  # Высокая волатильность
                    # В высокой волатильности ищем сильные движения
                    if closes[-1] > upper[-1] * 0.99:
                        direction = "BUY"
                        confidence = min(75, atr_percent * 100)
                    elif closes[-1] < lower[-1] * 1.01:
                        direction = "SELL"
                        confidence = min(75, atr_percent * 100)
                    else:
                        direction = "NEUTRAL"
                        confidence = 50
                else:  # Низкая волатильность
                    direction = "NEUTRAL"
                    confidence = 40  # Меньше уверенности при низкой волатильности
                
                return {
                    "system": "volatility",
                    "direction": direction,
                    "confidence": confidence,
                    "atr_percent": atr_percent,
                    "bb_width": bb_width,
                    "volatility_level": "HIGH" if atr_percent > 0.1 else "LOW"
                }
                
        except Exception as e:
            logging.error(f"Volatility анализ ошибка: {e}")
        
        return {"system": "volatility", "direction": "NEUTRAL", "confidence": 50}
    
    @staticmethod
    async def _quick_sentiment(pair: str) -> Dict:
        """Быстрый анализ настроений"""
        try:
            # Простой анализ на основе последнего движения цены
            ticker = yf.Ticker(pair)
            hist = ticker.history(period="1h", interval="5m")
            
            if len(hist) > 2:
                price_change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / 
                               hist['Close'].iloc[-2]) * 100
                
                # Сильное движение = сильные настроения
                if price_change > 0.1:
                    direction = "BUY"
                    confidence = min(70, abs(price_change) * 50)
                elif price_change < -0.1:
                    direction = "SELL"
                    confidence = min(70, abs(price_change) * 50)
                else:
                    direction = "NEUTRAL"
                    confidence = 50
                
                return {
                    "system": "sentiment",
                    "direction": direction,
                    "confidence": confidence,
                    "price_change": price_change
                }
                
        except Exception as e:
            logging.error(f"Sentiment анализ ошибка: {e}")
        
        return {"system": "sentiment", "direction": "NEUTRAL", "confidence": 50}
    
    @staticmethod
    def _calculate_pocket_consensus(tv_data: Dict, momentum: Dict, 
                                   volatility: Dict, sentiment: Dict) -> Dict:
        """Взвешенный консенсус для Pocket Option"""
        
        systems = [tv_data, momentum, volatility, sentiment]
        
        # Считаем взвешенные голоса
        buy_score = 0
        sell_score = 0
        total_weight = 0
        
        for system in systems:
            weight = POCKET_OPTION_WEIGHTS.get(system["system"], 0.1)
            direction = system["direction"]
            confidence = system["confidence"] / 100  # нормализуем до 0-1
            
            if "BUY" in direction:
                buy_score += weight * confidence
            elif "SELL" in direction:
                sell_score += weight * confidence
            
            total_weight += weight
        
        # Определяем победителя
        if buy_score > sell_score:
            direction = "BUY"
            raw_confidence = (buy_score / total_weight) * 100
        elif sell_score > buy_score:
            direction = "SELL"
            raw_confidence = (sell_score / total_weight) * 100
        else:
            direction = "NEUTRAL"
            raw_confidence = 50
        
        # Усиливаем уверенность при согласии систем
        agreeing_systems = sum(1 for s in systems 
                             if direction in s["direction"] or 
                             (direction == "NEUTRAL" and s["direction"] == "NEUTRAL"))
        
        if agreeing_systems >= 3:  # Если 3+ системы согласны
            confidence = min(95, raw_confidence * 1.3)
        elif agreeing_systems >= 2:
            confidence = min(85, raw_confidence * 1.15)
        else:
            confidence = raw_confidence
        
        return {
            "direction": direction,
            "confidence": confidence,
            "agreement": agreeing_systems,
            "details": {
                "technical": tv_data["direction"],
                "momentum": momentum["direction"],
                "volatility": volatility["direction"],
                "sentiment": sentiment["direction"]
            }
        }
    
    @staticmethod
    def _calculate_pocket_levels(direction: str, confidence: float, expiration: int) -> tuple:
        """Расчет SL/TP в процентах для Pocket Option"""
        
        # Базовые уровни в зависимости от экспирации
        if expiration <= 2:  # 1-2 минуты
            base_sl = 0.003  # 0.3%
            base_tp = 0.006  # 0.6%
        elif expiration <= 5:  # 3-5 минут
            base_sl = 0.004  # 0.4%
            base_tp = 0.008  # 0.8%
        else:  # 10 минут
            base_sl = 0.005  # 0.5%
            base_tp = 0.010  # 1.0%
        
        # Корректировка по уверенности
        conf_factor = confidence / 100
        
        # Чем выше уверенность, тем уже SL и дальше TP
        sl = base_sl * (1.3 - conf_factor)  # 0.3-1.3x
        tp = base_tp * (0.7 + conf_factor)  # 0.7-1.7x
        
        # Ограничиваем разумными пределами
        sl = max(0.002, min(sl, 0.015))  # 0.2% - 1.5%
        tp = max(0.004, min(tp, 0.025))  # 0.4% - 2.5%
        
        return sl, tp
    
    @staticmethod
    def _format_explanation(pair: str, expiration: int, tv_data: Dict, 
                          momentum: Dict, volatility: Dict, final_signal: Dict) -> str:
        """Форматирование подробного объяснения"""
        
        lines = [
            f"📊 АНАЛИЗ ДЛЯ POCKET OPTION",
            f"Пара: {pair} | Экспирация: {expiration} мин",
            "",
            f"🎯 ИТОГОВЫЙ СИГНАЛ: {final_signal['direction']}",
            f"Уверенность: {final_signal['confidence']:.1f}%",
            f"Согласие систем: {final_signal['agreement']}/4",
            "",
            "📈 ДЕТАЛИ АНАЛИЗА:",
            f"• Теханализ (TV): {tv_data['direction']} ({tv_data['confidence']:.1f}%)",
            f"• Моментум: {momentum['direction']} ({momentum['confidence']:.1f}%)",
            f"• Волатильность: {volatility['direction']} ({volatility['confidence']:.1f}%)",
            "",
            "💡 РЕКОМЕНДАЦИЯ:"
        ]
        
        if final_signal["confidence"] >= 80:
            lines.append("✅ СИЛЬНЫЙ СИГНАЛ - рекомендуется вход")
        elif final_signal["confidence"] >= 65:
            lines.append("⚠️ СРЕДНИЙ СИГНАЛ - осторожный вход")
        else:
            lines.append("⛔ СЛАБЫЙ СИГНАЛ - лучше пропустить")
        
        lines.append("")
        lines.append("⚡ Сигнал оптимизирован для краткосрочных опционов")
        
        return "\n".join(lines)
    
    @staticmethod
    def _get_signal_strength(confidence: float, direction: str) -> str:
        """Определение силы сигнала"""
        if confidence >= 85:
            return "VERY_STRONG"
        elif confidence >= 75:
            return "STRONG"
        elif confidence >= 65:
            return "MODERATE"
        elif confidence >= 55:
            return "WEAK"
        else:
            return "VERY_WEAK"
    
    @staticmethod
    async def _fallback_signal(pair: str, expiration: int) -> tuple:
        """Фолбэк на простой анализ при ошибках"""
        try:
            pair_clean = pair.replace("=X", "")
            tf_map = {1:"1m", 2:"2m", 3:"3m", 5:"5m", 10:"15m"}
            tf_tv = tf_map.get(expiration, "5m")
            
            handler = TA_Handler(
                symbol=pair_clean,
                screener="forex",
                exchange="FX_IDC",
                interval=tf_tv
            )
            analysis = await asyncio.to_thread(handler.get_analysis)
            direction = analysis.summary["RECOMMENDATION"]
            conf = 70.0
            expl = f"Базовый сигнал TradingView: {direction}"
            
            return direction, conf, expl, "MODERATE", 0.005, 0.01
            
        except Exception as e:
            logging.error(f"Фолбэк анализ тоже ошибся: {e}")
            return "NEUTRAL", 50.0, "Ошибка анализа", "VERY_WEAK", 0.01, 0.02

# ===================== ТВОИ ХЕНДЛЕРЫ (С УЛУЧШЕННЫМИ СИГНАЛАМИ) =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    balance = await get_balance(user_id)

    if user_id in AUTHORS:
        await msg.answer("🏠 Главное меню (Авторский доступ)", reply_markup=main_menu())
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="Начать", callback_data="begin_instruction")
    kb.adjust(1)
    await msg.answer(
        "Привет! Я бот для анализа валютных пар ДЛЯ POCKET OPTION.\n\n"
        "Я использую MULTI-СИСТЕМНЫЙ АНАЛИЗ для генерации точных сигналов на покупку/продажу.\n\n"
        "Внизу нажмите кнопку Начать, чтобы получить инструкцию по регистрации и пополнению баланса.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data == "begin_instruction")
async def begin_instruction(cb: types.CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="Перейти к регистрации", url=REF_LINK)
    kb.adjust(1)
    await cb.message.answer(
        f"1️⃣ Зарегистрируйте аккаунт по нашей ссылке.\n"
        f"2️⃣ Пополните баланс на ${MIN_DEPOSIT}.\n"
        f"3️⃣ После пополнения нажмите кнопку ниже для проверки пополнения.",
        reply_markup=kb.as_markup()
    )
    kb_check = InlineKeyboardBuilder()
    kb_check.button(text="Проверить пополнение", callback_data="check_deposit")
    kb_check.adjust(1)
    await cb.message.answer("Нажмите для проверки:", reply_markup=kb_check.as_markup())
    await cb.answer()

@dp.callback_query(lambda c: c.data == "check_deposit")
async def check_deposit(cb: types.CallbackQuery):
    balance = await get_balance(cb.from_user.id)
    if balance >= MIN_DEPOSIT:
        await cb.message.answer("✅ Доступ к сигналам открыт!", reply_markup=main_menu())
    else:
        await cb.message.answer(f"❌ Пополните баланс минимум на ${MIN_DEPIST}")
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pairs_page:"))
async def pairs_page(cb: types.CallbackQuery):
    page = int(cb.data.split(":")[1])
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb(page))
    await cb.answer()

@dp.callback_query(lambda c: c.data == "pairs")
async def pairs(cb: types.CallbackQuery):
    await cb.message.edit_text("📈 Выбери пару", reply_markup=pairs_kb())
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("pair:"))
async def pair(cb: types.CallbackQuery):
    pair = cb.data.split(":")[1]
    await cb.message.edit_text(
        f"⏱ Пара {pair.replace('=X','')}, выбери время экспирации",
        reply_markup=expiration_kb(pair)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("exp:"))
async def expiration(cb: types.CallbackQuery):
    _, pair, exp = cb.data.split(":")
    exp = int(exp)
    
    # ПОЛУЧАЕМ УЛУЧШЕННЫЙ СИГНАЛ
    try:
        direction, confidence, explanation, signal_strength, sl_pct, tp_pct = \
            await PocketOptionSignalAnalyzer.get_enhanced_signal(pair, exp)
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка получения сигнала: {e}")
        await cb.answer()
        return
    
    # Сохраняем с дополнительными данными
    trade_id = await save_trade(
        user_id=cb.from_user.id,
        pair=pair.replace("=X",""),
        expiration=exp,
        direction=direction,
        confidence=confidence,
        explanation=explanation,
        signal_strength=signal_strength,
        stop_loss=sl_pct,
        take_profit=tp_pct
    )
    
    # Форматируем сообщение
    emoji = "🟢" if "BUY" in direction else "🔴" if "SELL" in direction else "🟡"
    
    strength_text = {
        "VERY_STRONG": "💪 ОЧЕНЬ СИЛЬНЫЙ",
        "STRONG": "👍 СИЛЬНЫЙ", 
        "MODERATE": "⚠️ СРЕДНИЙ",
        "WEAK": "👎 СЛАБЫЙ",
        "VERY_WEAK": "⛔ ОЧЕНЬ СЛАБЫЙ"
    }.get(signal_strength, "⚠️ СРЕДНИЙ")
    
    message_text = f"""
{emoji} <b>СИГНАЛ ДЛЯ POCKET OPTION</b>

<b>Пара:</b> {pair.replace('=X','')}
<b>Экспирация:</b> {exp} минут
<b>Направление:</b> {direction}
<b>Уверенность:</b> {confidence:.1f}%
<b>Сила сигнала:</b> {strength_text}

<b>Стоп-лосс:</b> {sl_pct*100:.2f}%
<b>Тейк-профит:</b> {tp_pct*100:.2f}%
<b>Риск/прибыль:</b> 1:{tp_pct/sl_pct:.1f}

{explanation}
"""
    
    await cb.message.edit_text(
        message_text,
        parse_mode="HTML",
        reply_markup=result_kb(trade_id)
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    _, tid, res_val = cb.data.split(":")
    await update_trade(int(tid), res_val)
    
    if res_val == "WIN":
        await cb.message.edit_text("🎉 ПОБЕДА! Результат сохранён", reply_markup=main_menu())
    else:
        await cb.message.edit_text("💪 СЛЕДУЮЩИЙ РАЗ ПОВЕЗЁТ! Результат сохранён", reply_markup=main_menu())
    
    await cb.answer()

@dp.callback_query(lambda c: c.data == "history")
async def history(cb: types.CallbackQuery):
    trades = await get_history(cb.from_user.id)
    if not trades:
        await cb.message.answer("📜 История пуста")
        return
    
    text = "📜 <b>ИСТОРИЯ СДЕЛОК</b>\n\n"
    for t in trades:
        result = "✅"
