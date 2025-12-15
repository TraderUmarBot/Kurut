"""
🔥 POCKET OPTION ULTIMATE SIGNAL BOT - РАБОЧАЯ ВЕРСИЯ
Исправлены все ошибки из логов
"""

import os
import sys
import asyncio
import logging
import aiohttp
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
from collections import defaultdict

# ===================== ОСНОВНЫЕ ИМПОРТЫ =====================
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.methods import DeleteWebhook, SetWebhook
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

from tradingview_ta import TA_Handler
import yfinance as yf
from textblob import TextBlob

# ===================== КОНФИГ (FIXED) =====================
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===================== БОТ И БД =====================
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_POOL: asyncpg.pool.Pool | None = None

# ===================== КОНСТАНТЫ =====================
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "EURGBP=X", "EURAUD=X", "GBPAUD=X",
    "CADJPY=X", "CHFJPY=X", "EURCAD=X", "GBPCAD=X", "AUDCAD=X", "AUDCHF=X", "CADCHF=X"
]
EXPIRATIONS = [1, 2, 3, 5, 10]
PAIRS_PER_PAGE = 6
MIN_DEPOSIT = 20.0

# ===================== БАЗА ДАННЫХ (ИСПРАВЛЕНО!) =====================
async def init_db():
    global DB_POOL
    if DB_POOL is None:
        try:
            DB_POOL = await asyncpg.create_pool(
                DATABASE_URL, 
                min_size=1, 
                max_size=10,
                command_timeout=60
            )
            logging.info("✅ Подключение к БД успешно")
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к БД: {e}")
            sys.exit(1)
    
    async with DB_POOL.acquire() as conn:
        # УДАЛИТЬ СТАРЫЕ ТАБЛИЦЫ ПЕРЕД СОЗДАНИЕМ НОВЫХ
        await conn.execute("DROP TABLE IF EXISTS trades CASCADE")
        await conn.execute("DROP TABLE IF EXISTS users CASCADE")
        
        # СОЗДАТЬ ТАБЛИЦЫ ЗАНОВО С ПРАВИЛЬНЫМИ СТОЛБЦАМИ
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            pocket_id TEXT,
            balance FLOAT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pair TEXT NOT NULL,
            expiration INT NOT NULL,
            direction TEXT NOT NULL,
            confidence FLOAT DEFAULT 0,
            explanation TEXT,
            result TEXT,
            signal_strength TEXT,
            stop_loss FLOAT,
            take_profit FLOAT,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """)
        
        # СОЗДАТЬ ИНДЕКСЫ
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_user_id ON trades(user_id);
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp DESC);
        """)
        
        logging.info("✅ Таблицы БД созданы/обновлены")

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
            """INSERT INTO trades 
               (user_id, pair, expiration, direction, confidence, explanation, 
                signal_strength, stop_loss, take_profit)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
               RETURNING id""",
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

# ===================== FSM =====================
class TradeState(StatesGroup):
    choosing_pair = State()
    choosing_expiration = State()

# ===================== КЛАВИАТУРЫ =====================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Валютные пары", callback_data="pairs")
    kb.button(text="📜 История сделок", callback_data="history")
    kb.adjust(1)
    return kb.as_markup()

def pairs_kb(page=0):
    kb = InlineKeyboardBuilder()
    start = page * PAIRS_PER_PAGE
    for p in PAIRS[start:start + PAIRS_PER_PAGE]:
        kb.button(text=p.replace("=X", ""), callback_data=f"pair:{p}")
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

# ===================== УЛУЧШЕННЫЙ АНАЛИЗ СИГНАЛОВ =====================
class PocketSignalAnalyzer:
    """Упрощенный, но эффективный анализатор для Pocket Option"""
    
    @staticmethod
    async def get_enhanced_signal(pair: str, expiration: int) -> tuple:
        """
        Улучшенный сигнал
        Возвращает: (направление, уверенность%, объяснение, сила_сигнала, SL%, TP%)
        """
        try:
            pair_clean = pair.replace("=X", "")
            
            # 1. TradingView анализ (основной)
            tv_result = await PocketSignalAnalyzer._tv_analysis(pair_clean, expiration)
            
            # 2. Простой момент анализ
            momentum_result = await PocketSignalAnalyzer._simple_momentum(pair_clean)
            
            # 3. Консенсус
            final_direction, final_confidence = PocketSignalAnalyzer._calculate_consensus(
                tv_result, momentum_result
            )
            
            # 4. SL/TP
            sl_pct, tp_pct = PocketSignalAnalyzer._calculate_levels(final_confidence, expiration)
            
            # 5. Форматирование
            explanation = PocketSignalAnalyzer._format_explanation(
                pair_clean, expiration, tv_result, momentum_result, 
                final_direction, final_confidence
            )
            
            # 6. Сила сигнала
            signal_strength = PocketSignalAnalyzer._get_strength(final_confidence)
            
            return (
                final_direction,
                final_confidence,
                explanation,
                signal_strength,
                sl_pct,
                tp_pct
            )
            
        except Exception as e:
            logging.error(f"Ошибка анализа {pair}: {e}")
            # Фолбэк
            return "NEUTRAL", 50.0, "Ошибка анализа, попробуйте позже", "WEAK", 0.01, 0.02
    
    @staticmethod
    async def _tv_analysis(pair: str, expiration: int) -> Dict:
        """Анализ TradingView"""
        try:
            # Маппинг таймфреймов
            tf_map = {1: "1m", 2: "2m", 3: "3m", 5: "5m", 10: "15m"}
            tf = tf_map.get(expiration, "5m")
            
            handler = TA_Handler(
                symbol=pair,
                screener="forex",
                exchange="FX_IDC",
                interval=tf
            )
            analysis = await asyncio.to_thread(handler.get_analysis)
            
            direction = analysis.summary.get("RECOMMENDATION", "NEUTRAL")
            
            # Расчет уверенности на основе оценок
            buy = analysis.summary.get("BUY", 0)
            sell = analysis.summary.get("SELL", 0)
            neutral = analysis.summary.get("NEUTRAL", 0)
            total = buy + sell + neutral
            
            if total > 0:
                if "BUY" in direction:
                    confidence = (buy / total) * 100
                elif "SELL" in direction:
                    confidence = (sell / total) * 100
                else:
                    confidence = 50
            else:
                confidence = 50
            
            return {
                "system": "tradingview",
                "direction": direction,
                "confidence": min(95, confidence),
                "timeframe": tf
            }
            
        except Exception as e:
            logging.warning(f"TV анализ ошибка: {e}")
            return {"system": "tradingview", "direction": "NEUTRAL", "confidence": 50}
    
    @staticmethod
    async def _simple_momentum(pair: str) -> Dict:
        """Простой анализ момента"""
        try:
            # Используем Yahoo Finance для последних данных
            ticker = yf.Ticker(pair)
            hist = ticker.history(period="1d", interval="5m")
            
            if len(hist) > 5:
                closes = hist['Close'].values
                
                # Простой RSI расчет
                gains = []
                losses = []
                
                for i in range(1, min(15, len(closes))):
                    change = closes[i] - closes[i-1]
                    if change > 0:
                        gains.append(change)
                    else:
                        losses.append(abs(change))
                
                avg_gain = np.mean(gains) if gains else 0
                avg_loss = np.mean(losses) if losses else 0
                
                if avg_loss == 0:
                    rsi = 100 if avg_gain > 0 else 50
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))
                
                # Определяем направление
                if rsi > 70:
                    direction = "SELL"
                    confidence = min(80, (rsi - 70) * 3)
                elif rsi < 30:
                    direction = "BUY"
                    confidence = min(80, (30 - rsi) * 3)
                elif rsi > 60:
                    direction = "SELL"
                    confidence = 65
                elif rsi < 40:
                    direction = "BUY"
                    confidence = 65
                else:
                    direction = "NEUTRAL"
                    confidence = 50
                    
                return {
                    "system": "momentum",
                    "direction": direction,
                    "confidence": confidence,
                    "rsi": rsi
                }
                
        except Exception as e:
            logging.warning(f"Momentum анализ ошибка: {e}")
        
        return {"system": "momentum", "direction": "NEUTRAL", "confidence": 50}
    
    @staticmethod
    def _calculate_consensus(tv_data: Dict, momentum: Dict) -> tuple:
        """Взвешенный консенсус"""
        # Веса: TV - 70%, Momentum - 30%
        tv_weight = 0.7
        mom_weight = 0.3
        
        tv_dir = tv_data["direction"]
        tv_conf = tv_data["confidence"] / 100
        mom_dir = momentum["direction"]
        mom_conf = momentum["confidence"] / 100
        
        # Подсчет голосов
        buy_score = 0
        sell_score = 0
        
        if "BUY" in tv_dir:
            buy_score += tv_weight * tv_conf
        elif "SELL" in tv_dir:
            sell_score += tv_weight * tv_conf
            
        if "BUY" in mom_dir:
            buy_score += mom_weight * mom_conf
        elif "SELL" in mom_dir:
            sell_score += mom_weight * mom_conf
        
        # Определение направления
        if buy_score > sell_score:
            direction = "BUY"
            raw_confidence = (buy_score / (tv_weight + mom_weight)) * 100
        elif sell_score > buy_score:
            direction = "SELL"
            raw_confidence = (sell_score / (tv_weight + mom_weight)) * 100
        else:
            direction = "NEUTRAL"
            raw_confidence = 50
        
        # Усиление при согласии
        if (("BUY" in tv_dir and "BUY" in mom_dir) or 
            ("SELL" in tv_dir and "SELL" in mom_dir)):
            confidence = min(95, raw_confidence * 1.2)
        else:
            confidence = raw_confidence
        
        return direction, confidence
    
    @staticmethod
    def _calculate_levels(confidence: float, expiration: int) -> tuple:
        """Расчет SL/TP"""
        # Базовые уровни
        if expiration <= 2:
            base_sl = 0.003
            base_tp = 0.006
        elif expiration <= 5:
            base_sl = 0.004
            base_tp = 0.008
        else:
            base_sl = 0.005
            base_tp = 0.010
        
        # Корректировка по уверенности
        conf_factor = confidence / 100
        
        sl = base_sl * (1.3 - conf_factor)
        tp = base_tp * (0.7 + conf_factor)
        
        # Ограничения
        sl = max(0.002, min(sl, 0.015))
        tp = max(0.004, min(tp, 0.025))
        
        return sl, tp
    
    @staticmethod
    def _format_explanation(pair: str, expiration: int, tv_data: Dict, 
                          momentum: Dict, direction: str, confidence: float) -> str:
        """Форматирование объяснения"""
        
        lines = [
            f"📊 АНАЛИЗ ДЛЯ POCKET OPTION",
            f"Пара: {pair} | Экспирация: {expiration} мин",
            "",
            f"🎯 ИТОГОВЫЙ СИГНАЛ: {direction}",
            f"Уверенность: {confidence:.1f}%",
            "",
            "📈 ИСТОЧНИКИ:",
            f"• TradingView: {tv_data['direction']} ({tv_data['confidence']:.1f}%)",
            f"• Моментум анализ: {momentum['direction']} ({momentum['confidence']:.1f}%)",
            ""
        ]
        
        if confidence >= 80:
            lines.append("✅ СИЛЬНЫЙ СИГНАЛ - рекомендуется вход")
        elif confidence >= 65:
            lines.append("⚠️ СРЕДНИЙ СИГНАЛ - осторожный вход")
        else:
            lines.append("⛔ СЛАБЫЙ СИГНАЛ - лучше пропустить")
        
        lines.append("")
        lines.append("⚡ Оптимизировано для краткосрочных опционов")
        
        return "\n".join(lines)
    
    @staticmethod
    def _get_strength(confidence: float) -> str:
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

# ===================== ХЕНДЛЕРЫ =====================
@dp.message(Command("start"))
async def start(msg: types.Message):
    user_id = msg.from_user.id
    
    # АВТОРСКИЙ РЕЖИМ
    if user_id in AUTHORS:
        await add_user(user_id, "AUTHOR")
        await update_balance(user_id, 9999.0)
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🚀 СУПЕР-АНАЛИЗ", callback_data="pairs")
        kb.button(text="📊 СТАТИСТИКА", callback_data="admin_stats")
        kb.button(text="⚙️ НАСТРОЙКИ", callback_data="menu")
        kb.adjust(1)
        
        await msg.answer(
            f"🔥 <b>АВТОРСКИЙ РЕЖИМ АКТИВИРОВАН</b>\n\n"
            f"👑 ID: {user_id}\n"
            f"💰 Баланс: $9999.0\n"
            f"🎯 Сигналы без ограничений\n\n"
            f"<i>Все функции разблокированы!</i>",
            parse_mode="HTML",
            reply_markup=kb.as_markup()
        )
        return
    
    # ОБЫЧНЫЙ ПОЛЬЗОВАТЕЛЬ
    balance = await get_balance(user_id)

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
        await cb.message.answer(f"❌ Пополните баланс минимум на ${MIN_DEPOSIT}")
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
    try:
        _, pair, exp = cb.data.split(":")
        exp = int(exp)
        
        # ПОЛУЧАЕМ УЛУЧШЕННЫЙ СИГНАЛ
        direction, confidence, explanation, signal_strength, sl_pct, tp_pct = \
            await PocketSignalAnalyzer.get_enhanced_signal(pair, exp)
        
        # Сохраняем сделку
        trade_id = await save_trade(
            user_id=cb.from_user.id,
            pair=pair.replace("=X", ""),
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
        
    except Exception as e:
        logging.error(f"Ошибка в expiration handler: {e}")
        await cb.message.answer(f"❌ Ошибка получения сигнала: {e}")
    
    await cb.answer()

@dp.callback_query(lambda c: c.data.startswith("res:"))
async def res(cb: types.CallbackQuery):
    try:
        _, tid, res_val = cb.data.split(":")
        await update_trade(int(tid), res_val)
        
        if res_val == "WIN":
            await cb.message.edit_text("🎉 ПОБЕДА! Результат сохранён", reply_markup=main_menu())
        else:
            await cb.message.edit_text("💪 СЛЕДУЮЩИЙ РАЗ ПОВЕЗЁТ! Результат сохранён", reply_markup=main_menu())
        
    except Exception as e:
        logging.error(f"Ошибка сохранения результата: {e}")
        await cb.message.edit_text("❌ Ошибка сохранения", reply_markup=main_menu())
    
    await cb.answer()

@dp.callback_query(lambda c: c.data == "history")
async def history(cb: types.CallbackQuery):
    try:
        trades = await get_history(cb.from_user.id)
        if not trades:
            await cb.message.answer("📜 История пуста")
            return
        
        text = "📜 <b>ИСТОРИЯ СДЕЛОК</b>\n\n"
        for t in trades:
            result = t['result'] if t['result'] else "—"
            result_emoji = "✅" if result == "WIN" else "❌" if result == "LOSE" else "➖"
            text += f"{result_emoji} {t['pair']} | {t['direction']} | {result}\n"
            text += f"   Время: {t['timestamp'].strftime('%H:%M')} | Уверенность: {t['confidence']:.1f}%\n\n"
        
        await cb.message.answer(text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка получения истории: {e}")
        await cb.message.answer("❌ Ошибка загрузки истории")

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(cb: types.CallbackQuery):
    """Статистика для авторов"""
    if cb.from_user.id not in AUTHORS:
        await cb.answer("❌ Нет доступа")
        return
    
    try:
        async with DB_POOL.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN result = 'WIN' THEN 1 END) as wins,
                    COUNT(CASE WHEN result = 'LOSE' THEN 1 END) as losses,
                    AVG(confidence) as avg_confidence
                FROM trades
                WHERE result IS NOT NULL
            """)
        
        if stats and stats['total'] > 0:
            win_rate = (stats['wins'] / stats['total']) * 100
            text = f"""
📊 <b>СТАТИСТИКА СИГНАЛОВ</b>

Всего сделок: {stats['total']}
✅ Выигрышей: {stats['wins']}
❌ Проигрышей: {stats['losses']}
📈 Винрейт: {win_rate:.1f}%

Средняя уверенность: {stats['avg_confidence']:.1f}%
"""
        else:
            text = "📊 Статистика пока недоступна (мало данных)"
        
        await cb.message.edit_text(text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка статистики: {e}")
        await cb.message.edit_text("❌ Ошибка загрузки статистики")
    
    await cb.answer()

@dp.callback_query(lambda c: c.data == "menu")
async def menu(cb: types.CallbackQuery):
    """Возврат в меню"""
    await cb.message.edit_text("🏠 Главное меню", reply_markup=main_menu())
    await cb.answer()

# ===================== POSTBACK =====================
async def handle_postback(request: web.Request):
    try:
        event = request.query.get("event")
        click_id = request.query.get("click_id")
        amount = float(request.query.get("amount", 0))

        if not click_id:
            return web.Response(text="No click_id", status=400)

        try:
            user_id = int(click_id)
        except ValueError:
            user_id = click_id

        await add_user(user_id, pocket_id=str(click_id))
        if event in ["deposit", "reg"] and amount > 0:
            await update_balance(user_id, amount)

        return web.Response(text="OK")
    except Exception as e:
        logging.error(f"Postback error: {e}")
        return web.Response(text="ERROR", status=500)

# ===================== WEBHOOK =====================
async def main():
    """Основная функция запуска"""
    try:
        # Инициализация БД
        await init_db()
        
        # Настройка вебхука
        await bot(DeleteWebhook(drop_pending_updates=True))
        await bot(SetWebhook(url=WEBHOOK_URL))
        
        # Создание aiohttp приложения
        app = web.Application()
        handler = SimpleRequestHandler(dp, bot)
        handler.register(app, WEBHOOK_PATH)
        app.router.add_get("/postback", handle_postback)
        
        # Health check
        async def health_check(request):
            return web.Response(text="OK")
        
        app.router.add_get("/health", health_check)
        
        # Запуск сервера
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        
        logging.info(f"🚀 BOT LIVE на {HOST}:{PORT}")
        logging.info(f"🌐 Webhook URL: {WEBHOOK_URL}")
        
        # Бесконечный цикл
        await asyncio.Event().wait()
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if DB_POOL:
            await DB_POOL.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Critical error: {e}")
