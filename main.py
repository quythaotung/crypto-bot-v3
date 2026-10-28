#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
BYBIT H1 SMART WAVE PRO - SINGLE FILE PRODUCTION TELEGRAM BOT
===============================================================================
Mục tiêu: Quét toàn bộ Crypto USDT Perpetual trên Bybit V5, phân tích H1/H4/D1,
phát hiện 3 Setup LONG (Early Reversal, Accumulation Breakout, Breakout Retest),
tích hợp Anti-Miss Engine, Smart SL/TP, Wave Strength Score, Trailing Stop,
quản lý trạng thái bằng SQLite và giao tiếp qua Telegram Bot.
===============================================================================
"""

import os
import sys
import time
import math
import logging
import sqlite3
import asyncio
import threading
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify

# Telegram Bot Imports (python-telegram-bot v20+)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

# -----------------------------------------------------------------------------
# 1. LOGGING & GLOBAL CONFIGURATION
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SmartWavePro")

# Variables từ Môi trường (Environment Variables)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "smart_wave_pro.db")

BYBIT_BASE_URL = "https://api.bybit.com"
SCAN_INTERVAL_MINUTES = 60  # Quét sau mỗi nến H1 đóng (hoặc chu kỳ 1 giờ)
SYMBOL_REFRESH_HOURS = 6   # Cập nhật danh sách coin mỗi 6 giờ

# Danh sách mã TradFi/Non-crypto loại trừ nếu xuất hiện
TRADFI_PREFIXES_EXCLUDE = (
    "XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", 
    "US30", "US500", "NAS100", "DE30", "UK100", "NIKKEI", 
    "AAPL", "NVDA", "TSLA", "AMZN", "MSFT", "GOOGL", "META"
)

# Lock chống quét chồng lặp
SCAN_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------
# 2. FLASK HEALTH CHECK SERVER (Duy trì keep-alive trên Render)
# -----------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health_check():
    return jsonify({
        "status": "online",
        "system": "BYBIT H1 SMART WAVE PRO",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

def run_flask_server():
    logger.info(f"Khởi chạy Flask Health Check Server trên port {PORT}...")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------------------------------------------------------
# 3. DATABASE MANAGER (SQLITE)
# -----------------------------------------------------------------------------
class DatabaseManager:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                setup_name TEXT NOT NULL,
                state TEXT NOT NULL,
                timeframe TEXT DEFAULT 'H1',
                signal_price REAL NOT NULL,
                paper_entry REAL NOT NULL,
                sl_price REAL NOT NULL,
                sl_reason TEXT,
                tp1 REAL,
                tp2 REAL,
                tp1_rr REAL,
                tp2_rr REAL,
                wave_score INTEGER DEFAULT 0,
                max_gain_pct REAL DEFAULT 0.0,
                trailing_stop REAL DEFAULT 0.0,
                notified_m10 INTEGER DEFAULT 0,
                notified_m15 INTEGER DEFAULT 0,
                notified_m20 INTEGER DEFAULT 0,
                notified_m30 INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_list (
                symbol TEXT PRIMARY KEY,
                setup_name TEXT NOT NULL,
                watch_price REAL NOT NULL,
                candles_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_symbols INTEGER,
                scanned_ok INTEGER,
                api_errors INTEGER,
                insufficient_data INTEGER,
                signals_found INTEGER
            )
            """)
            conn.commit()

    def save_signal(self, data: Dict[str, Any]):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO signals (
                symbol, setup_name, state, timeframe, signal_price, paper_entry,
                sl_price, sl_reason, tp1, tp2, tp1_rr, tp2_rr, wave_score,
                max_gain_pct, trailing_stop, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                setup_name=excluded.setup_name,
                state=excluded.state,
                signal_price=excluded.signal_price,
                paper_entry=excluded.paper_entry,
                sl_price=excluded.sl_price,
                sl_reason=excluded.sl_reason,
                tp1=excluded.tp1,
                tp2=excluded.tp2,
                tp1_rr=excluded.tp1_rr,
                tp2_rr=excluded.tp2_rr,
                wave_score=excluded.wave_score,
                updated_at=CURRENT_TIMESTAMP
            """, (
                data["symbol"], data["setup_name"], data["state"], data.get("timeframe", "H1"),
                data["signal_price"], data["paper_entry"], data["sl_price"], data.get("sl_reason", ""),
                data.get("tp1", 0.0), data.get("tp2", 0.0), data.get("tp1_rr", 0.0), data.get("tp2_rr", 0.0),
                data.get("wave_score", 0), data.get("max_gain_pct", 0.0), data.get("trailing_stop", 0.0)
            ))
            conn.commit()

    def update_signal_state(self, symbol: str, state: str, max_gain: float = None, trailing_stop: float = None,
                            m10: int = None, m15: int = None, m20: int = None, m30: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "UPDATE signals SET state=?, updated_at=CURRENT_TIMESTAMP"
            params = [state]
            if max_gain is not None:
                query += ", max_gain_pct=?"
                params.append(max_gain)
            if trailing_stop is not None:
                query += ", trailing_stop=?"
                params.append(trailing_stop)
            if m10 is not None:
                query += ", notified_m10=?"
                params.append(m10)
            if m15 is not None:
                query += ", notified_m15=?"
                params.append(m15)
            if m20 is not None:
                query += ", notified_m20=?"
                params.append(m20)
            if m30 is not None:
                query += ", notified_m30=?"
                params.append(m30)
            query += " WHERE symbol=?"
            params.append(symbol)
            cursor.execute(query, params)
            conn.commit()

    def get_active_signals_list(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM signals WHERE state NOT IN ('FINISHED', 'INVALIDATED')")
            return [dict(row) for row in cursor.fetchall()]

    def get_signal_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM signals WHERE symbol=?", (symbol,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def add_watch(self, symbol: str, setup_name: str, watch_price: float):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO watch_list (symbol, setup_name, watch_price, candles_count, created_at)
            VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                setup_name=excluded.setup_name,
                watch_price=excluded.watch_price,
                candles_count=0,
                created_at=CURRENT_TIMESTAMP
            """, (symbol, setup_name, watch_price))
            conn.commit()

    def get_watch_list(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM watch_list")
            return [dict(row) for row in cursor.fetchall()]

    def delete_watch(self, symbol: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watch_list WHERE symbol=?", (symbol,))
            conn.commit()

    def log_scan(self, total: int, ok: int, errors: int, nodata: int, signals: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO scan_logs (total_symbols, scanned_ok, api_errors, insufficient_data, signals_found)
            VALUES (?, ?, ?, ?, ?)
            """, (total, ok, errors, nodata, signals))
            conn.commit()

db = DatabaseManager()

# -----------------------------------------------------------------------------
# 4. BYBIT API V5 CLIENT & SYMBOL DISCOVERY
# -----------------------------------------------------------------------------
class BybitClient:
    def __init__(self, base_url: str = BYBIT_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "SmartWaveProBot/1.0"})

    def _request(self, endpoint: str, params: Dict[str, Any] = None, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result", {})
                    else:
                        logger.warning(f"Bybit API error code {data.get('retCode')}: {data.get('retMsg')}")
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi HTTP/Connection khi gọi {endpoint}: {e}")
                time.sleep(1.5 * (attempt + 1))
        return None

    def get_all_crypto_usdt_perpetual_symbols(self) -> List[Dict[str, Any]]:
        symbols = []
        cursor = ""
        while True:
            params = {
                "category": "linear",
                "status": "Trading",
                "limit": 1000
            }
            if cursor:
                params["cursor"] = cursor

            res = self._request("/v5/market/instruments-info", params=params)
            if not res or "list" not in res:
                break

            for item in res["list"]:
                symbol = item.get("symbol", "")
                contract_type = item.get("contractType", "")
                settle_coin = item.get("settleCoin", "")
                base_coin = item.get("baseCoin", "").upper()

                if contract_type == "LinearPerpetual" and settle_coin == "USDT":
                    if base_coin.startswith(TRADFI_PREFIXES_EXCLUDE):
                        continue
                    symbols.append({
                        "symbol": symbol,
                        "base_coin": base_coin,
                        "quote_coin": item.get("quoteCoin", ""),
                        "status": item.get("status", "")
                    })

            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break

        logger.info(f"Đã quét thành công {len(symbols)} cặp coin Crypto USDT Perpetual hợp lệ trên Bybit.")
        return symbols

    def fetch_klines(self, symbol: str, interval: str = "60", limit: int = 200) -> Optional[pd.DataFrame]:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        res = self._request("/v5/market/kline", params=params)
        if not res or "list" not in res or len(res["list"]) == 0:
            return None

        raw_list = res["list"]
        rows = []
        for item in raw_list:
            rows.append({
                "open_time": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "turnover": float(item[6])
            })

        df = pd.DataFrame(rows)
        df.sort_values(by="open_time", ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_btc_24h_change(self) -> float:
        res = self._request("/v5/market/tickers", params={"category": "linear", "symbol": "BTCUSDT"})
        if res and "list" in res and len(res["list"]) > 0:
            try:
                return float(res["list"][0].get("price24hPcnt", 0.0)) * 100
            except:
                return 0.0
        return 0.0

bybit = BybitClient()

# -----------------------------------------------------------------------------
# 5. TECHNICAL INDICATORS & STRUCTURE ANALYZER
# -----------------------------------------------------------------------------
class TechnicalAnalyzer:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) < 30:
            return df

        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['vol_sma20'] = df['volume'].rolling(window=20).mean()

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss.replace(0, 0.00001))
        df['rsi'] = 100 - (100 / (1 + rs))

        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=14).mean()

        up_move = df['high'] - df['high'].shift(1)
        down_move = df['low'].shift(1) - df['low']

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        atr_series = tr.ewm(alpha=1/14, adjust=False).mean()
        plus_di = 100 * (pd.Series(plus_dm).ewm(alpha=1/14, adjust=False).mean() / (atr_series.replace(0, 0.00001)))
        minus_di = 100 * (pd.Series(minus_dm).ewm(alpha=1/14, adjust=False).mean() / (atr_series.replace(0, 0.00001)))

        dx = 100 * (plus_di - minus_di).abs() / ((plus_di + minus_di).replace(0, 0.00001))
        df['adx'] = dx.ewm(alpha=1/14, adjust=False).mean()
        df['plus_di'] = plus_di
        df['minus_di'] = minus_di

        return df

    @staticmethod
    def find_pivots(df: pd.DataFrame, left: int = 3, right: int = 3) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        swing_highs = []
        swing_lows = []

        if df is None or len(df) < (left + right + 1):
            return swing_highs, swing_lows

        for i in range(left, len(df) - right):
            current_high = df['high'].iloc[i]
            current_low = df['low'].iloc[i]

            if all(current_high >= df['high'].iloc[i - k] for k in range(1, left + 1)) and \
               all(current_high > df['high'].iloc[i + k] for k in range(1, right + 1)):
                swing_highs.append({'index': i, 'price': current_high, 'time': df['open_time'].iloc[i]})

            if all(current_low <= df['low'].iloc[i - k] for k in range(1, left + 1)) and \
               all(current_low < df['low'].iloc[i + k] for k in range(1, right + 1)):
                swing_lows.append({'index': i, 'price': current_low, 'time': df['open_time'].iloc[i]})

        return swing_highs, swing_lows

    @staticmethod
    def check_h4_bullish_choch(df_h4: pd.DataFrame) -> Tuple[bool, float, float]:
        if df_h4 is None or len(df_h4) < 20:
            return False, 0.0, 0.0

        swing_highs, swing_lows = TechnicalAnalyzer.find_pivots(df_h4, left=2, right=2)
        if not swing_highs or not swing_lows:
            return False, 0.0, 0.0

        last_swing_high = swing_highs[-1]['price']
        last_swing_low = swing_lows[-1]['price']
        atr_h4 = df_h4['atr'].iloc[-2] if not np.isnan(df_h4['atr'].iloc[-2]) else (last_swing_high * 0.01)

        recent_h4 = df_h4.tail(8)
        for _, row in recent_h4.iterrows():
            if row['close'] > (last_swing_high + 0.1 * atr_h4):
                return True, last_swing_high, last_swing_low

        return False, 0.0, 0.0

# -----------------------------------------------------------------------------
# 6. WAVE STRENGTH SCORE ENGINE (0 - 100 Pts)
# -----------------------------------------------------------------------------
class WaveStrengthEngine:
    @staticmethod
    def calculate_score(df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_24h_gain: float) -> int:
        score = 0
        if df_h1 is None or df_h4 is None or len(df_h1) < 20 or len(df_h4) < 20:
            return 50

        h1_last = df_h1.iloc[-2]
        h4_last = df_h4.iloc[-2]

        is_choch, _, _ = TechnicalAnalyzer.check_h4_bullish_choch(df_h4)
        if is_choch:
            score += 25
        elif h4_last['close'] > h4_last['ema20']:
            score += 15

        adx_h4 = h4_last['adx'] if not np.isnan(h4_last['adx']) else 20.0
        plus_di = h4_last['plus_di'] if not np.isnan(h4_last['plus_di']) else 0.0
        minus_di = h4_last['minus_di'] if not np.isnan(h4_last['minus_di']) else 0.0

        if adx_h4 >= 25 and plus_di > minus_di:
            score += 20
        elif adx_h4 >= 20 and plus_di > minus_di:
            score += 12

        rsi_h1 = h1_last['rsi'] if not np.isnan(h1_last['rsi']) else 50.0
        rsi_h4 = h4_last['rsi'] if not np.isnan(h4_last['rsi']) else 50.0

        if rsi_h1 >= 55 and rsi_h4 >= 55:
            score += 15
        elif rsi_h1 >= 50 or rsi_h4 >= 50:
            score += 8

        vol_sma = h1_last['vol_sma20'] if not np.isnan(h1_last['vol_sma20']) else 1.0
        vol_ratio = h1_last['volume'] / (vol_sma if vol_sma > 0 else 1.0)

        if vol_ratio >= 2.0:
            score += 20
        elif vol_ratio >= 1.5:
            score += 14
        elif vol_ratio >= 1.2:
            score += 8

        if h4_last['close'] > h4_last['ema20']:
            score += 10

        coin_24h_gain = ((df_h1['close'].iloc[-2] - df_h1['close'].iloc[-25]) / df_h1['close'].iloc[-25]) * 100 if len(df_h1) > 25 else 0.0
        if coin_24h_gain > btc_24h_gain:
            score += 10

        return min(max(score, 0), 100)

# -----------------------------------------------------------------------------
# 7. SETUP DETECTOR (3 LONG SETUPS & ANTI-MISS ENGINE)
# -----------------------------------------------------------------------------
class SetupDetector:
    @staticmethod
    def detect_setup_1_early_reversal(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> Tuple[bool, bool, Dict[str, Any]]:
        if df_h1 is None or df_h4 is None or len(df_h1) < 30 or len(df_h4) < 20:
            return False, False, {}

        h1_last = df_h1.iloc[-2]
        h4_last = df_h4.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)

        high_12 = df_h1['high'].iloc[-14:-2].max()
        cond_h1_breakout = h1_last['close'] > high_12
        vol_sma = h1_last['vol_sma20'] if not np.isnan(h1_last['vol_sma20']) else 1.0
        cond_h1_vol = h1_last['volume'] > (1.5 * vol_sma)
        cond_h1_ema = h1_last['ema20'] > df_h1['ema20'].iloc[-3]
        cond_h1_dist = (h1_last['close'] - high_12) <= (1.5 * atr_h1)

        h1_valid = cond_h1_breakout and cond_h1_vol and cond_h1_ema and cond_h1_dist
        if not h1_valid:
            return False, False, {}

        is_h4_choch, _, h4_swing_low = TechnicalAnalyzer.check_h4_bullish_choch(df_h4)
        cond_h4_rsi = h4_last['rsi'] > 50 and h4_last['rsi'] > df_h4['rsi'].iloc[-3]
        cond_h4_di = h4_last['plus_di'] > h4_last['minus_di']
        h4_confirmed = is_h4_choch and cond_h4_rsi and cond_h4_di

        _, h1_lows = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
        h1_sl_price = h1_lows[-1]['price'] if h1_lows else df_h1['low'].iloc[-6:-2].min()

        details = {
            "symbol": "",
            "setup_name": "SETUP 1 – EARLY REVERSAL",
            "signal_price": h1_last['close'],
            "paper_entry": df_h1['open'].iloc[-1],
            "structure_low": h1_sl_price if h1_lows else h4_swing_low,
            "atr_h1": atr_h1,
            "vol_ratio": round(h1_last['volume'] / (vol_sma if vol_sma > 0 else 1.0), 2)
        }

        if h1_valid and h4_confirmed:
            return True, False, details
        elif h1_valid:
            return False, True, details

        return False, False, {}

    @staticmethod
    def detect_setup_2_accumulation_breakout(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> Tuple[bool, bool, Dict[str, Any]]:
        if df_h1 is None or len(df_h1) < 50:
            return False, False, {}

        h1_last = df_h1.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)

        base_df = df_h1.iloc[-30:-2]
        base_high = base_df['high'].max()
        base_low = base_df['low'].min()
        range_height = base_high - base_low

        cond_range_atr = range_height <= (5.0 * atr_h1)
        atr_recent = df_h1['atr'].iloc[-6:-2].mean()
        atr_prior = df_h1['atr'].iloc[-20:-6].mean()
        cond_atr_contract = atr_recent <= atr_prior

        cond_breakout = h1_last['close'] > (base_high + 0.1 * atr_h1)
        vol_sma = h1_last['vol_sma20'] if not np.isnan(h1_last['vol_sma20']) else 1.0
        cond_vol = h1_last['volume'] > (1.8 * vol_sma)

        candle_range = h1_last['high'] - h1_last['low']
        candle_body = abs(h1_last['close'] - h1_last['open'])
        cond_body = (candle_body / (candle_range if candle_range > 0 else 1.0)) >= 0.60
        rsi_h1 = h1_last['rsi'] if not np.isnan(h1_last['rsi']) else 50.0
        cond_rsi = 55.0 <= rsi_h1 <= 72.0

        if cond_range_atr and cond_atr_contract and cond_breakout and cond_vol and cond_body and cond_rsi:
            details = {
                "symbol": "",
                "setup_name": "SETUP 2 – ACCUMULATION BREAKOUT",
                "signal_price": h1_last['close'],
                "paper_entry": df_h1['open'].iloc[-1],
                "base_low": base_low,
                "base_high": base_high,
                "atr_h1": atr_h1,
                "vol_ratio": round(h1_last['volume'] / (vol_sma if vol_sma > 0 else 1.0), 2)
            }
            h4_last = df_h4.iloc[-2] if df_h4 is not None and len(df_h4) >= 5 else None
            h4_bearish = h4_last is not None and h4_last['close'] < h4_last['ema20'] and h4_last['rsi'] < 45

            if h4_bearish:
                return False, True, details
            return True, False, details

        return False, False, {}

    @staticmethod
    def detect_setup_3_breakout_retest(df_h1: pd.DataFrame, df_h4: pd.DataFrame, prior_breakout_price: float) -> Tuple[bool, Dict[str, Any]]:
        if df_h1 is None or len(df_h1) < 20 or prior_breakout_price <= 0:
            return False, {}

        h1_last = df_h1.iloc[-2]
        h1_prev = df_h1.iloc[-3]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)

        recent_low = df_h1['low'].iloc[-8:-2].min()
        cond_retest_dist = abs(recent_low - prior_breakout_price) <= (0.3 * atr_h1)
        cond_no_deep_close = all(df_h1['close'].iloc[-8:-2] >= (prior_breakout_price - 0.3 * atr_h1))
        cond_bullish_confirm = h1_last['close'] > h1_prev['high'] and h1_last['close'] > h1_last['open']

        vol_sma = h1_last['vol_sma20'] if not np.isnan(h1_last['vol_sma20']) else 1.0
        cond_vol = h1_last['volume'] > (1.2 * vol_sma)
        h4_rsi = df_h4['rsi'].iloc[-2] if df_h4 is not None and len(df_h4) >= 5 else 50.0
        cond_h4_rsi = h4_rsi >= 55.0

        if cond_retest_dist and cond_no_deep_close and cond_bullish_confirm and cond_vol and cond_h4_rsi:
            details = {
                "symbol": "",
                "setup_name": "SETUP 3 – BREAKOUT RETEST",
                "signal_price": h1_last['close'],
                "paper_entry": df_h1['open'].iloc[-1],
                "retest_low": recent_low,
                "atr_h1": atr_h1,
                "vol_ratio": round(h1_last['volume'] / (vol_sma if vol_sma > 0 else 1.0), 2)
            }
            return True, details

        return False, {}

# -----------------------------------------------------------------------------
# 8. SMART SL & SMART TP ENGINE
# -----------------------------------------------------------------------------
class SmartRiskEngine:
    @staticmethod
    def calculate_sl(setup_name: str, details: Dict[str, Any]) -> Tuple[float, str, bool, str]:
        paper_entry = details["paper_entry"]
        atr_h1 = details["atr_h1"]

        if "SETUP 1" in setup_name:
            struct_low = details.get("structure_low", paper_entry * 0.95)
            reason = "H1 Swing Low - 0.35 ATR H1"
        elif "SETUP 2" in setup_name:
            struct_low = details.get("base_low", paper_entry * 0.95)
            reason = "Accumulation Base Low - 0.35 ATR H1"
        elif "SETUP 3" in setup_name:
            struct_low = details.get("retest_low", paper_entry * 0.95)
            reason = "Retest Low - 0.35 ATR H1"
        else:
            struct_low = paper_entry * 0.95
            reason = "Default Structure Low"

        sl_price = struct_low - (0.35 * atr_h1)
        if sl_price >= paper_entry:
            return 0.0, "SL >= Paper Entry (Invalid)", False, "INVALID_SL"

        risk = paper_entry - sl_price
        risk_pct = (risk / paper_entry) * 100
        risk_atr = risk / atr_h1 if atr_h1 > 0 else 1.0

        tag = "NORMAL"
        if risk_atr > 3.0:
            tag = "SL TOO WIDE"
        elif risk_pct < 0.5:
            tag = "SL TOO TIGHT"

        return round(sl_price, 4), reason, True, tag

    @staticmethod
    def calculate_tp(df_h1: pd.DataFrame, df_h4: pd.DataFrame, paper_entry: float, sl_price: float) -> Tuple[float, float, float, float, str]:
        risk = paper_entry - sl_price
        if risk <= 0:
            return round(paper_entry * 1.05, 4), round(paper_entry * 1.10, 4), 1.0, 2.0, "DEFAULT"

        atr_h1 = df_h1['atr'].iloc[-2] if 'atr' in df_h1.columns else (paper_entry * 0.01)

        h1_highs, _ = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
        h4_highs, _ = TechnicalAnalyzer.find_pivots(df_h4, left=2, right=2) if df_h4 is not None else ([], [])

        all_res = [h['price'] for h in h1_highs + h4_highs if h['price'] > paper_entry]
        all_res.sort()

        grouped_res = []
        for res in all_res:
            if not grouped_res:
                grouped_res.append(res)
            elif (res - grouped_res[-1]) >= (0.25 * atr_h1):
                grouped_res.append(res)

        tp1 = grouped_res[0] if len(grouped_res) >= 1 else paper_entry + (2.0 * risk)
        tp2_note = "CONFIRMED RESISTANCE"
        tp2 = grouped_res[1] if len(grouped_res) >= 2 else paper_entry + (3.5 * risk)
        if len(grouped_res) < 2:
            tp2_note = "ATR PROJECTION – NOT CONFIRMED RESISTANCE"

        rr_tp1 = round((tp1 - paper_entry) / risk, 2)
        rr_tp2 = round((tp2 - paper_entry) / risk, 2)

        return round(tp1, 4), round(tp2, 4), rr_tp1, rr_tp2, tp2_note

# -----------------------------------------------------------------------------
# 9. TRAILING STOP & SMART TP MONITORING ENGINE
# -----------------------------------------------------------------------------
class TrailingAndMonitorEngine:
    @staticmethod
    def calculate_trailing_stop(df_h1: pd.DataFrame, df_h4: pd.DataFrame, highest_price: float, prev_trailing: float) -> float:
        if df_h1 is None or len(df_h1) < 5:
            return prev_trailing

        atr_h1 = df_h1['atr'].iloc[-2] if not np.isnan(df_h1['atr'].iloc[-2]) else (highest_price * 0.01)
        atr_h4 = df_h4['atr'].iloc[-2] if df_h4 is not None and not np.isnan(df_h4['atr'].iloc[-2]) else (highest_price * 0.02)

        buffer = max(2.5 * atr_h1, 0.8 * atr_h4)
        candidate = highest_price - buffer
        return max(prev_trailing, round(candidate, 4))

# -----------------------------------------------------------------------------
# 10. TELEGRAM NOTIFICATION SERVICE
# -----------------------------------------------------------------------------
class TelegramService:
    def __init__(self, token: str = BOT_TOKEN, chat_id: str = CHAT_ID):
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send_message_sync(self, text: str, parse_mode: str = "HTML") -> bool:
        if not self.token or self.token.startswith("YOUR_"):
            logger.info(f"[TELEGRAM PREVIEW]\n{text}")
            return False

        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            resp = requests.post(self.api_url, json=payload, timeout=8)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Lỗi gửi tin nhắn Telegram: {e}")
            return False

    @staticmethod
    def format_signal_message(data: Dict[str, Any]) -> str:
        symbol = data['symbol']
        chart_url = f"https://www.bybit.com/trade/usdt/{symbol}"
        gain_tp1 = round(((data['tp1'] - data['paper_entry']) / data['paper_entry']) * 100, 2)
        gain_tp2 = round(((data['tp2'] - data['paper_entry']) / data['paper_entry']) * 100, 2)
        risk_pct = round(((data['paper_entry'] - data['sl_price']) / data['paper_entry']) * 100, 2)

        msg = f"""🚀 <b>BYBIT H1 SMART WAVE PRO</b>
🪙 <b>Coin:</b> #{symbol}
📊 <b>Market:</b> Crypto USDT Perpetual
🟢 <b>{data['setup_name']}</b>
⏱ <b>Timeframe:</b> H1

💰 <b>Signal Price:</b> {data['signal_price']}
📍 <b>Paper Entry:</b> {data['paper_entry']}
🛡 <b>SL Structure:</b> {data['sl_price']} ({data.get('sl_reason', 'Structure Low')})
📉 <b>Risk:</b> {risk_pct}%

🎯 <b>TP1:</b> {data['tp1']} (+{gain_tp1}%) | <b>R:</b> {data['tp1_rr']}
🎯 <b>TP2:</b> {data['tp2']} (+{gain_tp2}%) | <b>R:</b> {data['tp2_rr']}

💪 <b>Wave Strength:</b> {data['wave_score']}/100
🔥 <b>Volume:</b> {data.get('vol_ratio', 1.5)}x SMA20
🌊 <b>Wave Phase:</b> EARLY EXPANSION
✅ <b>Status:</b> CONFIRMED – PAPER SIGNAL

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{symbol}</a>"""
        return msg

    @staticmethod
    def format_tp_update_message(symbol: str, gain_pct: float, score: int, status_type: str, trailing_sl: float) -> str:
        chart_url = f"https://www.bybit.com/trade/usdt/{symbol}"

        if status_type == "STRONG":
            header = "🔥 <b>SMART TP – EXTENDED WAVE</b>"
            status_text = "EXTENDED – Tiếp tục mở rộng mục tiêu"
            emoji_score = "💪"
        elif status_type == "WEAK":
            header = "⚠️ <b>SMART TP – MOMENTUM WEAKENING</b>"
            status_text = "TP NEAR / WATCH EXIT – Lực tăng suy yếu"
            emoji_score = "📉"
        else:
            header = "📊 <b>SMART TP – MODERATE MOMENTUM</b>"
            status_text = "MONITOR – Đang tiếp tục giám sát"
            emoji_score = "⚡"

        msg = f"""{header}
🪙 <b>Coin:</b> #{symbol}
📈 <b>Mức tăng hiện tại:</b> +{gain_pct:.2f}%
{emoji_score} <b>Wave Strength Score:</b> {score}/100
🛡 <b>Trailing Stop Reference:</b> {trailing_sl}
✅ <b>Trạng thái:</b> {status_text}

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{symbol}</a>"""
        return msg

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 11. SCANNER ENGINE & SCHEDULER
# -----------------------------------------------------------------------------
class MarketScanner:
    def __init__(self):
        self.symbols_cache = []
        self.last_symbol_refresh = 0

    def refresh_symbols_if_needed(self):
        now = time.time()
        if not self.symbols_cache or (now - self.last_symbol_refresh) > (SYMBOL_REFRESH_HOURS * 3600):
            logger.info("Đang làm mới danh sách Crypto USDT Perpetual từ Bybit...")
            self.symbols_cache = bybit.get_all_crypto_usdt_perpetual_symbols()
            self.last_symbol_refresh = now

    def scan_market(self):
        self.refresh_symbols_if_needed()
        total_symbols = len(self.symbols_cache)
        scanned_ok = 0
        api_errors = 0
        insufficient_data = 0
        signals_found = 0

        logger.info(f"=== BẮT ĐẦU VÒNG QUÉT H1 MỚI: {total_symbols} SYMBOLS ===")
        btc_gain = bybit.get_btc_24h_change()

        for item in self.symbols_cache:
            symbol = item["symbol"]
            try:
                df_h1 = bybit.fetch_klines(symbol, interval="60", limit=120)
                df_h4 = bybit.fetch_klines(symbol, interval="240", limit=80)

                if df_h1 is None or len(df_h1) < 50 or df_h4 is None or len(df_h4) < 20:
                    insufficient_data += 1
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                df_h4 = TechnicalAnalyzer.add_indicators(df_h4)
                scanned_ok += 1

                active_sig = db.get_signal_by_symbol(symbol)
                if active_sig and active_sig["state"] not in ("FINISHED", "INVALIDATED"):
                    self._process_active_signal(active_sig, df_h1, df_h4, btc_gain)
                    continue

                is_s1_conf, is_s1_watch, s1_details = SetupDetector.detect_setup_1_early_reversal(df_h1, df_h4)
                if is_s1_conf:
                    self._create_paper_signal(symbol, "SETUP 1 – EARLY REVERSAL", s1_details, df_h1, df_h4, btc_gain)
                    signals_found += 1
                    continue
                elif is_s1_watch:
                    db.add_watch(symbol, "SETUP 1 – EARLY REVERSAL", s1_details["signal_price"])

                is_s2_conf, is_s2_watch, s2_details = SetupDetector.detect_setup_2_accumulation_breakout(df_h1, df_h4)
                if is_s2_conf:
                    self._create_paper_signal(symbol, "SETUP 2 – ACCUMULATION BREAKOUT", s2_details, df_h1, df_h4, btc_gain)
                    signals_found += 1
                    continue
                elif is_s2_watch:
                    db.add_watch(symbol, "SETUP 2 – ACCUMULATION BREAKOUT", s2_details.get("signal_price", 0.0))

                watch_item = next((w for w in db.get_watch_list() if w["symbol"] == symbol), None)
                if watch_item:
                    is_s3_conf, s3_details = SetupDetector.detect_setup_3_breakout_retest(df_h1, df_h4, watch_item["watch_price"])
                    if is_s3_conf:
                        self._create_paper_signal(symbol, "SETUP 3 – BREAKOUT RETEST", s3_details, df_h1, df_h4, btc_gain)
                        db.delete_watch(symbol)
                        signals_found += 1
                        continue

            except Exception as e:
                logger.error(f"Lỗi khi xử lý symbol {symbol}: {e}")
                api_errors += 1

            time.sleep(0.05)

        db.log_scan(total_symbols, scanned_ok, api_errors, insufficient_data, signals_found)
        logger.info(f"=== TỔNG KẾT QUÉT: Scanned OK={scanned_ok}/{total_symbols}, Signals={signals_found}, Errors={api_errors} ===")

    def _create_paper_signal(self, symbol: str, setup_name: str, details: Dict[str, Any],
                             df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_gain: float):
        details["symbol"] = symbol
        sl_price, sl_reason, is_valid_sl, _ = SmartRiskEngine.calculate_sl(setup_name, details)
        if not is_valid_sl:
            return

        paper_entry = details["paper_entry"]
        tp1, tp2, rr_tp1, rr_tp2, _ = SmartRiskEngine.calculate_tp(df_h1, df_h4, paper_entry, sl_price)
        wave_score = WaveStrengthEngine.calculate_score(df_h1, df_h4, btc_gain)

        sig_data = {
            "symbol": symbol,
            "setup_name": setup_name,
            "state": "CONFIRMED",
            "signal_price": details["signal_price"],
            "paper_entry": paper_entry,
            "sl_price": sl_price,
            "sl_reason": sl_reason,
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": rr_tp1,
            "tp2_rr": rr_tp2,
            "wave_score": wave_score,
            "vol_ratio": details.get("vol_ratio", 1.5)
        }

        db.save_signal(sig_data)
        msg_text = telegram_service.format_signal_message(sig_data)
        telegram_service.send_message_sync(msg_text)

    def _process_active_signal(self, sig: Dict[str, Any], df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_gain: float):
        symbol = sig["symbol"]
        paper_entry = sig["paper_entry"]
        sl_price = sig["sl_price"]
        current_close = df_h1['close'].iloc[-2]
        current_high = df_h1['high'].iloc[-2]
        current_low = df_h1['low'].iloc[-2]

        if current_low <= sl_price:
            db.update_signal_state(symbol, "FINISHED")
            logger.info(f"Signal #{symbol} đã chạm Stop Loss {sl_price}. Kết thúc theo dõi.")
            return

        gain_pct = ((current_close - paper_entry) / paper_entry) * 100
        prev_max_gain = sig.get("max_gain_pct", 0.0)
        new_max_gain = max(prev_max_gain, gain_pct)

        prev_trailing = sig.get("trailing_stop", 0.0)
        new_trailing = TrailingAndMonitorEngine.calculate_trailing_stop(df_h1, df_h4, current_high, prev_trailing)

        if new_trailing > 0 and current_close < new_trailing:
            db.update_signal_state(symbol, "FINISHED", max_gain=new_max_gain, trailing_stop=new_trailing)
            logger.info(f"Signal #{symbol} đóng dưới Trailing Stop {new_trailing}. TREND EXIT.")
            return

        current_score = WaveStrengthEngine.calculate_score(df_h1, df_h4, btc_gain)
        notified_m10 = sig.get("notified_m10", 0)
        notified_m15 = sig.get("notified_m15", 0)

        if gain_pct >= 10.0 and not notified_m10:
            status_type = "STRONG" if current_score >= 80 else ("MODERATE" if current_score >= 60 else "WEAK")
            msg = telegram_service.format_tp_update_message(symbol, gain_pct, current_score, status_type, new_trailing)
            telegram_service.send_message_sync(msg)
            db.update_signal_state(symbol, "SMART_TP", max_gain=new_max_gain, trailing_stop=new_trailing, m10=1)

        elif gain_pct >= 15.0 and not notified_m15:
            status_type = "STRONG" if current_score >= 80 else ("MODERATE" if current_score >= 60 else "WEAK")
            msg = telegram_service.format_tp_update_message(symbol, gain_pct, current_score, status_type, new_trailing)
            telegram_service.send_message_sync(msg)
            db.update_signal_state(symbol, "EXTENDED", max_gain=new_max_gain, trailing_stop=new_trailing, m15=1)

        else:
            db.update_signal_state(symbol, sig["state"], max_gain=new_max_gain, trailing_stop=new_trailing)

scanner = MarketScanner()

# -----------------------------------------------------------------------------
# 12. BACKTEST ANALYZER
# -----------------------------------------------------------------------------
class BacktestAnalyzer:
    @staticmethod
    def run_simple_backtest() -> Dict[str, Any]:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM signals")
            rows = [dict(r) for r in cursor.fetchall()]

        if not rows:
            return {"total": 0, "winrate_10": 0.0, "winrate_15": 0.0, "avg_gain": 0.0}

        total = len(rows)
        win_10 = sum(1 for r in rows if r["max_gain_pct"] >= 10.0)
        win_15 = sum(1 for r in rows if r["max_gain_pct"] >= 15.0)
        avg_gain = sum(r["max_gain_pct"] for r in rows) / total

        return {
            "total": total,
            "winrate_10": round((win_10 / total) * 100, 2),
            "winrate_15": round((win_15 / total) * 100, 2),
            "avg_gain": round(avg_gain, 2)
        }

# -----------------------------------------------------------------------------
# 13. TELEGRAM INTERACTIVE COMMAND HANDLERS
# -----------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 Quét Ngay (/scan)", callback_data="btn_scan"), InlineKeyboardButton("📊 Trạng Thái (/status)", callback_data="btn_status")],
        [InlineKeyboardButton("🟢 Active Setups", callback_data="btn_active"), InlineKeyboardButton("🔥 Top Coins", callback_data="btn_top")],
        [InlineKeyboardButton("📈 Thống Kê (/stats)", callback_data="btn_stats"), InlineKeyboardButton("❓ Hướng Dẫn (/help)", callback_data="btn_help")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_msg = """🚀 <b>CHÀO MỪNG ĐẾN VỚI BYBIT H1 SMART WAVE PRO BOT</b>

Hệ thống chuyên tự động quét toàn bộ Crypto Futures USDT Perpetual trên Bybit V5, phân tích cấu trúc H1/H4/D1 để phát hiện sớm các sóng tăng bùng nổ.

Vui lòng chọn các tính năng bên dưới:"""
    await update.message.reply_text(welcome_msg, parse_mode="HTML", reply_markup=reply_markup)

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Vòng quét đang chạy, vui lòng chờ trong giây lát...")
        return

    await update.message.reply_text("🔄 Đang tiến hành quét toàn bộ thị trường Bybit Crypto Futures USDT Perpetual...")
    async with SCAN_LOCK:
        await asyncio.to_thread(scanner.scan_market)
    await update.message.reply_text("✅ Đã hoàn tất vòng quét thị trường thực tế!")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM scan_logs ORDER BY id DESC LIMIT 1")
        last_log = cursor.fetchone()

    if not last_log:
        await update.message.reply_text("Chưa có dữ liệu nhật ký quét.")
        return

    log_dict = dict(last_log)
    msg = f"""📊 <b>BÁO CÁO TRẠNG THÁI HỆ THỐNG</b>
⏱ <b>Lần quét gần nhất:</b> {log_dict['timestamp']}
🌐 <b>Tổng số coin hợp lệ:</b> {log_dict['total_symbols']}
✅ <b>Quét thành công:</b> {log_dict['scanned_ok']}
⚠️ <b>Thiếu dữ liệu / Lỗi:</b> {log_dict['insufficient_data']} / {log_dict['api_errors']}
🎯 <b>Tín hiệu tìm thấy:</b> {log_dict['signals_found']}"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    signals = db.get_active_signals_list()
    if not signals:
        await update.message.reply_text("Hiện không có setup LONG nào đang theo dõi.")
        return

    msg = "🟢 <b>DANH SÁCH SETUP Đang THEO DÕI:</b>\n\n"
    for s in signals:
        msg += f"• <b>#{s['symbol']}</b> | {s['setup_name']}\n  Entry: {s['paper_entry']} | Max Gain: +{s['max_gain_pct']:.2f}%\n"

    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    signals = db.get_active_signals_list()
    if not signals:
        await update.message.reply_text("Chưa có dữ liệu điểm sức mạnh coin.")
        return

    sorted_sig = sorted(signals, key=lambda x: x['wave_score'], reverse=True)[:5]
    msg = "🔥 <b>TOP COIN CÓ ĐIỂM SỨC MẠNH SÓNG CAO NHẤT:</b>\n\n"
    for s in sorted_sig:
        msg += f"• <b>#{s['symbol']}</b> - Wave Score: <b>{s['wave_score']}/100</b>\n  Setup: {s['setup_name']}\n"

    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = BacktestAnalyzer.run_simple_backtest()
    msg = f"""📈 <b>THỐNG KÊ TÍN HIỆU GIẢ LẬP (PAPER TRADING)</b>
🔹 <b>Tổng số tín hiệu:</b> {stats['total']}
🎯 <b>Tỷ lệ đạt +10%:</b> {stats['winrate_10']}%
🚀 <b>Tỷ lệ đạt +15%:</b> {stats['winrate_15']}%
📊 <b>Mức tăng trung bình:</b> +{stats['avg_gain']}%"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Cú pháp: `/coin SYMBOL` (Ví dụ: `/coin BTCUSDT`)", parse_mode="Markdown")
        return

    symbol = context.args[0].upper().replace("#", "")
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    await update.message.reply_text(f"🔍 Đang phân tích coin #{symbol}...")

    df_h1 = await asyncio.to_thread(bybit.fetch_klines, symbol, "60", 100)
    df_h4 = await asyncio.to_thread(bybit.fetch_klines, symbol, "240", 50)

    if df_h1 is None or df_h4 is None:
        await update.message.reply_text(f"❌ Không tìm thấy dữ liệu cho symbol #{symbol} trên Bybit.")
        return

    df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
    df_h4 = TechnicalAnalyzer.add_indicators(df_h4)
    btc_gain = bybit.get_btc_24h_change()

    score = WaveStrengthEngine.calculate_score(df_h1, df_h4, btc_gain)
    last_close = df_h1['close'].iloc[-1]
    rsi_h1 = df_h1['rsi'].iloc[-1]
    rsi_h4 = df_h4['rsi'].iloc[-1]

    msg = f"""📊 <b>PHÂN TÍCH NHANH: #{symbol}</b>
💰 <b>Giá hiện tại:</b> {last_close}
💪 <b>Wave Strength Score:</b> {score}/100
📈 <b>RSI H1:</b> {rsi_h1:.1f}
📈 <b>RSI H4:</b> {rsi_h4:.1f}
🔗 <a href="https://www.bybit.com/trade/usdt/{symbol}">Mở biểu đồ Bybit #{symbol}</a>"""

    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """❓ <b>DANH SÁCH LỆNH CỦA BOT</b>

/start - Menu chính tương tác nút bấm
/scan - Kích hoạt vòng quét thủ công toàn bộ coin
/status - Xem báo cáo tình trạng hệ thống & số coin
/active - Danh sách các Setup đang được theo dõi
/top - Xem Top coin có điểm sức mạnh sóng cao nhất
/stats - Thống kê tỷ lệ thắng và hiệu suất tín hiệu
/coin SYMBOL - Phân tích nhanh một đồng coin bất kỳ
/help - Xem hướng dẫn sử dụng"""
    await update.message.reply_text(help_text, parse_mode="HTML")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "btn_scan":
        await cmd_scan(query, context)
    elif data == "btn_status":
        await cmd_status(query, context)
    elif data == "btn_active":
        await cmd_active(query, context)
    elif data == "btn_top":
        await cmd_top(query, context)
    elif data == "btn_stats":
        await cmd_stats(query, context)
    elif data == "btn_help":
        await cmd_help(query, context)

# -----------------------------------------------------------------------------
# 14. AUTOMATED BACKGROUND SCAN SCHEDULER
# -----------------------------------------------------------------------------
async def scheduled_scanner_loop():
    logger.info("Đã khởi chạy Scheduler vòng quét tự động...")
    while True:
        try:
            if not SCAN_LOCK.locked():
                async with SCAN_LOCK:
                    await asyncio.to_thread(scanner.scan_market)
        except Exception as e:
            logger.error(f"Lỗi trong vòng quét tự động: {e}")

        await asyncio.sleep(SCAN_INTERVAL_MINUTES * 60)

# -----------------------------------------------------------------------------
# 15. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG BYBIT H1 SMART WAVE PRO BOT ")
    logger.info("==================================================")

    flask_thread = threading.Thread(target=run_flask_server, daemon=True)
    flask_thread.start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN! Vui lòng thiết lập biến môi trường BOT_TOKEN.")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("active", cmd_active))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("coin", cmd_coin))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(callback_handler))

    async def main_async():
        asyncio.create_task(scheduled_scanner_loop())
        await app.initialize()
        await app.start()
        logger.info("Bot đã sẵn sàng và đang lắng nghe lệnh trên Telegram...")
        await app.updater.start_polling()

        stop_event = asyncio.Event()
        await stop_event.wait()

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Dừng bot...")

if __name__ == "__main__":
    main()
