#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
BYBIT H1 SMART WAVE PRO - FULL AUTO 24/7 PRODUCTION TELEGRAM BOT
===============================================================================
Mục tiêu: Tự động hoàn toàn 24/7, đồng bộ thời gian đóng nến H1, quét toàn bộ
Crypto USDT Perpetual, chủ động bắn tín hiệu, theo dõi Smart TP, Trailing Stop
và lưu trữ trạng thái khôi phục qua SQLite trong duy nhất 1 file main.py.
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
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("SmartWaveProFullAuto")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "smart_wave_pro_auto.db")

BYBIT_BASE_URL = "https://api.bybit.com"
MAX_SYMBOLS_SCAN = 300
SYMBOL_REFRESH_HOURS = 6

TRADFI_PREFIXES_EXCLUDE = (
    "XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", 
    "US30", "US500", "NAS100", "DE30", "UK100", "NIKKEI", 
    "AAPL", "NVDA", "TSLA", "AMZN", "MSFT", "GOOGL", "META"
)

# Biến toàn cục trạng thái hệ thống cho lệnh /status
SYSTEM_STATE = {
    "is_scanning": False,
    "last_scan_time": "Chưa quét",
    "next_scan_time": "Đang tính toán...",
    "total_symbols_scanned": 0,
    "scanned_ok": 0,
    "api_errors": 0,
    "signals_found": 0
}

SCAN_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------
# 2. FLASK HEALTH CHECK SERVER (Keep-alive 24/7 trên Render)
# -----------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health_check():
    return jsonify({
        "status": "online",
        "system": "BYBIT H1 SMART WAVE PRO FULL AUTO",
        "is_scanning": SYSTEM_STATE["is_scanning"],
        "last_scan": SYSTEM_STATE["last_scan_time"],
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
        self.session.headers.update({"User-Agent": "SmartWaveProFullAuto/1.0"})

    def _request(self, endpoint: str, params: Dict[str, Any] = None, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result", {})
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi kết nối API Bybit {endpoint}: {e}")
                time.sleep(1.5 * (attempt + 1))
        return None

    def get_all_crypto_usdt_perpetual_symbols(self) -> List[Dict[str, Any]]:
        symbols = []
        cursor = ""
        while True:
            params = {"category": "linear", "status": "Trading", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            res = self._request("/v5/market/instruments-info", params=params)
            if not res or "list" not in res:
                break
            for item in res["list"]:
                symbol = item.get("symbol", "")
                if item.get("contractType", "") == "LinearPerpetual" and item.get("settleCoin", "") == "USDT":
                    base_coin = item.get("baseCoin", "").upper()
                    if not base_coin.startswith(TRADFI_PREFIXES_EXCLUDE):
                        symbols.append({"symbol": symbol})
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
        return symbols[:MAX_SYMBOLS_SCAN]

    def fetch_klines(self, symbol: str, interval: str = "60", limit: int = 200) -> Optional[pd.DataFrame]:
        res = self._request("/v5/market/kline", params={"category": "linear", "symbol": symbol, "interval": interval, "limit": limit})
        if not res or "list" not in res or len(res["list"]) == 0:
            return None
        rows = [{"open_time": int(i[0]), "open": float(i[1]), "high": float(i[2]), "low": float(i[3]), "close": float(i[4]), "volume": float(i[5])} for i in res["list"]]
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
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['rsi'] = 100 - (100 / (1 + gain / loss.replace(0, 0.00001)))
        tr = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift(1)).abs(), (df['low'] - df['close'].shift(1)).abs()], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=14).mean()

        up_move = df['high'] - df['high'].shift(1)
        down_move = df['low'].shift(1) - df['low']
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        atr_series = tr.ewm(alpha=1/14, adjust=False).mean()
        df['plus_di'] = 100 * (pd.Series(plus_dm).ewm(alpha=1/14, adjust=False).mean() / atr_series.replace(0, 0.00001))
        df['minus_di'] = 100 * (pd.Series(minus_dm).ewm(alpha=1/14, adjust=False).mean() / atr_series.replace(0, 0.00001))
        df['adx'] = (100 * (df['plus_di'] - df['minus_di']).abs() / (df['plus_di'] + df['minus_di']).replace(0, 0.00001)).ewm(alpha=1/14, adjust=False).mean()
        return df

    @staticmethod
    def find_pivots(df: pd.DataFrame, left: int = 3, right: int = 3) -> Tuple[List[Dict], List[Dict]]:
        highs, lows = [], []
        if df is None or len(df) < (left + right + 1):
            return highs, lows
        for i in range(left, len(df) - right):
            if all(df['high'].iloc[i] >= df['high'].iloc[i - k] for k in range(1, left + 1)) and all(df['high'].iloc[i] > df['high'].iloc[i + k] for k in range(1, right + 1)):
                highs.append({'price': df['high'].iloc[i], 'time': df['open_time'].iloc[i]})
            if all(df['low'].iloc[i] <= df['low'].iloc[i - k] for k in range(1, left + 1)) and all(df['low'].iloc[i] < df['low'].iloc[i + k] for k in range(1, right + 1)):
                lows.append({'price': df['low'].iloc[i], 'time': df['open_time'].iloc[i]})
        return highs, lows

    @staticmethod
    def check_h4_bullish_choch(df_h4: pd.DataFrame) -> Tuple[bool, float, float]:
        if df_h4 is None or len(df_h4) < 20:
            return False, 0.0, 0.0
        highs, lows = TechnicalAnalyzer.find_pivots(df_h4, left=2, right=2)
        if not highs or not lows:
            return False, 0.0, 0.0
        last_high, last_low = highs[-1]['price'], lows[-1]['price']
        atr_h4 = df_h4['atr'].iloc[-2] if not np.isnan(df_h4['atr'].iloc[-2]) else (last_high * 0.01)
        for _, row in df_h4.tail(8).iterrows():
            if row['close'] > (last_high + 0.1 * atr_h4):
                return True, last_high, last_low
        return False, 0.0, 0.0

# -----------------------------------------------------------------------------
# 6. WAVE STRENGTH SCORE ENGINE
# -----------------------------------------------------------------------------
class WaveStrengthEngine:
    @staticmethod
    def calculate_score(df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_gain: float) -> int:
        score = 0
        if df_h1 is None or df_h4 is None or len(df_h1) < 20 or len(df_h4) < 20:
            return 50
        h1_last, h4_last = df_h1.iloc[-2], df_h4.iloc[-2]

        is_choch, _, _ = TechnicalAnalyzer.check_h4_bullish_choch(df_h4)
        if is_choch: score += 25
        elif h4_last['close'] > h4_last['ema20']: score += 15

        adx = h4_last.get('adx', 20)
        if adx >= 25 and h4_last.get('plus_di', 0) > h4_last.get('minus_di', 0): score += 20
        elif adx >= 20: score += 12

        if h1_last.get('rsi', 50) >= 55 and h4_last.get('rsi', 50) >= 55: score += 15
        elif h1_last.get('rsi', 50) >= 50: score += 8

        vol_ratio = h1_last['volume'] / (h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1)
        if vol_ratio >= 2.0: score += 20
        elif vol_ratio >= 1.5: score += 14
        elif vol_ratio >= 1.2: score += 8

        if h4_last['close'] > h4_last['ema20']: score += 10
        return min(max(score, 0), 100)

# -----------------------------------------------------------------------------
# 7. SETUP DETECTOR (3 LONG SETUPS & ANTI-MISS ENGINE)
# -----------------------------------------------------------------------------
class SetupDetector:
    @staticmethod
    def detect_setup_1_early_reversal(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> Tuple[bool, bool, Dict[str, Any]]:
        if df_h1 is None or df_h4 is None or len(df_h1) < 30 or len(df_h4) < 20:
            return False, False, {}
        h1_last, h4_last = df_h1.iloc[-2], df_h4.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)

        high_12 = df_h1['high'].iloc[-14:-2].max()
        cond_breakout = h1_last['close'] > high_12
        vol_sma = h1_last['vol_sma20'] if not np.isnan(h1_last['vol_sma20']) else 1
        cond_vol = h1_last['volume'] > (1.5 * vol_sma)
        h1_valid = cond_breakout and cond_vol and (h1_last['close'] - high_12 <= 1.5 * atr_h1)
        if not h1_valid: return False, False, {}

        is_choch, _, h4_low = TechnicalAnalyzer.check_h4_bullish_choch(df_h4)
        h4_confirmed = is_choch and (h4_last['rsi'] > 50) and (h4_last['plus_di'] > h4_last['minus_di'])

        _, lows = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
        sl_low = lows[-1]['price'] if lows else h4_low

        details = {"setup_name": "SETUP 1 – EARLY REVERSAL", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "structure_low": sl_low, "atr_h1": atr_h1, "vol_ratio": round(h1_last['volume']/vol_sma, 2)}
        if h1_valid and h4_confirmed: return True, False, details
        elif h1_valid: return False, True, details
        return False, False, {}

    @staticmethod
    def detect_setup_2_accumulation_breakout(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> Tuple[bool, bool, Dict[str, Any]]:
        if df_h1 is None or len(df_h1) < 50: return False, False, {}
        h1_last = df_h1.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)
        base_df = df_h1.iloc[-30:-2]
        base_high, base_low = base_df['high'].max(), base_df['low'].min()

        if (base_high - base_low) <= (5.0 * atr_h1) and h1_last['close'] > (base_high + 0.1 * atr_h1):
            vol_sma = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1
            if h1_last['volume'] > (1.8 * vol_sma) and (55 <= h1_last.get('rsi', 50) <= 72):
                details = {"setup_name": "SETUP 2 – ACCUMULATION BREAKOUT", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "base_low": base_low, "base_high": base_high, "atr_h1": atr_h1, "vol_ratio": round(h1_last['volume']/vol_sma, 2)}
                return True, False, details
        return False, False, {}

    @staticmethod
    def detect_setup_3_breakout_retest(df_h1: pd.DataFrame, df_h4: pd.DataFrame, prior_breakout: float) -> Tuple[bool, Dict[str, Any]]:
        if df_h1 is None or len(df_h1) < 20 or prior_breakout <= 0: return False, {}
        h1_last = df_h1.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)
        recent_low = df_h1['low'].iloc[-8:-2].min()
        if abs(recent_low - prior_breakout) <= (0.3 * atr_h1) and h1_last['close'] > h1_last['open']:
            details = {"setup_name": "SETUP 3 – BREAKOUT RETEST", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "retest_low": recent_low, "atr_h1": atr_h1, "vol_ratio": 1.3}
            return True, details
        return False, {}

# -----------------------------------------------------------------------------
# 8. SMART SL & SMART TP ENGINE
# -----------------------------------------------------------------------------
class SmartRiskEngine:
    @staticmethod
    def calculate_sl(setup_name: str, details: Dict[str, Any]) -> Tuple[float, str, bool]:
        entry, atr = details["paper_entry"], details["atr_h1"]
        if "SETUP 1" in setup_name: struct_low = details.get("structure_low", entry * 0.95); reason = "H1 Swing Low - 0.35 ATR"
        elif "SETUP 2" in setup_name: struct_low = details.get("base_low", entry * 0.95); reason = "Base Low - 0.35 ATR"
        else: struct_low = details.get("retest_low", entry * 0.95); reason = "Retest Low - 0.35 ATR"
        sl = struct_low - (0.35 * atr)
        if sl >= entry: return 0.0, "Invalid SL", False
        return round(sl, 4), reason, True

    @staticmethod
    def calculate_tp(df_h1: pd.DataFrame, df_h4: pd.DataFrame, entry: float, sl: float) -> Tuple[float, float, float, float]:
        risk = entry - sl
        highs, _ = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
        res = sorted([h['price'] for h in highs if h['price'] > entry])
        tp1 = res[0] if res else entry + (2.0 * risk)
        tp2 = res[1] if len(res) >= 2 else entry + (3.5 * risk)
        return round(tp1, 4), round(tp2, 4), round((tp1 - entry)/risk, 2), round((tp2 - entry)/risk, 2)

# -----------------------------------------------------------------------------
# 9. TRAILING STOP & MONITORING ENGINE
# -----------------------------------------------------------------------------
class TrailingAndMonitorEngine:
    @staticmethod
    def calculate_trailing_stop(df_h1: pd.DataFrame, df_h4: pd.DataFrame, highest: float, prev: float) -> float:
        atr_h1 = df_h1['atr'].iloc[-2] if not np.isnan(df_h1['atr'].iloc[-2]) else (highest * 0.01)
        candidate = highest - (2.5 * atr_h1)
        return max(prev, round(candidate, 4))

# -----------------------------------------------------------------------------
# 10. TELEGRAM NOTIFICATION SERVICE
# -----------------------------------------------------------------------------
class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token, self.chat_id = token, chat_id
        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send_message_sync(self, text: str):
        if not self.token or self.token.startswith("YOUR_"): return
        try: requests.post(self.api_url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=8)
        except Exception as e: logger.error(f"Lỗi gửi Telegram: {e}")

    def notify_signal(self, data: Dict[str, Any]):
        chart_url = f"https://www.bybit.com/trade/usdt/{data['symbol']}"
        msg = f"""🚀 <b>BYBIT H1 SMART WAVE PRO – FULL AUTO</b>
🪙 <b>Coin:</b> #{data['symbol']}
🟢 <b>{data['setup_name']}</b>
⏱ <b>Timeframe:</b> H1

💰 <b>Signal Price:</b> {data['signal_price']}
📍 <b>Paper Entry:</b> {data['paper_entry']}
🛡 <b>SL Structure:</b> {data['sl_price']} ({data['sl_reason']})
🎯 <b>TP1:</b> {data['tp1']} (R: {data['tp1_rr']}) | <b>TP2:</b> {data['tp2']} (R: {data['tp2_rr']})
💪 <b>Wave Strength:</b> {data['wave_score']}/100
✅ <b>Status:</b> CONFIRMED – PAPER SIGNAL

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{data['symbol']}</a>"""
        self.send_message_sync(msg)

    def notify_tp_update(self, symbol: str, gain: float, score: int, status: str, trailing: float):
        chart_url = f"https://www.bybit.com/trade/usdt/{symbol}"
        msg = f"""🔥 <b>SMART TP & TRAILING UPDATE</b>
🪙 <b>Coin:</b> #{symbol}
📈 <b>Mức tăng hiện tại:</b> +{gain:.2f}%
💪 <b>Wave Strength:</b> {score}/100
🛡 <b>Trailing Stop:</b> {trailing}
✅ <b>Trạng thái:</b> {status}

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{symbol}</a>"""
        self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 11. MARKET SCANNER & FULL AUTO SCHEDULER
# -----------------------------------------------------------------------------
class MarketScanner:
    def __init__(self):
        self.symbols_cache = []
        self.last_refresh = 0

    def refresh_symbols(self):
        now = time.time()
        if not self.symbols_cache or (now - self.last_refresh) > (SYMBOL_REFRESH_HOURS * 3600):
            logger.info("Đang tải danh sách Crypto USDT Perpetual từ Bybit...")
            self.symbols_cache = bybit.get_all_crypto_usdt_perpetual_symbols()
            self.last_refresh = now

    def scan_market(self):
        SYSTEM_STATE["is_scanning"] = True
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        self.refresh_symbols()
        total = len(self.symbols_cache)
        ok, errors, found = 0, 0, 0

        logger.info(f"=== [FULL AUTO] BẮT ĐẦU QUÉT THỊ TRƯỜNG: {total} COIN ===")
        btc_gain = bybit.get_btc_24h_change()

        for item in self.symbols_cache:
            symbol = item["symbol"]
            try:
                df_h1 = bybit.fetch_klines(symbol, "60", 120)
                df_h4 = bybit.fetch_klines(symbol, "240", 80)
                if df_h1 is None or len(df_h1) < 50 or df_h4 is None or len(df_h4) < 20: continue

                df_h1, df_h4 = TechnicalAnalyzer.add_indicators(df_h1), TechnicalAnalyzer.add_indicators(df_h4)
                ok += 1

                # Quản lý lệnh đang chạy
                active = db.get_signal_by_symbol(symbol)
                if active and active["state"] not in ('FINISHED', 'INVALIDATED'):
                    self._process_active(active, df_h1, df_h4, btc_gain)
                    continue

                # Quét Setup mới
                is_conf, is_watch, details = SetupDetector.detect_setup_1_early_reversal(df_h1, df_h4)
                if is_conf: self._create_signal(symbol, details, df_h1, df_h4, btc_gain); found += 1; continue
                elif is_watch: db.add_watch(symbol, details["setup_name"], details["signal_price"])

                is_conf2, _, details2 = SetupDetector.detect_setup_2_accumulation_breakout(df_h1, df_h4)
                if is_conf2: self._create_signal(symbol, details2, df_h1, df_h4, btc_gain); found += 1; continue

                watch = next((w for w in db.get_watch_list() if w["symbol"] == symbol), None)
                if watch:
                    is_conf3, details3 = SetupDetector.detect_setup_3_breakout_retest(df_h1, df_h4, watch["watch_price"])
                    if is_conf3: self._create_signal(symbol, details3, df_h1, df_h4, btc_gain); db.delete_watch(symbol); found += 1

            except Exception as e:
                logger.error(f"Lỗi quét {symbol}: {e}")
                errors += 1
            time.sleep(0.03)

        SYSTEM_STATE["total_symbols_scanned"] = total
        SYSTEM_STATE["scanned_ok"] = ok
        SYSTEM_STATE["api_errors"] = errors
        SYSTEM_STATE["signals_found"] += found
        SYSTEM_STATE["is_scanning"] = False
        db.log_scan(total, ok, errors, total - ok, found)
        logger.info(f"=== [FULL AUTO] HOÀN TẤT QUÉT: T Tìm thấy {found} tín hiệu ===")

    def _create_signal(self, symbol: str, details: Dict, df_h1, df_h4, btc_gain):
        details["symbol"] = symbol
        sl, reason, valid = SmartRiskEngine.calculate_sl(details["setup_name"], details)
        if not valid: return
        tp1, tp2, rr1, rr2 = SmartRiskEngine.calculate_tp(df_h1, df_h4, details["paper_entry"], sl)
        score = WaveStrengthEngine.calculate_score(df_h1, df_h4, btc_gain)

        sig = {"symbol": symbol, "setup_name": details["setup_name"], "state": "CONFIRMED", "signal_price": details["signal_price"], "paper_entry": details["paper_entry"], "sl_price": sl, "sl_reason": reason, "tp1": tp1, "tp2": tp2, "tp1_rr": rr1, "tp2_rr": rr2, "wave_score": score}
        db.save_signal(sig)
        telegram_service.notify_signal(sig)

    def _process_active(self, sig: Dict, df_h1, df_h4, btc_gain):
        symbol, entry, sl = sig["symbol"], sig["paper_entry"], sig["sl_price"]
        close, high, low = df_h1['close'].iloc[-2], df_h1['high'].iloc[-2], df_h1['low'].iloc[-2]

        if low <= sl:
            db.update_signal_state(symbol, "FINISHED")
            return

        gain = ((close - entry) / entry) * 100
        max_gain = max(sig.get("max_gain_pct", 0), gain)
        trailing = TrailingAndMonitorEngine.calculate_trailing_stop(df_h1, df_h4, high, sig.get("trailing_stop", 0))

        if trailing > 0 and close < trailing:
            db.update_signal_state(symbol, "FINISHED", max_gain=max_gain, trailing_stop=trailing)
            return

        score = WaveStrengthEngine.calculate_score(df_h1, df_h4, btc_gain)
        m10, m15 = sig.get("notified_m10", 0), sig.get("notified_m15", 0)

        if gain >= 10.0 and not m10:
            telegram_service.notify_tp_update(symbol, gain, score, "EXTENDED WAVE (+10%)", trailing)
            db.update_signal_state(symbol, "SMART_TP", max_gain=max_gain, trailing_stop=trailing, m10=1)
        elif gain >= 15.0 and not m15:
            telegram_service.notify_tp_update(symbol, gain, score, "EXTENDED WAVE (+15%)", trailing)
            db.update_signal_state(symbol, "EXTENDED", max_gain=max_gain, trailing_stop=trailing, m15=1)
        else:
            db.update_signal_state(symbol, sig["state"], max_gain=max_gain, trailing_stop=trailing)

scanner = MarketScanner()

# Scheduler tự động tính toán thời gian đến phút 01 của mỗi giờ tiếp theo
async def full_auto_scheduler():
    logger.info("Đã khởi chạy Full Auto 24/7 Scheduler...")
    while True:
        try:
            now = datetime.now(timezone.utc)
            # Tính thời điểm đầu giờ tiếp theo + 90 giây (phút 01:30)
            next_hour = (now + timedelta(hours=1)).replace(minute=1, second=30, microsecond=0)
            sleep_seconds = (next_hour - now).total_seconds()
            SYSTEM_STATE["next_scan_time"] = next_hour.strftime('%Y-%m-%d %H:%M:%S UTC')
            
            logger.info(f"Lên lịch quét kế tiếp vào: {SYSTEM_STATE['next_scan_time']} (Chờ {int(sleep_seconds)}s)")
            await asyncio.sleep(sleep_seconds)

            if not SCAN_LOCK.locked():
                async with SCAN_LOCK:
                    await asyncio.to_thread(scanner.scan_market)
        except Exception as e:
            logger.error(f"Lỗi trong Full Auto Scheduler: {e}")
            await asyncio.sleep(60)

# -----------------------------------------------------------------------------
# 12. TELEGRAM COMMAND HANDLERS
# -----------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "🚀 <b>BYBIT H1 SMART WAVE PRO – FULL AUTO 24/7</b>\n\nBot đang hoạt động tự động ngầm 24/7. Lệnh hữu ích:\n/status - Trạng thái quét tự động\n/active - Danh sách Setup đang theo dõi\n/stats - Thống kê tín hiệu"
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Vòng quét tự động đang chạy...")
        return
    await update.message.reply_text("🔄 Kích hoạt quét thủ công...")
    async with SCAN_LOCK:
        await asyncio.to_thread(scanner.scan_market)
    await update.message.reply_text("✅ Quét thủ công hoàn tất!")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = f"""📊 <b>TRẠNG THÁI HỆ THỐNG FULL AUTO</b>
🤖 <b>Trạng thái quét:</b> {'Đang chạy...' if SYSTEM_STATE['is_scanning'] else 'Đang chờ (Idle)'}
⏱ <b>Lần quét gần nhất:</b> {SYSTEM_STATE['last_scan_time']}
⏳ <b>Lần quét kế tiếp:</b> {SYSTEM_STATE['next_scan_time']}
🌐 <b>Số coin quét thành công:</b> {SYSTEM_STATE['scanned_ok']} / {SYSTEM_STATE['total_symbols_scanned']}
🎯 <b>Tổng tín hiệu đã phát hiện:</b> {SYSTEM_STATE['signals_found']}"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    signals = db.get_active_signals_list()
    if not signals:
        await update.message.reply_text("Không có Setup nào đang theo dõi.")
        return
    msg = "🟢 <b>SETUP ĐANG THEO DÕI:</b>\n\n"
    for s in signals:
        msg += f"• <b>#{s['symbol']}</b> | {s['setup_name']}\n  Entry: {s['paper_entry']} | Max Gain: +{s['max_gain_pct']:.2f}%\n"
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with db.get_connection() as conn:
        rows = [dict(r) for r in conn.cursor().execute("SELECT * FROM signals").fetchall()]
    total = len(rows)
    win10 = sum(1 for r in rows if r["max_gain_pct"] >= 10.0)
    await update.message.reply_text(f"📈 <b>THỐNG KÊ:</b>\nTổng tín hiệu: {total}\nĐạt +10%: {win10} ({round(win10/total*100, 2) if total else 0}%)", parse_mode="HTML")

# -----------------------------------------------------------------------------
# 13. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG FULL AUTO 24/7 BOT ")
    logger.info("==================================================")

    # Chạy Flask Server keep-alive
    threading.Thread(target=run_flask_server, daemon=True).start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN!")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("active", cmd_active))
    app.add_handler(CommandHandler("stats", cmd_stats))

    async def main_async():
        # Khởi chạy Scheduler tự động chạy ngầm theo giờ đóng nến
        asyncio.create_task(full_auto_scheduler())
        
        # Ngay khi khởi động, chạy ngay 1 vòng quét đầu tiên lập tức
        asyncio.create_task(asyncio.to_thread(scanner.scan_market))

        await app.initialize()
        await app.start()
        logger.info("Bot Full Auto đã sẵn sàng 24/7!")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Đừng bot...")

if __name__ == "__main__":
    main()
