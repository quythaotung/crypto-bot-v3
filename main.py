#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
BYBIT H1 SMART WAVE PRO - STRICT METADATA CRYPTO FILTERING EDITION
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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

# -----------------------------------------------------------------------------
# 1. LOGGING & CONFIGURATION
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SmartWaveProCryptoOnly")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "smart_wave_pro_crypto.db")

BYBIT_BASE_URL = "https://api.bybit.com"
SYMBOL_REFRESH_HOURS = 6

SYSTEM_STATE = {
    "is_scanning": False,
    "last_scan_time": "Chưa quét",
    "next_scan_time": "Đang tính toán...",
    "total_raw_symbols": 0,
    "crypto_valid": 0,
    "tradfi_blocked": 0,
    "unknown_symbols": 0,
    "scanned_ok": 0,
    "api_errors": 0,
    "signals_found": 0
}

SCAN_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------
# 2. FLASK KEEP-ALIVE SERVER
# -----------------------------------------------------------------------------
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health_check():
    return jsonify({
        "status": "online",
        "system": "BYBIT H1 SMART WAVE PRO CRYPTO ONLY",
        "is_scanning": SYSTEM_STATE["is_scanning"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

def run_flask_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------------------------------------------------------
# 3. DATABASE MANAGER
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
                crypto_valid INTEGER,
                tradfi_blocked INTEGER,
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
                            m10: int = None, m15: int = None, m20: int = None):
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

    def log_scan(self, total: int, crypto: int, tradfi: int, signals: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO scan_logs (total_symbols, crypto_valid, tradfi_blocked, signals_found)
            VALUES (?, ?, ?, ?)
            """, (total, crypto, tradfi, signals))
            conn.commit()

db = DatabaseManager()

# -----------------------------------------------------------------------------
# 4. BYBIT V5 CLIENT & METADATA ASSET CLASSIFIER
# -----------------------------------------------------------------------------
class BybitClient:
    def __init__(self, base_url: str = BYBIT_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "SmartWaveProCryptoOnly/1.0"})

    def _request(self, endpoint: str, params: Dict[str, Any] = None, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=12)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result", {})
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi API Bybit {endpoint}: {e}")
                time.sleep(1.5 * (attempt + 1))
        return None

    def get_verified_crypto_usdt_symbols(self) -> Tuple[List[Dict[str, Any]], int, int, int]:
        """
        Quét 100% hợp đồng Linear Perpetual và lọc nghiêm ngặt TradFi/Stock/Forex/Commodity
        dựa trên metadata Bybit (baseCoin, quoteCoin, symbol, v.v.)
        """
        raw_symbols = []
        cursor = ""
        while True:
            params = {"category": "linear", "status": "Trading", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            res = self._request("/v5/market/instruments-info", params=params)
            if not res or "list" not in res:
                break
            for item in res["list"]:
                if item.get("contractType", "") == "LinearPerpetual" and item.get("settleCoin", "") == "USDT":
                    raw_symbols.append(item)
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break

        SYSTEM_STATE["total_raw_symbols"] = len(raw_symbols)
        crypto_valid = []
        tradfi_blocked = 0
        unknown_count = 0

        # Danh sách tiền tố/từ khóa TradFi, Cổ phiếu, Forex, Hàng hóa chắc chắn không phải Crypto
        tradfi_keywords = {
            "XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", 
            "US30", "US500", "NAS100", "DE30", "UK100", "NIKKEI", "OIL", "WTI", "BRENT",
            "AAPL", "NVDA", "TSLA", "AMZN", "MSFT", "GOOGL", "META", "NFLX", "AMD",
            "LRCX", "NOKIA", "COIN", "MSTR", "SPY", "QQQ"
        }

        for item in raw_symbols:
            symbol = item.get("symbol", "").upper()
            base_coin = item.get("baseCoin", "").upper()
            
            # Kiểm tra nếu thuộc danh sách TradFi / Cổ phiếu kinh điển
            if base_coin in tradfi_keywords or any(symbol.startswith(prefix) for prefix in ["XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "US30", "US500", "NAS100"]):
                tradfi_blocked += 1
                continue

            # Kiểm tra các tiêu chí đặc thù của Cổ phiếu TradFi (ví dụ mã chứa tên công ty thông dụng hoặc kết hiệu lạ)
            if base_coin in {"NOKIA", "LRCX", "AAPL", "TSLA", "NVDA", "AMZN", "MSFT"}:
                tradfi_blocked += 1
                continue

            # Nếu thỏa mãn điều kiện là coin số (Crypto)
            if base_coin and len(base_coin) <= 10:
                crypto_valid.append({"symbol": symbol})
            else:
                unknown_count += 1
                logger.warning(f"Phát hiện tài sản UNKNOWN (Bỏ qua để an toàn): {symbol} (Base: {base_coin})")

        SYSTEM_STATE["crypto_valid"] = len(crypto_valid)
        SYSTEM_STATE["tradfi_blocked"] = tradfi_blocked
        SYSTEM_STATE["unknown_symbols"] = unknown_count

        return crypto_valid, len(raw_symbols), tradfi_blocked, unknown_count

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
# 5. TECHNICAL ANALYSIS & STRUCTURE ANALYZER (H1, H4, D1)
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
    def check_structure_bullish(df_tf: pd.DataFrame) -> bool:
        if df_tf is None or len(df_tf) < 20:
            return True
        highs, lows = TechnicalAnalyzer.find_pivots(df_tf, left=2, right=2)
        if len(highs) >= 2 and len(lows) >= 2:
            return highs[-1]['price'] >= highs[-2]['price'] and lows[-1]['price'] >= lows[-2]['price']
        return df_tf['close'].iloc[-2] > df_tf['ema20'].iloc[-2]

# -----------------------------------------------------------------------------
# 6. WAVE STRENGTH SCORE ENGINE
# -----------------------------------------------------------------------------
class WaveStrengthEngine:
    @staticmethod
    def calculate_score(df_h1: pd.DataFrame, df_h4: pd.DataFrame, df_d1: pd.DataFrame, btc_gain: float) -> int:
        score = 0
        if df_h1 is None or df_h4 is None: return 50
        h1_last, h4_last = df_h1.iloc[-2], df_h4.iloc[-2]

        if TechnicalAnalyzer.check_structure_bullish(df_h4): score += 25
        if TechnicalAnalyzer.check_structure_bullish(df_d1): score += 15

        adx = h4_last.get('adx', 20)
        if adx >= 25 and h4_last.get('plus_di', 0) > h4_last.get('minus_di', 0): score += 20
        elif adx >= 20: score += 10

        if h1_last.get('rsi', 50) >= 55: score += 15
        vol_ratio = h1_last['volume'] / (h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1)
        if vol_ratio >= 1.8: score += 15
        elif vol_ratio >= 1.2: score += 8

        if h4_last['close'] > h4_last['ema20']: score += 10
        return min(max(score, 0), 100)

# -----------------------------------------------------------------------------
# 7. SETUP DETECTOR (STRICT QUALITY CONTROL)
# -----------------------------------------------------------------------------
class SetupDetector:
    @staticmethod
    def detect_all_setups(df_h1: pd.DataFrame, df_h4: pd.DataFrame, df_d1: pd.DataFrame) -> Tuple[bool, bool, Dict[str, Any]]:
        if df_h1 is None or df_h4 is None or len(df_h1) < 40 or len(df_h4) < 20:
            return False, False, {}

        h1_last = df_h1.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)
        vol_sma = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1

        # Setup 2: Accumulation Breakout
        base_df = df_h1.iloc[-30:-2]
        base_high, base_low = base_df['high'].max(), base_df['low'].min()
        if (base_high - base_low) <= (5.0 * atr_h1) and h1_last['close'] > (base_high + 0.1 * atr_h1):
            if h1_last['volume'] > (1.8 * vol_sma) and 55 <= h1_last.get('rsi', 50) <= 72:
                details = {"setup_name": "SETUP 2 – ACCUMULATION BREAKOUT", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "base_low": base_low, "base_high": base_high, "atr_h1": atr_h1, "vol_ratio": round(h1_last['volume']/vol_sma, 2)}
                return True, False, details

        # Setup 1: Early Reversal
        high_12 = df_h1['high'].iloc[-14:-2].max()
        if h1_last['close'] > high_12 and h1_last['volume'] > (1.5 * vol_sma) and (h1_last['close'] - high_12 <= 1.5 * atr_h1):
            _, lows = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
            sl_low = lows[-1]['price'] if lows else df_h1['low'].iloc[-6:-2].min()
            details = {"setup_name": "SETUP 1 – EARLY REVERSAL", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "structure_low": sl_low, "atr_h1": atr_h1, "vol_ratio": round(h1_last['volume']/vol_sma, 2)}
            
            h4_confirmed = TechnicalAnalyzer.check_structure_bullish(df_h4)
            if h4_confirmed:
                return True, False, details
            else:
                return False, True, details

        return False, False, {}

    @staticmethod
    def detect_setup_3_retest(df_h1: pd.DataFrame, prior_breakout: float) -> Tuple[bool, Dict[str, Any]]:
        if df_h1 is None or len(df_h1) < 20 or prior_breakout <= 0: return False, {}
        h1_last = df_h1.iloc[-2]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (h1_last['close'] * 0.01)
        recent_low = df_h1['low'].iloc[-8:-2].min()
        if abs(recent_low - prior_breakout) <= (0.3 * atr_h1) and h1_last['close'] > h1_last['open']:
            details = {"setup_name": "SETUP 3 – BREAKOUT RETEST", "signal_price": h1_last['close'], "paper_entry": df_h1['open'].iloc[-1], "retest_low": recent_low, "atr_h1": atr_h1, "vol_ratio": 1.2}
            return True, details
        return False, {}

# -----------------------------------------------------------------------------
# 8. SMART SL & SMART TP ENGINE
# -----------------------------------------------------------------------------
class SmartRiskEngine:
    @staticmethod
    def calculate_sl(setup_name: str, details: Dict[str, Any]) -> Tuple[float, str, bool, str]:
        entry, atr = details["paper_entry"], details["atr_h1"]
        if "SETUP 1" in setup_name: struct_low = details.get("structure_low", entry * 0.95); reason = "H1 Swing Low - 0.35 ATR"
        elif "SETUP 2" in setup_name: struct_low = details.get("base_low", entry * 0.95); reason = "Base Low - 0.35 ATR"
        else: struct_low = details.get("retest_low", entry * 0.95); reason = "Retest Low - 0.35 ATR"
        
        sl = struct_low - (0.35 * atr)
        if sl >= entry: return 0.0, "Invalid SL", False, "INVALID"

        risk = entry - sl
        risk_atr = risk / atr if atr > 0 else 1.0
        sl_tag = "NORMAL"
        if risk_atr > 4.5:
            sl_tag = "SL TOO WIDE"

        return round(sl, 4), reason, True, sl_tag

    @staticmethod
    def calculate_tp(df_h1: pd.DataFrame, df_h4: pd.DataFrame, df_d1: pd.DataFrame, entry: float, sl: float) -> Tuple[float, str, float, float, str, float]:
        risk = entry - sl
        h1_highs, _ = TechnicalAnalyzer.find_pivots(df_h1, left=2, right=2)
        h4_highs, _ = TechnicalAnalyzer.find_pivots(df_h4, left=2, right=2) if df_h4 is not None else ([], [])
        d1_highs, _ = TechnicalAnalyzer.find_pivots(df_d1, left=2, right=2) if df_d1 is not None else ([], [])

        res = sorted(list(set([h['price'] for h in h1_highs + h4_highs + d1_highs if h['price'] > entry])))
        
        tp1 = res[0] if len(res) >= 1 else entry + (2.0 * risk)
        tp1_note = "CONFIRMED RESISTANCE" if len(res) >= 1 else "ATR PROJECTION"
        rr_tp1 = round((tp1 - entry) / risk, 2)

        if len(res) >= 2:
            tp2 = res[1]
            tp2_note = "CONFIRMED RESISTANCE"
        else:
            tp2 = entry + (3.5 * risk)
            tp2_note = "ATR PROJECTION"
        rr_tp2 = round((tp2 - entry) / risk, 2)

        return round(tp1, 4), tp1_note, rr_tp1, round(tp2, 4), tp2_note, rr_tp2

# -----------------------------------------------------------------------------
# 9. TRAILING STOP & MONITORING ENGINE
# -----------------------------------------------------------------------------
class TrailingAndMonitorEngine:
    @staticmethod
    def calculate_trailing_stop(df_h1: pd.DataFrame, df_h4: pd.DataFrame, highest: float, prev: float) -> float:
        atr_h1 = df_h1['atr'].iloc[-2] if not np.isnan(df_h1['atr'].iloc[-2]) else (highest * 0.01)
        atr_h4 = df_h4['atr'].iloc[-2] if df_h4 is not None and not np.isnan(df_h4['atr'].iloc[-2]) else (highest * 0.02)
        candidate = highest - max(2.5 * atr_h1, 0.8 * atr_h4)
        return max(prev, round(candidate, 4))

# -----------------------------------------------------------------------------
# 10. TELEGRAM NOTIFICATION SERVICE
# -----------------------------------------------------------------------------
class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token, self.chat_id = token, chat_id
        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send_message_sync(self, text: str):
        if not self.token or self.token.startswith("YOUR_") or not self.chat_id:
            logger.info(f"[TELEGRAM PREVIEW]: {text}")
            return
        try:
            resp = requests.post(self.api_url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=8)
            if resp.status_code != 200:
                logger.error(f"Lỗi Telegram API: {resp.text}")
        except Exception as e:
            logger.error(f"Lỗi kết nối Telegram: {e}")

    def notify_signal(self, data: Dict[str, Any]):
        chart_url = f"https://www.bybit.com/trade/usdt/{data['symbol']}"
        msg = f"""🚀 <b>BYBIT H1 SMART WAVE PRO – CRYPTO ONLY</b>
🪙 <b>Coin:</b> #{data['symbol']}
🟢 <b>{data['setup_name']}</b>
⏱ <b>Timeframe:</b> H1 / H4 / D1

💰 <b>Signal Price:</b> {data['signal_price']}
📍 <b>Paper Entry:</b> {data['paper_entry']}
🛡 <b>SL Structure:</b> {data['sl_price']} ({data['sl_reason']})

🎯 <b>TP1:</b> {data['tp1']} ({data['tp1_note']}) | <b>R:</b> {data['tp1_rr']}
🎯 <b>TP2:</b> {data['tp2']} ({data['tp2_note']}) | <b>R:</b> {data['tp2_rr']}

💪 <b>Wave Strength Score:</b> {data['wave_score']}/100
✅ <b>Status:</b> CONFIRMED – VERIFIED CRYPTO

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{data['symbol']}</a>"""
        self.send_message_sync(msg)

    def notify_tp_update(self, symbol: str, gain: float, score: int, status: str, trailing: float):
        chart_url = f"https://www.bybit.com/trade/usdt/{symbol}"
        msg = f"""🔥 <b>SMART TP & TRAILING UPDATE</b>
🪙 <b>Coin:</b> #{symbol}
📈 <b>Mức tăng hiện tại:</b> +{gain:.2f}%
💪 <b>Wave Strength Score:</b> {score}/100
🛡 <b>Trailing Stop:</b> {trailing}
✅ <b>Trạng thái:</b> {status}

🔗 <a href="{chart_url}">Mở biểu đồ Bybit #{symbol}</a>"""
        self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 11. MARKET SCANNER & 24/7 SCHEDULER
# -----------------------------------------------------------------------------
class MarketScanner:
    def scan_market(self):
        SYSTEM_STATE["is_scanning"] = True
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.info("=== [CRYPTO SCANNER] BẮT ĐẦU QUÉT TOÀN BỘ HỢP ĐỒNG TRÊN BYBIT ===")
        crypto_symbols, total_raw, tradfi_blocked, unknown_count = bybit.get_verified_crypto_usdt_symbols()
        
        ok, errors, found = 0, 0, 0
        btc_gain = bybit.get_btc_24h_change()

        for item in crypto_symbols:
            symbol = item["symbol"]
            try:
                df_h1 = bybit.fetch_klines(symbol, "60", 120)
                df_h4 = bybit.fetch_klines(symbol, "240", 80)
                df_d1 = bybit.fetch_klines(symbol, "D", 50)
                if df_h1 is None or len(df_h1) < 50 or df_h4 is None or len(df_h4) < 20:
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                df_h4 = TechnicalAnalyzer.add_indicators(df_h4)
                df_d1 = TechnicalAnalyzer.add_indicators(df_d1)
                ok += 1

                active = db.get_signal_by_symbol(symbol)
                if active and active["state"] not in ('FINISHED', 'INVALIDATED'):
                    self._process_active(active, df_h1, df_h4, df_d1, btc_gain)
                    continue

                is_conf, is_watch, details = SetupDetector.detect_all_setups(df_h1, df_h4, df_d1)
                if is_conf or is_watch:
                    self._create_signal(symbol, details, df_h1, df_h4, df_d1, btc_gain, is_watch)
                    found += 1
                    continue

                watch = next((w for w in db.get_watch_list() if w["symbol"] == symbol), None)
                if watch:
                    is_s3, details3 = SetupDetector.detect_setup_3_retest(df_h1, watch["watch_price"])
                    if is_s3:
                        self._create_signal(symbol, details3, df_h1, df_h4, df_d1, btc_gain, is_watch=False)
                        db.delete_watch(symbol)
                        found += 1

            except Exception as e:
                logger.error(f"Lỗi xử lý symbol {symbol}: {e}")
                errors += 1
            time.sleep(0.02)

        SYSTEM_STATE["scanned_ok"] = ok
        SYSTEM_STATE["api_errors"] = errors
        SYSTEM_STATE["signals_found"] += found
        SYSTEM_STATE["is_scanning"] = False
        db.log_scan(total_raw, len(crypto_symbols), tradfi_blocked, found)
        logger.info(f"=== [CRYPTO SCANNER] HOÀN TẤT: Hợp lệ={len(crypto_symbols)}, Chặn TradFi={tradfi_blocked}, Tìm thấy={found} ===")

    def _create_signal(self, symbol: str, details: Dict, df_h1, df_h4, df_d1, btc_gain, is_watch: bool):
        details["symbol"] = symbol
        sl, reason, valid, sl_tag = SmartRiskEngine.calculate_sl(details["setup_name"], details)
        if not valid: return
        
        tp1, tp1_note, rr1, tp2, tp2_note, rr2 = SmartRiskEngine.calculate_tp(df_h1, df_h4, df_d1, details["paper_entry"], sl)
        score = WaveStrengthEngine.calculate_score(df_h1, df_h4, df_d1, btc_gain)

        state = "CONFIRMED"
        if rr1 < 1.5:
            details["setup_name"] += " [LOW REWARD SPACE]"
            state = "WATCH / LOW REWARD"
        elif score < 70 or is_watch:
            state = "WATCH / LOW CONFIDENCE"

        sig = {
            "symbol": symbol, "setup_name": details["setup_name"], "state": state,
            "signal_price": details["signal_price"], "paper_entry": details["paper_entry"],
            "sl_price": sl, "sl_reason": reason,
            "tp1": tp1, "tp1_note": tp1_note, "tp2": tp2, "tp2_note": tp2_note,
            "tp1_rr": rr1, "tp2_rr": rr2, "wave_score": score
        }

        db.save_signal(sig)
        if state == "CONFIRMED":
            telegram_service.notify_signal(sig)

    def _process_active(self, sig: Dict, df_h1, df_h4, df_d1, btc_gain):
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
            telegram_service.notify_tp_update(symbol, gain, WaveStrengthEngine.calculate_score(df_h1, df_h4, df_d1, btc_gain), "TREND EXIT", trailing)
            return

        score = WaveStrengthEngine.calculate_score(df_h1, df_h4, df_d1, btc_gain)
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

async def full_auto_scheduler():
    logger.info("Đã kích hoạt Scheduler Quét Tự Động 24/7 (Đồng bộ phút 01 của mỗi giờ)...")
    while True:
        try:
            now = datetime.now(timezone.utc)
            next_hour = (now + timedelta(hours=1)).replace(minute=1, second=30, microsecond=0)
            sleep_sec = (next_hour - now).total_seconds()
            SYSTEM_STATE["next_scan_time"] = next_hour.strftime('%Y-%m-%d %H:%M:%S UTC')
            
            await asyncio.sleep(sleep_sec)
            if not SCAN_LOCK.locked():
                async with SCAN_LOCK:
                    await asyncio.to_thread(scanner.scan_market)
        except Exception as e:
            logger.error(f"Lỗi Scheduler: {e}")
            await asyncio.sleep(60)

# -----------------------------------------------------------------------------
# 12. TELEGRAM COMMAND HANDLERS
# -----------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 <b>BYBIT H1 SMART WAVE PRO – CRYPTO ONLY</b>\nBot hoạt động 24/7 chỉ quét thị trường Tiền Điện Tử.\n/status - Thống kê tài sản\n/active - Lệnh đang chạy", parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Vòng quét đang chạy...")
        return
    await update.message.reply_text("🔄 Quét thủ công bắt đầu...")
    async with SCAN_LOCK:
        await asyncio.to_thread(scanner.scan_market)
    await update.message.reply_text("✅ Hoàn tất!")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = 'Đang chạy' if SYSTEM_STATE['is_scanning'] else 'Đang chờ (Idle)'
    msg = f"""📊 <b>THỐNG KÊ BỘ LỌC TÀI SẢN BYBIT</b>
🤖 <b>Trạng thái quét:</b> {status_text}
⏱ <b>Lần quét gần nhất:</b> {SYSTEM_STATE['last_scan_time']}
⏳ <b>Lần quét kế tiếp:</b> {SYSTEM_STATE['next_scan_time']}
🌐 <b>Tổng số hợp đồng gốc:</b> {SYSTEM_STATE['total_raw_symbols']}
✅ <b>Crypto hợp lệ quét:</b> {SYSTEM_STATE['crypto_valid']}
🚫 <b>TradFi / Cổ phiếu đã chặn:</b> {SYSTEM_STATE['tradfi_blocked']}
❓ <b>Tài sản chưa xác định (Unknown):</b> {SYSTEM_STATE['unknown_symbols']}
🎯 <b>Tín hiệu đã phát hiện:</b> {SYSTEM_STATE['signals_found']}"""
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

# -----------------------------------------------------------------------------
# 13. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG CRYPTO ONLY 24/7 BOT ")
    logger.info("==================================================")

    threading.Thread(target=run_flask_server, daemon=True).start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN!")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("active", cmd_active))

    async def main_async():
        asyncio.create_task(full_auto_scheduler())
        asyncio.create_task(asyncio.to_thread(scanner.scan_market))

        await app.initialize()
        await app.start()
        logger.info("Bot Crypto Only đã sẵn sàng 24/7!")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Dừng bot...")

if __name__ == "__main__":
    main()
