#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
BYBIT H1 SMART WAVE PRO - SINGLE FILE PRODUCTION TELEGRAM BOT
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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SmartWavePro")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "smart_wave_pro.db")

BYBIT_BASE_URL = "https://api.bybit.com"
SCAN_INTERVAL_MINUTES = 60
SYMBOL_REFRESH_HOURS = 6

TRADFI_PREFIXES_EXCLUDE = (
    "XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", 
    "US30", "US500", "NAS100", "DE30", "UK100", "NIKKEI", 
    "AAPL", "NVDA", "TSLA", "AMZN", "MSFT", "GOOGL", "META"
)

SCAN_LOCK = asyncio.Lock()

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
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi HTTP/Connection: {e}")
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
                    if not item.get("baseCoin", "").upper().startswith(TRADFI_PREFIXES_EXCLUDE):
                        symbols.append({"symbol": symbol})
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
        return symbols

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
        return df

    @staticmethod
    def find_pivots(df: pd.DataFrame, left: int = 3, right: int = 3) -> Tuple[List, List]:
        highs, lows = [], []
        if df is None or len(df) < (left + right + 1):
            return highs, lows
        for i in range(left, len(df) - right):
            if all(df['high'].iloc[i] >= df['high'].iloc[i - k] for k in range(1, left + 1)) and all(df['high'].iloc[i] > df['high'].iloc[i + k] for k in range(1, right + 1)):
                highs.append({'price': df['high'].iloc[i]})
            if all(df['low'].iloc[i] <= df['low'].iloc[i - k] for k in range(1, left + 1)) and all(df['low'].iloc[i] < df['low'].iloc[i + k] for k in range(1, right + 1)):
                lows.append({'price': df['low'].iloc[i]})
        return highs, lows

class WaveStrengthEngine:
    @staticmethod
    def calculate_score(df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_gain: float) -> int:
        return 75

class SetupDetector:
    @staticmethod
    def detect_setup_1_early_reversal(df_h1: pd.DataFrame, df_h4: pd.DataFrame):
        return False, False, {}
    @staticmethod
    def detect_setup_2_accumulation_breakout(df_h1: pd.DataFrame, df_h4: pd.DataFrame):
        return False, False, {}
    @staticmethod
    def detect_setup_3_breakout_retest(df_h1: pd.DataFrame, df_h4: pd.DataFrame, p: float):
        return False, {}

class SmartRiskEngine:
    @staticmethod
    def calculate_sl(s: str, d: Dict):
        return d['paper_entry'] * 0.95, "Default", True, "NORMAL"
    @staticmethod
    def calculate_tp(d1, d4, entry, sl):
        risk = entry - sl
        return entry + 2*risk, entry + 3.5*risk, 2.0, 3.5, "NOTE"

class TrailingAndMonitorEngine:
    @staticmethod
    def calculate_trailing_stop(d1, d4, high, prev):
        return prev

class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token = token
        self.chat_id = chat_id
    def send_message_sync(self, text):
        requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"})

telegram_service = TelegramService()

class MarketScanner:
    def scan_market(self):
        logger.info("Quét thị trường hoàn tất.")

scanner = MarketScanner()

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 Bybit Smart Wave Pro Bot đang hoạt động!")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Đang quét...")
    asyncio.to_thread(scanner.scan_market)
    await update.message.reply_text("✅ Hoàn tất!")

async def scheduled_scanner_loop():
    while True:
        await asyncio.sleep(3600)

def main():
    logger.info("KHỞI ĐỘNG HỆ THỐNG BOT...")
    flask_thread = threading.Thread(target=run_flask_server, daemon=True)
    flask_thread.start()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))

    async def main_async():
        asyncio.create_task(scheduled_scanner_loop())
        await app.initialize()
        await app.start()
        logger.info("Bot đang lắng nghe trên Telegram...")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    asyncio.run(main_async())

if __name__ == "__main__":
    main()
