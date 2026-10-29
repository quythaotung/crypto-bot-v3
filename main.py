#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
MEXC H1 SMART WAVE PRO - HIGH WINRATE LIQUIDITY SWEEP & SUPPORT RECLAIM
===============================================================================
"""

import os
import sys
import time
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

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
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
logger = logging.getLogger("MexcLiquiditySweepBot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "mexc_sweep_signals.db")

MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"
SYMBOL_REFRESH_HOURS = 6

SYSTEM_STATE = {
    "is_scanning": False,
    "last_scan_time": "Chưa quét",
    "next_scan_time": "Đang tính toán...",
    "total_raw_symbols": 0,
    "crypto_valid": 0,
    "tradfi_blocked": 0,
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
        "system": "MEXC H1 SWEEP PRO PLATFORM",
        "is_scanning": SYSTEM_STATE["is_scanning"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

def run_flask_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------------------------------------------------------
# 3. DATABASE MANAGER (SQLITE ROBUST)
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
                symbol TEXT NOT NULL,
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
                candle_timestamp INTEGER NOT NULL,
                telegram_sent INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, setup_name, candle_timestamp)
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

    def save_signal(self, data: Dict[str, Any]) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT OR IGNORE INTO signals (
                symbol, setup_name, state, timeframe, signal_price, paper_entry,
                sl_price, sl_reason, tp1, tp2, tp1_rr, tp2_rr, wave_score,
                max_gain_pct, trailing_stop, candle_timestamp, telegram_sent, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data["symbol"], data["setup_name"], data["state"], data.get("timeframe", "H1"),
                data["signal_price"], data["paper_entry"], data["sl_price"], data.get("sl_reason", ""),
                data.get("tp1", 0.0), data.get("tp2", 0.0), data.get("tp1_rr", 0.0), data.get("tp2_rr", 0.0),
                data.get("wave_score", 0), data.get("max_gain_pct", 0.0), data.get("trailing_stop", 0.0),
                data["candle_timestamp"], data.get("telegram_sent", 0)
            ))
            conn.commit()
            return cursor.lastrowid

    def update_telegram_sent_status(self, symbol: str, setup_name: str, candle_ts: int, status: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE signals SET telegram_sent = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE symbol = ? AND setup_name = ? AND candle_timestamp = ?
            """, (status, symbol, setup_name, candle_ts))
            conn.commit()

    def update_signal_state(self, symbol: str, state: str, max_gain: float = None, trailing_stop: float = None):
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
            cursor.execute("SELECT * FROM signals WHERE symbol=? ORDER BY id DESC LIMIT 1", (symbol,))
            row = cursor.fetchone()
            return dict(row) if row else None

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
# 4. MEXC FUTURES CLIENT
# -----------------------------------------------------------------------------
class MexcFuturesClient:
    def __init__(self, base_url: str = MEXC_FUTURES_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MexcSweepBot/1.0"})

    def _request(self, endpoint: str, params: Dict[str, Any] = None, retries: int = 3) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=12)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success", False) is True or data.get("code") == 0 or "data" in data:
                        return data.get("data", data)
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi API MEXC {endpoint}: {e}")
                time.sleep(1.5 * (attempt + 1))
        return None

    def get_verified_crypto_usdt_symbols(self) -> Tuple[List[Dict[str, Any]], int, int]:
        res = self._request("/api/v1/contract/detail")
        if not res or not isinstance(res, list):
            return [], 0, 0

        raw_symbols = res
        SYSTEM_STATE["total_raw_symbols"] = len(raw_symbols)
        crypto_valid = []
        tradfi_blocked = 0

        tradfi_keywords = {
            "XAU", "XAG", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", 
            "US30", "US500", "NAS100", "DE30", "UK100", "NIKKEI", "OIL", "WTI", "BRENT",
            "AAPL", "NVDA", "TSLA", "AMZN", "MSFT", "GOOGL", "META", "NFLX", "AMD"
        }

        for item in raw_symbols:
            symbol = item.get("symbol", "").upper()
            quote_coin = item.get("quoteCoin", "").upper()
            base_coin = item.get("baseCoin", "").upper()

            if quote_coin == "USDT" and item.get("state") == 0:
                if base_coin in tradfi_keywords or any(symbol.startswith(prefix) for prefix in ["XAU", "XAG", "EUR", "GBP"]):
                    tradfi_blocked += 1
                    continue
                crypto_valid.append({"symbol": symbol, "base_coin": base_coin})

        SYSTEM_STATE["crypto_valid"] = len(crypto_valid)
        SYSTEM_STATE["tradfi_blocked"] = tradfi_blocked
        return crypto_valid, len(raw_symbols), tradfi_blocked

    def fetch_klines(self, symbol: str, interval: str = "Min60", limit: int = 150) -> Optional[pd.DataFrame]:
        # MEXC Interval format: Min60 (H1), Min240 (H4), Day1 (D1)
        endpoint = f"/api/v1/contract/kline/{symbol}"
        res = self._request(endpoint, params={"interval": interval, "limit": limit})
        if not res or "time" not in res or not res["time"]:
            return None

        # MEXC trả về dạng dictionary các list song song
        times = res["time"]
        opens = res["open"]
        highs = res["high"]
        lows = res["low"]
        closes = res["close"]
        vols = res["vol"]

        rows = []
        for i in range(len(times)):
            rows.append({
                "open_time": int(times[i]) * 1000 if int(times[i]) < 10000000000 else int(times[i]),
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": float(vols[i])
            })

        df = pd.DataFrame(rows)
        df.sort_values(by="open_time", ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

mexc_client = MexcFuturesClient()

# -----------------------------------------------------------------------------
# 5. TECHNICAL ANALYSIS & INDICATORS
# -----------------------------------------------------------------------------
class TechnicalAnalyzer:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) < 30:
            return df
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema34'] = df['close'].ewm(span=34, adjust=False).mean()
        df['vol_sma20'] = df['volume'].rolling(window=20).mean()

        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['rsi'] = 100 - (100 / (1 + gain / loss.replace(0, 0.00001)))

        tr = pd.concat([
            df['high'] - df['low'], 
            (df['high'] - df['close'].shift(1)).abs(), 
            (df['low'] - df['close'].shift(1)).abs()
        ], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=14).mean()
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

# -----------------------------------------------------------------------------
# 6. HIGH WINRATE LIQUIDITY SWEEP SETUP DETECTOR & SCORING (100 PTS)
# -----------------------------------------------------------------------------
class LiquiditySweepDetector:
    @staticmethod
    def evaluate_setup(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> Tuple[bool, Dict[str, Any], int]:
        if df_h1 is None or len(df_h1) < 40:
            return False, {}, 0

        # Nến index = -2 (Nến H1 hoàn chỉnh vừa đóng)
        h1_last = df_h1.iloc[-2]
        open_p, high_p, low_p, close_p = h1_last['open'], h1_last['high'], h1_last['low'], h1_last['close']
        body_size = abs(close_p - open_p)
        lower_wick = min(open_p, close_p) - low_p
        total_range = high_p - low_p
        
        if total_range == 0:
            return False, {}, 0

        # Tìm các Swing Low cũ trước nến hiện tại (tính từ 30 nến trước)
        _, lows = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-2], left=2, right=2)
        if not lows:
            recent_swing_low = df_h1['low'].iloc[-25:-3].min()
        else:
            recent_swing_low = lows[-1]['price']

        # COND 1: Liquidity Sweep Check (Giá nhúng dưới Swing Low cũ nhưng rút chân đóng trên)
        is_sweep = low_p < recent_swing_low and close_p >= recent_swing_low
        # COND 2: Pin Bar / Wick Rejection Check (Râu dưới phải dài hơn 1.8x thân nến)
        is_wick_rejection = lower_wick >= (Body_ratio := 1.5 * body_size) or (lower_wick / total_range >= 0.5)

        if not (is_sweep or is_wick_rejection):
            return False, {}, 0

        # BỘ CHẤM ĐIỂM SỨC MẠNH LIQUIDITY SWEEP (100 ĐIỂM)
        score = 0

        # 1. Liquidity Sweep Đáy Swing Low (+25đ)
        if is_sweep:
            score += 25

        # 2. Reversal Candle / Râu rút chân mạnh (+20đ)
        if lower_wick >= (2.0 * body_size):
            score += 20
        elif lower_wick >= (1.5 * body_size):
            score += 12

        # 3. Support Reclaim (Đóng nến nằm hoàn toàn trên cản) (+15đ)
        if close_p > recent_swing_low:
            score += 15

        # 4. Volume Spike Expansion (+15đ)
        vol_sma = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1
        vol_ratio = h1_last['volume'] / vol_sma
        if vol_ratio >= 2.2:
            score += 15
        elif vol_ratio >= 1.5:
            score += 10

        # 5. EMA Confluence / Reclaim EMA34 (+15đ)
        if close_p >= h1_last.get('ema34', 0) or low_p <= h1_last.get('ema34', 0) <= close_p:
            score += 15

        # 6. RSI Recovery (+10đ)
        rsi = h1_last.get('rsi', 50)
        if 40 <= rsi <= 68:
            score += 10

        # Lọc ngắt cứng: Bắt buộc Score >= 80 và phải có ít nhất Sweep hoặc Wick Rejection cực đẹp
        if score >= 80:
            atr = h1_last['atr'] if not np.isnan(h1_last['atr']) else (close_p * 0.01)
            sl_price = round(low_p - (0.35 * atr), 4)
            entry_price = round(df_h1['open'].iloc[-1], 4)

            if sl_price >= entry_price:
                return False, {}, 0

            risk = entry_price - sl_price
            
            # Tính TP1 (2.0R) và TP2 (3.5R)
            tp1 = round(entry_price + (2.0 * risk), 4)
            tp2 = round(entry_price + (3.5 * risk), 4)

            details = {
                "setup_name": "H1 LIQUIDITY SWEEP & SUPPORT RECLAIM",
                "signal_price": close_p,
                "paper_entry": entry_price,
                "sl_price": sl_price,
                "sl_reason": "Low of Liquidity Sweep - 0.35 ATR Buffer",
                "tp1": tp1,
                "tp1_note": "Target 2.0R Zone",
                "tp1_rr": 2.0,
                "tp2": tp2,
                "tp2_note": "Target 3.5R Expansion",
                "tp2_rr": 3.5,
                "wave_score": score,
                "candle_timestamp": int(h1_last['open_time'])
            }
            return True, details, score

        return False, {}, score

# -----------------------------------------------------------------------------
# 7. TELEGRAM NOTIFICATION SERVICE
# -----------------------------------------------------------------------------
class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token, self.chat_id = token, chat_id
        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send_message_sync(self, text: str) -> bool:
        if not self.token or self.token.startswith("YOUR_") or not self.chat_id:
            logger.info(f"[TELEGRAM PREVIEW]: {text}")
            return True
        try:
            resp = requests.post(self.api_url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=8)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Lỗi kết nối Telegram: {e}")
            return False

    def notify_signal(self, data: Dict[str, Any]) -> bool:
        chart_url = f"https://futures.mexc.com/exchange/{data['symbol']}"
        msg = f"""🚀 <b>MEXC H1 SWEEP PRO – HIGH WINRATE SIGNAL</b>
🪙 <b>Coin:</b> #{data['symbol']}
🟢 <b>{data['setup_name']}</b>
⏱ <b>Timeframe:</b> H1

💰 <b>Signal Price:</b> {data['signal_price']}
📍 <b>Paper Entry:</b> {data['paper_entry']}
🛡 <b>SL Structure:</b> {data['sl_price']} ({data['sl_reason']})

🎯 <b>TP1:</b> {data['tp1']} ({data['tp1_note']}) | <b>R:</b> {data['tp1_rr']}
🎯 <b>TP2:</b> {data['tp2']} ({data['tp2_note']}) | <b>R:</b> {data['tp2_rr']}

💪 <b>Sweep Quality Score:</b> {data['wave_score']}/100
✅ <b>Status:</b> CONFIRMED – VERIFIED MEXC FUTURES

🔗 <a href="{chart_url}">Mở biểu đồ MEXC #{data['symbol']}</a>"""
        return self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 8. MARKET SCANNER & 24/7 SCHEDULER
# -----------------------------------------------------------------------------
class MarketScanner:
    def scan_market(self):
        SYSTEM_STATE["is_scanning"] = True
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.info("=== [MEXC SCANNER] BẮT ĐẦU QUÉT TOÀN BỘ COIN MEXC FUTURES ===")
        crypto_symbols, total_raw, tradfi_blocked = mexc_client.get_verified_crypto_usdt_symbols()
        
        ok, errors, found = 0, 0, 0

        for item in crypto_symbols:
            symbol = item["symbol"]
            try:
                df_h1 = mexc_client.fetch_klines(symbol, "Min60", 120)
                df_h4 = mexc_client.fetch_klines(symbol, "Min240", 60)
                if df_h1 is None or len(df_h1) < 40:
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                df_h4 = TechnicalAnalyzer.add_indicators(df_h4) if df_h4 is not None else None
                ok += 1

                active = db.get_signal_by_symbol(symbol)
                if active and active["state"] not in ('FINISHED', 'INVALIDATED'):
                    self._process_active(active, df_h1)
                    continue

                is_setup, details, score = LiquiditySweepDetector.evaluate_setup(df_h1, df_h4)
                if is_setup:
                    self._create_signal(symbol, details)
                    found += 1

            except Exception as e:
                logger.error(f"Lỗi xử lý symbol {symbol}: {e}")
                errors += 1
            time.sleep(0.02)

        self._retry_pending_telegrams()

        SYSTEM_STATE["scanned_ok"] = ok
        SYSTEM_STATE["api_errors"] = errors
        SYSTEM_STATE["signals_found"] += found
        SYSTEM_STATE["is_scanning"] = False
        db.log_scan(total_raw, len(crypto_symbols), tradfi_blocked, found)
        logger.info(f"=== [MEXC SCANNER] HOÀN TẤT: Quét {ok}/{len(crypto_symbols)} coin, Phát hiện {found} tín hiệu ===")

    def _create_signal(self, symbol: str, details: Dict):
        details["symbol"] = symbol
        sig = {
            "symbol": symbol, 
            "setup_name": details["setup_name"], 
            "state": "CONFIRMED",
            "signal_price": details["signal_price"], 
            "paper_entry": details["paper_entry"],
            "sl_price": details["sl_price"], 
            "sl_reason": details["sl_reason"],
            "tp1": details["tp1"], 
            "tp1_note": details["tp1_note"], 
            "tp2": details["tp2"], 
            "tp2_note": details["tp2_note"],
            "tp1_rr": details["tp1_rr"], 
            "tp2_rr": details["tp2_rr"], 
            "wave_score": details["wave_score"], 
            "candle_timestamp": details["candle_timestamp"],
            "telegram_sent": 0
        }

        row_id = db.save_signal(sig)
        success = telegram_service.notify_signal(sig)
        if success:
            db.update_telegram_sent_status(symbol, sig["setup_name"], sig["candle_timestamp"], 1)

    def _retry_pending_telegrams(self):
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM signals WHERE telegram_sent = 0 AND state = 'CONFIRMED'")
            pending = [dict(row) for row in cursor.fetchall()]
        
        for p in pending:
            success = telegram_service.notify_signal(p)
            if success:
                db.update_telegram_sent_status(p["symbol"], p["setup_name"], p["candle_timestamp"], 1)
                logger.info(f"Đã gửi lại thông báo Telegram cho #{p['symbol']}")

    def _process_active(self, sig: Dict, df_h1):
        symbol, entry, sl = sig["symbol"], sig["paper_entry"], sig["sl_price"]
        close, low = df_h1['close'].iloc[-2], df_h1['low'].iloc[-2]

        if low <= sl:
            db.update_signal_state(symbol, "FINISHED")
            return

        gain = ((close - entry) / entry) * 100
        max_gain = max(sig.get("max_gain_pct", 0), gain)
        db.update_signal_state(symbol, sig["state"], max_gain=max_gain)

scanner = MarketScanner()

async def full_auto_scheduler():
    logger.info("Đã kích hoạt Scheduler Quét Tự Động MEXC 24/7 (Đồng bộ mỗi nến H1)...")
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
# 9. TELEGRAM COMMAND HANDLERS
# -----------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚀 <b>MEXC H1 SWEEP PRO – HIGH WINRATE BOT</b>\nBot hoạt động 24/7 quét tín hiệu Liquidity Sweep trên MEXC Futures.\n/status - Thống kê tài sản\n/active - Lệnh đang chạy", parse_mode="HTML")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = 'Đang chạy' if SYSTEM_STATE['is_scanning'] else 'Đang chờ (Idle)'
    msg = f"""📊 <b>THỐNG KÊ HỆ THỐNG MEXC PRO</b>
🤖 <b>Trạng thái quét:</b> {status_text}
⏱ <b>Lần quét gần nhất:</b> {SYSTEM_STATE['last_scan_time']}
⏳ <b>Lần quét kế tiếp:</b> {SYSTEM_STATE['next_scan_time']}
🌐 <b>Tổng hợp đồng gốc MEXC:</b> {SYSTEM_STATE['total_raw_symbols']}
✅ <b>Crypto hợp lệ:</b> {SYSTEM_STATE['crypto_valid']}
🚫 <b>TradFi bị chặn:</b> {SYSTEM_STATE['tradfi_blocked']}
🎯 <b>Tín hiệu đã phát hiện:</b> {SYSTEM_STATE['signals_found']}"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    signals = db.get_active_signals_list()
    if not signals:
        await update.message.reply_text("Không có Setup nào đang theo dõi.")
        return
    msg = "🟢 <b>SETUP SWEEP ĐANG THEO DÕI:</b>\n\n"
    for s in signals:
        msg += f"• <b>#{s['symbol']}</b> | {s['setup_name']}\n  Entry: {s['paper_entry']} | Max Gain: +{s['max_gain_pct']:.2f}%\n"
    await update.message.reply_text(msg, parse_mode="HTML")

# -----------------------------------------------------------------------------
# 10. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG MEXC SWEEP PRO 24/7 BOT ")
    logger.info("==================================================")

    threading.Thread(target=run_flask_server, daemon=True).start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN!")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("active", cmd_active))

    async def main_async():
        asyncio.create_task(full_auto_scheduler())
        asyncio.create_task(asyncio.to_thread(scanner.scan_market))

        await app.initialize()
        await app.start()
        logger.info("Bot MEXC Pro đã sẵn sàng 24/7!")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Dừng bot...")

if __name__ == "__main__":
    main()
