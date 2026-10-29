#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
MEXC DUAL-ENGINE LONG SIGNAL BOT (SPOT + FUTURES ORDERFLOW)
Setup 1: LONG READY ENTRY (Pullback / Reclaim Support)
Setup 2: LONG MONEY FLOW EXPLOSION (Accumulation Breakout)
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
# 1. CONFIGURATION & CONFIGURABLE PARAMETERS
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MexcDualEngineBot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "mexc_dual_signals.db")

MEXC_SPOT_BASE_URL = "https://api.mexc.com"
MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"

# Adjustable Hard Filters Thresholds for MEXC
MIN_SPOT_VOL_24H = float(os.environ.get("MIN_SPOT_VOL_24H", "1000000"))       # 1M USDT (phù hợp MEXC Spot)
MIN_FUTURES_TURNOVER_24H = float(os.environ.get("MIN_FUTURES_TURNOVER_24H", "5000000")) # 5M USDT
MAX_SPOT_SPREAD_PCT = float(os.environ.get("MAX_SPOT_SPREAD_PCT", "0.35"))    # 0.35%
COOLDOWN_HOURS = 4

# Blacklist
STABLE_AND_ETF_BLACKLIST = {
    "USDT", "USDC", "BUSD", "TUSD", "DAI", "EUR", "GBP", "FDUSD", "USDE",
    "3L", "3S", "4L", "4S", "5L", "5S", "DOWN", "UP", "BULL", "BEAR"
}

SYSTEM_STATE = {
    "is_scanning": False,
    "last_scan_time": "Chưa quét",
    "next_scan_time": "Đang tính toán...",
    "total_spot_symbols": 0,
    "qualified_symbols": 0,
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
        "system": "MEXC SPOT+FUTURES DUAL SIGNAL ENGINE",
        "is_scanning": SYSTEM_STATE["is_scanning"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

def run_flask_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------------------------------------------------------
# 3. SQLITE DATABASE MANAGER
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
                setup_type TEXT NOT NULL,
                signal_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                price REAL NOT NULL,
                entry_zone TEXT NOT NULL,
                sl_price REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp1_rr REAL NOT NULL,
                tp2_rr REAL NOT NULL,
                score INTEGER NOT NULL,
                spot_vol_ratio REAL,
                oi_change_pct REAL,
                taker_buy_pct REAL,
                cvd_val REAL,
                candle_timestamp INTEGER NOT NULL,
                telegram_sent INTEGER DEFAULT 0
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS oi_snapshots (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                hold_vol REAL NOT NULL,
                PRIMARY KEY (symbol, timestamp)
            )
            """)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                qualified_count INTEGER,
                signals_found INTEGER,
                errors_count INTEGER
            )
            """)
            conn.commit()

    def save_oi_snapshot(self, symbol: str, timestamp: int, hold_vol: float):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT OR REPLACE INTO oi_snapshots (symbol, timestamp, hold_vol)
            VALUES (?, ?, ?)
            """, (symbol, timestamp, hold_vol))
            conn.commit()

    def get_oi_1h_ago(self, symbol: str, current_ts: int) -> Optional[float]:
        ts_1h_ago = current_ts - 3600
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT hold_vol FROM oi_snapshots 
            WHERE symbol = ? AND timestamp BETWEEN ? AND ?
            ORDER BY ABS(timestamp - ?) ASC LIMIT 1
            """, (symbol, ts_1h_ago - 900, ts_1h_ago + 900, ts_1h_ago))
            row = cursor.fetchone()
            return row["hold_vol"] if row else None

    def cleanup_old_oi_snapshots(self):
        cutoff = int(time.time()) - 86400
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM oi_snapshots WHERE timestamp < ?", (cutoff,))
            conn.commit()

    def is_cooldown_active(self, symbol: str, setup_type: str, candle_ts: int) -> bool:
        cutoff = int(time.time()) - (COOLDOWN_HOURS * 3600)
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT 1 FROM signals 
            WHERE symbol = ? AND setup_type = ? 
            AND (candle_timestamp = ? OR strftime('%s', signal_time) > ?)
            """, (symbol, setup_type, candle_ts, cutoff))
            return cursor.fetchone() is not None

    def record_signal(self, data: Dict[str, Any]) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO signals (
                symbol, setup_type, price, entry_zone, sl_price, tp1, tp2,
                tp1_rr, tp2_rr, score, spot_vol_ratio, oi_change_pct,
                taker_buy_pct, cvd_val, candle_timestamp, telegram_sent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["symbol"], data["setup_type"], data["price"], data["entry_zone"],
                data["sl_price"], data["tp1"], data["tp2"], data["tp1_rr"], data["tp2_rr"],
                data["score"], data.get("spot_vol_ratio", 0.0), data.get("oi_change_pct", 0.0),
                data.get("taker_buy_pct", 0.0), data.get("cvd_val", 0.0),
                data["candle_timestamp"], data.get("telegram_sent", 0)
            ))
            conn.commit()
            return cursor.lastrowid

    def update_telegram_status(self, row_id: int, status: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE signals SET telegram_sent = ? WHERE id = ?", (status, row_id))
            conn.commit()

    def log_scan(self, qualified: int, signals: int, errors: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO scan_logs (qualified_count, signals_found, errors_count)
            VALUES (?, ?, ?)
            """, (qualified, signals, errors))
            conn.commit()

db = DatabaseManager()

# -----------------------------------------------------------------------------
# 4. MEXC API CLIENT
# -----------------------------------------------------------------------------
class MexcClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MexcDualEngineBot/1.0"})

    def _request(self, base_url: str, endpoint: str, params: Dict[str, Any] = None, retries: int = 3) -> Optional[Any]:
        url = f"{base_url}{endpoint}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) or isinstance(data, dict):
                        return data
                elif resp.status_code == 429:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Lỗi API Mexc [{url}]: {e}")
                time.sleep(1 * (attempt + 1))
        return None

    def get_qualified_universe(self) -> List[Dict[str, Any]]:
        spot_tickers = self._request(MEXC_SPOT_BASE_URL, "/api/v3/ticker/24hr")
        book_tickers = self._request(MEXC_SPOT_BASE_URL, "/api/v3/ticker/bookTicker")
        futures_details = self._request(MEXC_FUTURES_BASE_URL, "/api/v1/contract/detail")
        futures_tickers = self._request(MEXC_FUTURES_BASE_URL, "/api/v1/contract/ticker")

        if not spot_tickers or not book_tickers or not futures_details or not futures_tickers:
            logger.error("Không thể tải đầy đủ thông tin danh mục từ MEXC API!")
            return []

        futures_map = {}
        fut_ticker_data = futures_tickers.get("data", []) if isinstance(futures_tickers, dict) else futures_tickers
        if isinstance(fut_ticker_data, list):
            for ft in fut_ticker_data:
                sym = ft.get("symbol", "").replace("_", "").upper()
                futures_map[sym] = ft

        fut_detail_set = set()
        fut_detail_data = futures_details.get("data", []) if isinstance(futures_details, dict) else futures_details
        if isinstance(fut_detail_data, list):
            for fd in fut_detail_data:
                if fd.get("state") == 0:
                    sym = fd.get("symbol", "").replace("_", "").upper()
                    fut_detail_set.add(sym)

        book_map = {b["symbol"]: b for b in book_tickers if isinstance(b, dict) and "symbol" in b}

        qualified = []
        for st in spot_tickers:
            if not isinstance(st, dict): continue
            symbol_spot = st.get("symbol", "").upper()
            if not symbol_spot.endswith("USDT"):
                continue

            base_coin = symbol_spot[:-4]
            if base_coin in STABLE_AND_ETF_BLACKLIST or any(base_coin.endswith(suffix) for suffix in ["3L", "3S", "4L", "4S", "5L", "5S"]):
                continue

            spot_vol_quote = float(st.get("quoteVolume", 0.0))
            if spot_vol_quote < MIN_SPOT_VOL_24H:
                continue

            if symbol_spot not in fut_detail_set or symbol_spot not in futures_map:
                continue

            ft_info = futures_map[symbol_spot]
            futures_turnover_24h = float(ft_info.get("amount24", 0.0))
            if futures_turnover_24h < MIN_FUTURES_TURNOVER_24H:
                continue

            book = book_map.get(symbol_spot)
            if not book:
                continue
            bid = float(book.get("bidPrice", 0.0))
            ask = float(book.get("askPrice", 0.0))
            if bid <= 0:
                continue
            spread_pct = ((ask - bid) / bid) * 100.0
            if spread_pct > MAX_SPOT_SPREAD_PCT:
                continue

            hold_vol = float(ft_info.get("holdVol", 0.0))

            qualified.append({
                "spot_symbol": symbol_spot,
                "futures_symbol": ft_info.get("symbol", f"{base_coin}_USDT"),
                "spot_volume_24h": spot_vol_quote,
                "futures_turnover_24h": futures_turnover_24h,
                "spread_pct": spread_pct,
                "last_price": float(st.get("lastPrice", 0.0)),
                "hold_vol": hold_vol
            })

        return qualified

    def fetch_spot_klines(self, symbol: str, interval: str = "1h", limit: int = 100) -> Optional[pd.DataFrame]:
        res = self._request(MEXC_SPOT_BASE_URL, "/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
        if not res or not isinstance(res, list) or len(res) == 0:
            return None
        rows = []
        for i in res:
            rows.append({
                "open_time": int(i[0]),
                "open": float(i[1]),
                "high": float(i[2]),
                "low": float(i[3]),
                "close": float(i[4]),
                "volume": float(i[5])
            })
        df = pd.DataFrame(rows)
        df.sort_values(by="open_time", ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def fetch_spot_orderflow_and_cvd(self, symbol: str, limit: int = 1000) -> Tuple[float, float, bool]:
        res = self._request(MEXC_SPOT_BASE_URL, "/api/v3/aggTrades", params={"symbol": symbol, "limit": limit})
        if not res or not isinstance(res, list) or len(res) < 30:
            return 50.0, 0.0, False

        taker_buy_vol = 0.0
        taker_sell_vol = 0.0

        for t in res:
            qty = float(t.get("q", 0.0))
            price = float(t.get("p", 0.0))
            vol_usdt = qty * price
            is_buyer_maker = t.get("m", False)

            if is_buyer_maker:
                taker_buy_vol += vol_usdt
            else:
                taker_sell_vol += vol_usdt

        total_vol = taker_buy_vol + taker_sell_vol
        if total_vol <= 0:
            return 50.0, 0.0, False

        taker_buy_pct = (taker_buy_vol / total_vol) * 100.0
        cvd_val = taker_buy_vol - taker_sell_vol
        return round(taker_buy_pct, 2), round(cvd_val, 2), True

mexc_api = MexcClient()

# -----------------------------------------------------------------------------
# 5. TECHNICAL INDICATORS & STRUCTURE
# -----------------------------------------------------------------------------
class TechnicalAnalyzer:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) < 25:
            return df
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['vol_sma20'] = df['volume'].rolling(window=20).mean()

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

    @staticmethod
    def is_h1_uptrend(df_h1: pd.DataFrame) -> bool:
        if df_h1 is None or len(df_h1) < 25:
            return False
        highs, lows = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        if len(highs) >= 2 and len(lows) >= 2:
            return highs[-1]['price'] >= highs[-2]['price'] and lows[-1]['price'] >= lows[-2]['price']
        h1_last = df_h1.iloc[-2]
        return h1_last['close'] > h1_last['ema20'] > h1_last['ema50']

# -----------------------------------------------------------------------------
# 6. SETUP ENGINES
# -----------------------------------------------------------------------------
class SetupEngine:
    @staticmethod
    def evaluate_setup_1_ready_entry(
        df_h1: pd.DataFrame, df_m15: pd.DataFrame, oi_change_pct: float,
        taker_buy_pct: float, cvd_val: float, is_cvd_valid: bool
    ) -> Tuple[bool, Dict[str, Any], int]:
        if df_h1 is None or df_m15 is None or len(df_h1) < 25 or len(df_m15) < 12:
            return False, {}, 0

        if taker_buy_pct < 52.0 or oi_change_pct < -3.5:
            return False, {}, 0

        if not TechnicalAnalyzer.is_h1_uptrend(df_h1):
            return False, {}, 0

        h1_last = df_h1.iloc[-2]
        current_price = df_h1['close'].iloc[-1]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (current_price * 0.01)

        _, lows = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        swing_low = lows[-1]['price'] if lows else df_h1['low'].iloc[-15:-1].min()
        ema20 = h1_last['ema20']
        ema50 = h1_last['ema50']

        support_ref = max([s for s in [swing_low, ema20, ema50] if s <= current_price], default=swing_low)

        dist_to_support = abs(current_price - support_ref)
        if dist_to_support > (0.35 * atr_h1):
            return False, {}, 0

        m15_last = df_m15.iloc[-2]
        m15_is_bullish = m15_last['close'] > m15_last['open'] or (m15_last['close'] - m15_last['low']) >= 0.5 * (m15_last['high'] - m15_last['low'])
        if not m15_is_bullish:
            return False, {}, 0

        vol_sma_h1 = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1
        vol_ratio_h1 = h1_last['volume'] / vol_sma_h1
        if vol_ratio_h1 < 1.2:
            return False, {}, 0

        sl_price = round(swing_low - (0.20 * atr_h1), 4)
        if sl_price >= current_price:
            return False, {}, 0

        risk = current_price - sl_price

        highs, _ = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        valid_res = sorted([h['price'] for h in highs if h['price'] > current_price])

        tp1 = valid_res[0] if len(valid_res) >= 1 else current_price + (1.5 * risk)
        tp2 = valid_res[1] if len(valid_res) >= 2 else current_price + (2.5 * risk)

        rr1 = round((tp1 - current_price) / risk, 2)
        rr2 = round((tp2 - current_price) / risk, 2)

        if rr1 < 1.3 or rr2 < 1.8:
            return False, {}, 0

        score = 75
        if vol_ratio_h1 >= 1.5: score += 10
        if taker_buy_pct >= 58.0: score += 10
        if oi_change_pct >= 0.5: score += 5
        score = min(score, 100)

        details = {
            "setup_type": "LONG READY ENTRY",
            "price": current_price,
            "entry_zone": f"{round(support_ref, 4)} - {round(current_price, 4)}",
            "sl_price": round(sl_price, 4),
            "tp1": round(tp1, 4),
            "tp2": round(tp2, 4),
            "tp1_rr": rr1,
            "tp2_rr": rr2,
            "score": score,
            "spot_vol_ratio": round(vol_ratio_h1, 2),
            "oi_change_pct": round(oi_change_pct, 2),
            "taker_buy_pct": taker_buy_pct,
            "cvd_val": cvd_val,
            "reason_entry": "Giá hồi quy sát hỗ trợ kỹ thuật H1 với nến M15 xác nhận.",
            "reason_sl": "Dưới đáy Swing Low H1 + Đệm rủi ro 0.20 ATR.",
            "reason_tp": "Theo vùng kháng cự cấu trúc H1/H4 gần nhất.",
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, score

    @staticmethod
    def evaluate_setup_2_money_flow_explosion(
        df_h1: pd.DataFrame, oi_change_pct: float,
        taker_buy_pct: float, cvd_val: float, is_cvd_valid: bool
    ) -> Tuple[bool, Dict[str, Any], int]:
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0

        if taker_buy_pct < 55.0 or oi_change_pct < 3.0:
            return False, {}, 0

        h1_last = df_h1.iloc[-2]
        current_price = df_h1['close'].iloc[-1]
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (current_price * 0.01)

        base_range = df_h1.iloc[-20:-2]
        base_high = base_range['high'].max()
        base_low = base_range['low'].min()

        if h1_last['close'] <= base_high:
            return False, {}, 0

        vol_sma_h1 = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1
        vol_ratio_h1 = h1_last['volume'] / vol_sma_h1
        if vol_ratio_h1 < 1.8:
            return False, {}, 0

        dist_from_breakout = current_price - base_high
        if dist_from_breakout > (0.8 * atr_h1):
            return False, {}, 0

        sl_price = round(base_low - (0.20 * atr_h1), 4)
        if sl_price >= current_price:
            return False, {}, 0

        risk = current_price - sl_price

        highs, _ = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        valid_res = sorted([h['price'] for h in highs if h['price'] > current_price])

        tp1 = valid_res[0] if len(valid_res) >= 1 else current_price + (1.6 * risk)
        tp2 = valid_res[1] if len(valid_res) >= 2 else current_price + (2.6 * risk)

        rr1 = round((tp1 - current_price) / risk, 2)
        rr2 = round((tp2 - current_price) / risk, 2)

        if rr1 < 1.3 or rr2 < 1.8:
            return False, {}, 0

        score = 85
        if vol_ratio_h1 >= 2.5: score += 5
        if taker_buy_pct >= 60.0: score += 5
        if oi_change_pct >= 5.0: score += 5
        score = min(score, 100)

        details = {
            "setup_type": "LONG MONEY FLOW EXPLOSION",
            "price": current_price,
            "entry_zone": f"{round(base_high, 4)} - {round(current_price, 4)}",
            "sl_price": round(sl_price, 4),
            "tp1": round(tp1, 4),
            "tp2": round(tp2, 4),
            "tp1_rr": rr1,
            "tp2_rr": rr2,
            "score": score,
            "spot_vol_ratio": round(vol_ratio_h1, 2),
            "oi_change_pct": round(oi_change_pct, 2),
            "taker_buy_pct": taker_buy_pct,
            "cvd_val": cvd_val,
            "reason_entry": "Breakout dứt khoát dải tích lũy H1 kèm dòng tiền Spot & Futures bùng nổ.",
            "reason_sl": "Dưới đáy nền tích lũy H1 + Đệm rủi ro 0.20 ATR.",
            "reason_tp": "Theo các vùng kháng cự Swing High mở rộng.",
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, score

# -----------------------------------------------------------------------------
# 7. TELEGRAM SERVICE
# -----------------------------------------------------------------------------
class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token, self.chat_id = token, chat_id
        self.api_url = f"https://api.telegram.org/bot{token}/sendMessage"

    def send_message_sync(self, text: str) -> bool:
        if not self.token or self.token.startswith("YOUR_") or not self.chat_id:
            logger.info(f"[TELEGRAM PREVIEW]:\n{text}")
            return True
        try:
            resp = requests.post(self.api_url, json={
                "chat_id": self.chat_id, 
                "text": text, 
                "parse_mode": "HTML", 
                "disable_web_page_preview": True
            }, timeout=8)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Lỗi kết nối Telegram: {e}")
            return False

    def notify_signal(self, data: Dict[str, Any]) -> bool:
        symbol = data["symbol"]
        chart_url = f"https://www.mexc.com/exchange/{symbol}"
        header = "🎯 <b>[LONG READY ENTRY]</b>" if data["setup_type"] == "LONG READY ENTRY" else "🔥 <b>[LONG MONEY FLOW EXPLOSION]</b>"
        
        time_str = datetime.now(timezone.utc).strftime('%H:%M:%S %d/%m/%Y UTC')

        msg = f"""{header} <b>#{symbol}</b>

⭐ <b>Chất lượng Setup:</b> {data['score']}/100
⏱ <b>Thời gian:</b> {time_str}
💰 <b>Giá hiện tại:</b> {data['price']}

📊 <b>XÁC NHẬN DÒNG TIỀN:</b>
• Volume Ratio Spot H1: <b>{data['spot_vol_ratio']}x</b>
• OI Change Futures H1: <b>{data['oi_change_pct']:+.2f}%</b>
• Taker Buy Ratio Spot: <b>{data['taker_buy_pct']:.1f}%</b>
• CVD Spot Flow: <b>+{data['cvd_val']:,.0f} USDT</b>

📍 <b>KẾ HOẠCH BẮT LỆNH:</b>
• <b>Entry Zone:</b> {data['entry_zone']}
• <b>Stop Loss:</b> {data['sl_price']} ({data['reason_sl']})
• <b>TP1:</b> {data['tp1']} (R:R {data['tp1_rr']})
• <b>TP2:</b> {data['tp2']} (R:R {data['tp2_rr']})

💡 <b>Lý do vào lệnh:</b> {data['reason_entry']}
💡 <b>Lý do TP:</b> {data['reason_tp']}

🔗 <a href="{chart_url}">Mở Chart MEXC #{symbol}</a>"""
        return self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 8. MARKET SCANNER
# -----------------------------------------------------------------------------
class MarketScanner:
    def scan_market(self):
        SYSTEM_STATE["is_scanning"] = True
        current_ts = int(time.time())
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.info("=== [MEXC ENGINE] BẮT ĐẦU VÒNG QUÉT THỊ TRƯỜNG ===")
        
        qualified_list = mexc_api.get_qualified_universe()
        SYSTEM_STATE["qualified_symbols"] = len(qualified_list)
        
        ok_count, error_count, signals_count = 0, 0, 0

        for item in qualified_list:
            spot_sym = item["spot_symbol"]
            fut_sym = item["futures_symbol"]
            hold_vol = item["hold_vol"]

            try:
                db.save_oi_snapshot(fut_sym, current_ts, hold_vol)
                oi_1h_ago = db.get_oi_1h_ago(fut_sym, current_ts)
                oi_change_pct = (((hold_vol - oi_1h_ago) / oi_1h_ago) * 100.0) if oi_1h_ago and oi_1h_ago > 0 else 0.0

                df_h1 = mexc_api.fetch_spot_klines(spot_sym, "1h", 60)
                df_m15 = mexc_api.fetch_spot_klines(spot_sym, "15m", 40)

                if df_h1 is None or df_m15 is None or len(df_h1) < 25:
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                df_m15 = TechnicalAnalyzer.add_indicators(df_m15)
                ok_count += 1

                candle_ts = int(df_h1['open_time'].iloc[-2])
                taker_buy_pct, cvd_val, is_cvd_valid = mexc_api.fetch_spot_orderflow_and_cvd(spot_sym)

                if not db.is_cooldown_active(spot_sym, "LONG READY ENTRY", candle_ts):
                    is_s1, d1, score1 = SetupEngine.evaluate_setup_1_ready_entry(
                        df_h1, df_m15, oi_change_pct, taker_buy_pct, cvd_val, is_cvd_valid
                    )
                    if is_s1:
                        d1["symbol"] = spot_sym
                        row_id = db.record_signal(d1)
                        if telegram_service.notify_signal(d1):
                            db.update_telegram_status(row_id, 1)
                        signals_count += 1
                        time.sleep(0.05)
                        continue

                if not db.is_cooldown_active(spot_sym, "LONG MONEY FLOW EXPLOSION", candle_ts):
                    is_s2, d2, score2 = SetupEngine.evaluate_setup_2_money_flow_explosion(
                        df_h1, oi_change_pct, taker_buy_pct, cvd_val, is_cvd_valid
                    )
                    if is_s2:
                        d2["symbol"] = spot_sym
                        row_id = db.record_signal(d2)
                        if telegram_service.notify_signal(d2):
                            db.update_telegram_status(row_id, 1)
                        signals_count += 1
                        time.sleep(0.05)
                        continue

            except Exception as e:
                logger.error(f"Lỗi xử lý symbol {spot_sym}: {e}")
                error_count += 1
            time.sleep(0.02)

        db.cleanup_old_oi_snapshots()

        SYSTEM_STATE["scanned_ok"] = ok_count
        SYSTEM_STATE["api_errors"] = error_count
        SYSTEM_STATE["signals_found"] += signals_count
        SYSTEM_STATE["is_scanning"] = False

        db.log_scan(len(qualified_list), signals_count, error_count)
        logger.info(f"=== [MEXC ENGINE] HOÀN TẤT VÒNG QUÉT: Đạt chuẩn={len(qualified_list)}, Tìm thấy={signals_count} ===")

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
    await update.message.reply_text(
        "🚀 <b>MEXC DUAL-ENGINE LONG SIGNAL BOT</b>\n"
        "Bot quét tự động 24/7 phát hiện 2 Setup Long độc lập dựa trên Spot + Futures Orderflow.\n"
        "/status - Thống kê tài sản\n"
        "/scan - Kích hoạt quét tức thì", 
        parse_mode="HTML"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = 'Đang chạy' if SYSTEM_STATE['is_scanning'] else 'Đang chờ (Idle)'
    msg = f"""📊 <b>THỐNG KÊ HỆ THỐNG MEXC DUAL ENGINE</b>
🤖 <b>Trạng thái quét:</b> {status_text}
⏱ <b>Lần quét gần nhất:</b> {SYSTEM_STATE['last_scan_time']}
⏳ <b>Lần quét kế tiếp:</b> {SYSTEM_STATE['next_scan_time']}
🌐 <b>Số coin đạt lọc cứng:</b> {SYSTEM_STATE['qualified_symbols']}
✅ <b>Số coin quét thành công:</b> {SYSTEM_STATE['scanned_ok']}
🎯 <b>Tín hiệu đã phát hiện:</b> {SYSTEM_STATE['signals_found']}"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Vòng quét đang chạy, vui lòng chờ...")
        return
    await update.message.reply_text("🔄 Kích hoạt quét thủ công toàn bộ MEXC...")
    async with SCAN_LOCK:
        await asyncio.to_thread(scanner.scan_market)
    await update.message.reply_text("✅ Hoàn tất vòng quét thủ công!")

# -----------------------------------------------------------------------------
# 10. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG MEXC DUAL ENGINE 24/7 BOT ")
    logger.info("==================================================")

    threading.Thread(target=run_flask_server, daemon=True).start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN!")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("scan", cmd_scan))

    async def main_async():
        asyncio.create_task(full_auto_scheduler())
        asyncio.create_task(asyncio.to_thread(scanner.scan_market))

        await app.initialize()
        await app.start()
        logger.info("Bot MEXC Dual Engine đã sẵn sàng 24/7!")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Dừng bot...")

if __name__ == "__main__":
    main()
