#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
MEXC MULTI-ENGINE LONG SIGNAL BOT & BACKTEST SYSTEM (PRO 8 SETUPS - FUTURES ONLY)
SETUP 1 – H4 BREAKOUT MỞ SÓNG (Thuần PA H4/H1)
SETUP 2 – H1 SIDEWAY BREAKOUT (Thuần PA Breakout/Retest Box)
SETUP 3 – H1 PULLBACK RECOVERY LONG (Thuần PA H1)
SETUP 4 – H1 EARLY MOMENTUM RECLAIM (Thuần PA H1)
SETUP 5 – H4 EMA21 BREAKOUT RECLAIM (Thuần PA H4 - BẮT BỘC H4 > EMA200)
SETUP 6 – H1 3-CANDLE EMA21 RECLAIM (Thuần PA H1)
SETUP 7 – H1 BEATUSDT PULLBACK RE-ACCUMULATION (Thuần PA - BẮT BỘC H1 & H4 > EMA200)
SETUP 8 – H1 MOMENTUM EXPANSION (Chỉ bắt nến xanh thứ 2 - Thuần PA, Không Check Vol)
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
# 1. CONFIGURATION & PARAMETERS
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MexcMultiEnginePro")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "mexc_multi_signals.db")

MEXC_SPOT_BASE_URL = "https://api.mexc.com"
MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"

# Hard Filters cho MEXC Futures
MIN_FUTURES_TURNOVER_24H = float(os.environ.get("MIN_FUTURES_TURNOVER_24H", "2000000")) # 2M USDT
COOLDOWN_HOURS = 0  # Bỏ Cooldown để báo nến liên tiếp nếu đạt chuẩn

# Danh sách Stablecoin, ETF và Cổ phiếu cần chặn
STABLE_AND_ETF_BLACKLIST = {
    "USDT", "USDC", "BUSD", "TUSD", "DAI", "EUR", "GBP", "FDUSD", "USDE",
    "3L", "3S", "4L", "4S", "5L", "5S", "DOWN", "UP", "BULL", "BEAR", "STOCK"
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
        "system": "MEXC MULTI-ENGINE SIGNAL PLATFORM (PRO - 8 SETUPS FUTURES)",
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
                signal_status TEXT DEFAULT 'CONFIRMED',
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
        if COOLDOWN_HOURS <= 0:
            return False
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
                symbol, setup_type, signal_status, price, entry_zone, sl_price, tp1, tp2,
                tp1_rr, tp2_rr, score, spot_vol_ratio, oi_change_pct,
                taker_buy_pct, cvd_val, candle_timestamp, telegram_sent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["symbol"], data["setup_type"], data.get("signal_status", "CONFIRMED"),
                data["price"], data["entry_zone"], data["sl_price"], data["tp1"], data["tp2"],
                data["tp1_rr"], data["tp2_rr"], data["score"], data.get("spot_vol_ratio", 0.0),
                data.get("oi_change_pct", 0.0), data.get("taker_buy_pct", 0.0), data.get("cvd_val", 0.0),
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
# 4. MEXC API CLIENT (FUTURES OPTIMIZED & STOCK FILTERED)
# -----------------------------------------------------------------------------
class MexcClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MexcMultiEngineBot/1.0"})

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
        futures_details = self._request(MEXC_FUTURES_BASE_URL, "/api/v1/contract/detail")
        futures_tickers = self._request(MEXC_FUTURES_BASE_URL, "/api/v1/contract/ticker")

        if not futures_details or not futures_tickers:
            logger.error("Không thể tải đầy đủ thông tin danh mục Futures từ MEXC API!")
            return []

        fut_ticker_map = {}
        fut_ticker_data = futures_tickers.get("data", []) if isinstance(futures_tickers, dict) else futures_tickers
        if isinstance(fut_ticker_data, list):
            for ft in fut_ticker_data:
                sym = ft.get("symbol", "").replace("_", "").upper()
                fut_ticker_map[sym] = ft

        qualified = []
        fut_detail_data = futures_details.get("data", []) if isinstance(futures_details, dict) else futures_details
        
        if isinstance(fut_detail_data, list):
            for fd in fut_detail_data:
                if fd.get("state") != 0:
                    continue

                raw_symbol = fd.get("symbol", "")
                sym_clean = raw_symbol.replace("_", "").upper()

                if not sym_clean.endswith("USDT"):
                    continue

                base_coin = sym_clean[:-4]

                # BỘ LỌC CHẶN CỔ PHIẾU (STOCK), ETF, STABLECOIN
                if (base_coin in STABLE_AND_ETF_BLACKLIST or 
                    any(base_coin.endswith(suffix) for suffix in ["3L", "3S", "4L", "4S", "5L", "5S"]) or
                    "STOCK" in base_coin or 
                    "INDEX" in base_coin):
                    continue

                ft_info = fut_ticker_map.get(sym_clean, {})
                futures_turnover_24h = float(ft_info.get("amount24", 0.0))
                last_price = float(ft_info.get("lastPrice", 0.0))
                hold_vol = float(ft_info.get("holdVol", 0.0))

                if futures_turnover_24h < MIN_FUTURES_TURNOVER_24H:
                    continue

                qualified.append({
                    "spot_symbol": sym_clean,
                    "futures_symbol": raw_symbol,
                    "spot_volume_24h": 0.0,
                    "futures_turnover_24h": futures_turnover_24h,
                    "spread_pct": 0.0,
                    "last_price": last_price,
                    "hold_vol": hold_vol
                })

        return qualified

    def fetch_spot_klines(self, symbol: str, interval: str = "1h", limit: int = 100) -> Optional[pd.DataFrame]:
        interval_map = {"1h": "Min60", "4h": "Hour4", "1d": "Day1", "15m": "Min15"}
        fut_interval = interval_map.get(interval, "Min60")
        fut_symbol = symbol if "_" in symbol else f"{symbol[:-4]}_USDT"
        
        res = self._request(
            MEXC_FUTURES_BASE_URL, 
            f"/api/v1/contract/kline/{fut_symbol}", 
            params={"interval": fut_interval}
        )
        
        if not res or not isinstance(res, dict) or not res.get("success"):
            return None
            
        data = res.get("data", {})
        if not data or "time" not in data:
            return None
            
        times = data.get("time", [])
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        vols = data.get("vol", [])
        
        if not times or len(times) == 0:
            return None
            
        rows = []
        for i in range(len(times)):
            rows.append({
                "open_time": int(times[i]) * 1000,
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

mexc_api = MexcClient()

# -----------------------------------------------------------------------------
# 5. TECHNICAL INDICATORS
# -----------------------------------------------------------------------------
class TechnicalAnalyzer:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) < 25:
            return df
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean() if len(df) >= 200 else df['close'].ewm(span=50, adjust=False).mean()

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
                highs.append({'price': df['high'].iloc[i], 'time': df['open_time'].iloc[i], 'idx': i})
            if all(df['low'].iloc[i] <= df['low'].iloc[i - k] for k in range(1, left + 1)) and all(df['low'].iloc[i] < df['low'].iloc[i + k] for k in range(1, right + 1)):
                lows.append({'price': df['low'].iloc[i], 'time': df['open_time'].iloc[i], 'idx': i})
        return highs, lows

# -----------------------------------------------------------------------------
# 6. SETUP ENGINES (8 SETUPS - THUẦN PRICE ACTION, BỎ CHECK VOLUME)
# -----------------------------------------------------------------------------
class SetupEngine:
    @staticmethod
    def evaluate_setup_momentum_expansion(
        df_h1: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP 8 – CHỈ BÁO CÂY NẾN XANH BÙNG NỔ THỨ 2 (KHÔNG CHECK VOLUME)"""
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        c1 = df_h1.iloc[-2]  # Cây nến vừa đóng cửa (Nến thứ 2)
        c2 = df_h1.iloc[-3]  # Cây nến trước đó (Nến thứ 1)
        entry_price = c1['close']
        atr_h1 = c1['atr'] if not np.isnan(c1['atr']) else (entry_price * 0.01)

        # 1. BẮT BỘC NẾN THỨ 1 (C2) PHẢI LÀ NẾN XANH
        is_c2_green = c2['close'] > c2['open']
        if not is_c2_green:
            return False, {}, 0, "Bỏ qua nến xanh thứ 1 (Nến liền trước không phải nến xanh)"

        # 2. NẾN THỨ 2 (C1) PHẢI LÀ NẾN XANH BÙNG NỔ (Thân nến lớn hơn 1.0x ATR)
        is_c1_green = c1['close'] > c1['open']
        candle_body = abs(c1['close'] - c1['open'])
        if not is_c1_green or candle_body < (1.0 * atr_h1):
            return False, {}, 0, "Nến thứ 2 chưa đủ độ bùng nổ"

        # 3. Nới lỏng kiểm tra trượt giá real-time
        if current_realtime_price > entry_price + (1.5 * atr_h1):
            return False, {}, 0, "Giá real-time quá xa Entry"

        # 4. Giá đóng cửa nằm trên EMA21
        if c1['close'] < c1['ema21']:
            return False, {}, 0, "Giá chưa nằm trên EMA21"

        sl_price = round(min(c1['low'], c2['low']) - (0.15 * atr_h1), 4)
        risk = entry_price - sl_price
        if risk <= 0: return False, {}, 0, "SL lỗi"

        tp1 = round(entry_price + (1.5 * risk), 4)
        tp2 = round(entry_price + (2.5 * risk), 4)

        score = 95
        details = {
            "setup_type": "H1_MOMENTUM_EXPANSION",
            "signal_status": "CONFIRMED_2ND_CANDLE",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": sl_price,
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.5,
            "tp2_rr": 2.5,
            "score": score,
            "candle_timestamp": int(c1['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN NẾN XANH BÙNG NỔ THỨ 2"

    @staticmethod
    def evaluate_setup_1_h4_breakout(
        df_h4: pd.DataFrame, df_h1: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        if df_h4 is None or df_h1 is None or len(df_h4) < 25 or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu nến H4/H1 không đủ"

        h4_last = df_h4.iloc[-2]
        h4_prev = df_h4.iloc[-3]
        h1_last = df_h1.iloc[-2]
        entry_price = h1_last['close']

        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.2 * atr_h1):
            return False, {}, 0, f"Giá real-time ({current_realtime_price}) chạy xa Entry H1 ({entry_price})"

        ema21_h4 = h4_last['ema21']
        ema200_h4 = h4_last.get('ema200', 0)
        if not (h4_last['close'] > ema21_h4 and h4_last['close'] > ema200_h4 and ema21_h4 > h4_prev['ema21']):
            return False, {}, 0, "H4 chưa đóng trên EMA21/EMA200"

        h4_high_20 = df_h4['high'].iloc[-22:-2].max()
        if not (h4_last['close'] > h4_high_20):
            return False, {}, 0, "H4 chưa phá đỉnh 20 nến"

        if h1_last['close'] <= h1_last['ema21']:
            return False, {}, 0, "H1 chưa đóng trên EMA21"

        h1_highs, h1_lows = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        recent_h1_high = h1_highs[-1]['price'] if h1_highs else df_h1['high'].iloc[-10:-2].max()
        
        is_break_h1_high = h1_last['close'] > recent_h1_high
        signal_status = "CONFIRMED" if is_break_h1_high else "EARLY"

        h1_swing_low = h1_lows[-1]['price'] if h1_lows else df_h1['low'].iloc[-10:-1].min()
        sl_price = round(h1_swing_low - (0.20 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn Entry"

        risk = entry_price - sl_price
        h4_highs, _ = TechnicalAnalyzer.find_pivots(df_h4.iloc[:-1], left=2, right=2)
        valid_res = sorted([h['price'] for h in h4_highs if h['price'] > entry_price])

        tp1 = valid_res[0] if len(valid_res) >= 1 else entry_price + (2.0 * risk)
        tp2 = valid_res[1] if len(valid_res) >= 2 else entry_price + (3.2 * risk)

        rr1 = round((tp1 - entry_price) / risk, 2)
        rr2 = round((tp2 - entry_price) / risk, 2)

        if rr1 < 1.5:
            return False, {}, 0, f"RR1 ({rr1}) chưa đạt"

        score = 85
        if signal_status == "CONFIRMED": score += 10

        details = {
            "setup_type": "H4 BREAKOUT MỞ SÓNG",
            "signal_status": signal_status,
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": round(tp1, 4),
            "tp2": round(tp2, 4),
            "tp1_rr": rr1,
            "tp2_rr": rr2,
            "score": score,
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 1"

    @staticmethod
    def evaluate_setup_4_sideway_breakout(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        if df_h1 is None or len(df_h1) < 30:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        h1_last = df_h1.iloc[-2]
        entry_price = h1_last['close']

        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.2 * atr_h1):
            return False, {}, 0, "Giá real-time quá xa Entry H1"

        ema21_h1 = h1_last['ema21']
        ema200_h1 = h1_last.get('ema200', 0)

        if h1_last['close'] < ema21_h1 or h1_last['close'] < ema200_h1:
            return False, {}, 0, "H1 đóng dưới EMA21/EMA200"

        box_window = df_h1.iloc[-18:-2]
        box_high = box_window['high'].max()
        box_low = box_window['low'].min()

        recent_3_highs = df_h1['high'].iloc[-5:-2]
        had_breakout_before = (recent_3_highs > box_high).any()
        is_retesting = (df_h1['low'].iloc[-2] <= box_high * 1.002) and (h1_last['close'] > box_high)

        if had_breakout_before and is_retesting and h1_last['close'] > h1_last['open']:
            signal_status = "RETEST"
        elif h1_last['close'] > box_high:
            signal_status = "CONFIRMED"
        else:
            return False, {}, 0, "Chưa nổ breakout đỉnh Box"

        sl_price = round(box_low - (0.20 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL không hợp lệ"

        risk = entry_price - sl_price
        box_height = box_high - box_low
        tp1_box_target = entry_price + max(box_height * 1.2, risk * 1.5)

        h4_highs, _ = TechnicalAnalyzer.find_pivots(df_h4.iloc[:-1] if df_h4 is not None else df_h1.iloc[:-1], left=2, right=2)
        valid_res = sorted([h['price'] for h in h4_highs if h['price'] > entry_price])

        tp1 = round(tp1_box_target, 4)
        tp2 = round(valid_res[0] if len(valid_res) >= 1 else entry_price + (box_height * 2.2), 4)

        rr1 = round((tp1 - entry_price) / risk, 2)
        rr2 = round((tp2 - entry_price) / risk, 2)

        score = 80
        if signal_status == "CONFIRMED": score += 10

        details = {
            "setup_type": "H1_SIDEWAY_BREAKOUT",
            "signal_status": signal_status,
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": rr1,
            "tp2_rr": rr2,
            "score": score,
            "box_high": round(box_high, 4),
            "box_low": round(box_low, 4),
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 2"

    @staticmethod
    def evaluate_setup_5_pullback_recovery(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        if df_h1 is None or len(df_h1) < 35:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        h1_last = df_h1.iloc[-2]
        entry_price = h1_last['close']

        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.2 * atr_h1):
            return False, {}, 0, "Giá real-time quá xa Entry H1"

        ema200_h1 = h1_last.get('ema200', 0)
        if h1_last['close'] < ema200_h1:
            return False, {}, 0, "H1 nằm dưới EMA200"

        base_range = df_h1.iloc[-14:-2]
        box_high = base_range['high'].max()
        box_low = base_range['low'].min()

        if h1_last['close'] <= box_high or h1_last['close'] <= h1_last['ema21']:
            return False, {}, 0, "H1 chưa đóng vượt đỉnh tích lũy ngắn"

        sl_price = round(box_low - (0.20 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL không hợp lệ"

        risk = entry_price - sl_price
        tp1 = round(entry_price + (1.5 * risk), 4)
        tp2 = round(entry_price + (2.8 * risk), 4)

        score = 85
        details = {
            "setup_type": "H1_PULLBACK_RECOVERY",
            "signal_status": "CONFIRMED",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.5,
            "tp2_rr": 2.8,
            "score": score,
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 3"

    @staticmethod
    def evaluate_setup_6_early_momentum(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        candle_2 = df_h1.iloc[-2]
        candle_1 = df_h1.iloc[-3]
        entry_price = candle_2['close']

        atr_h1 = candle_2['atr'] if not np.isnan(candle_2['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.5 * atr_h1):
            return False, {}, 0, f"Giá real-time ({current_realtime_price}) đã chạy xa Entry nến 2 ({entry_price})"

        ema200_h1 = candle_2.get('ema200', 0)
        if entry_price < ema200_h1:
            return False, {}, 0, "H1 nằm dưới EMA200"

        ema21_h1 = candle_2['ema21']
        is_candle_2_green = candle_2['close'] > candle_2['open']
        is_candle_2_reclaim_ema21 = candle_2['close'] > ema21_h1

        if not (is_candle_2_green and is_candle_2_reclaim_ema21):
            return False, {}, 0, "Nến 2 chưa đóng xanh nằm trên EMA21"

        swing_low = min(candle_1['low'], candle_2['low'])
        sl_price = round(swing_low - (0.15 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn hoặc bằng Entry"

        risk = entry_price - sl_price
        tp1 = round(entry_price + (1.5 * risk), 4)
        tp2 = round(entry_price + (2.5 * risk), 4)

        score = 80
        details = {
            "setup_type": "H1_EARLY_MOMENTUM_PA",
            "signal_status": "EARLY_ENTRY",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.5,
            "tp2_rr": 2.5,
            "score": score,
            "candle_timestamp": int(candle_2['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 4 (EARLY THUẦN NẾN H1)"

    @staticmethod
    def evaluate_setup_7_h4_ema21_reclaim_pa(
        df_h4: pd.DataFrame, df_h1: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP 5 - H4 EMA21 RECLAIM (SIẾT CHẶT ĐIỀU KIỆN: H4 PHẢI TRÊN EMA200)"""
        if df_h4 is None or len(df_h4) < 25:
            return False, {}, 0, "Dữ liệu nến H4 không đủ"

        h4_candle_last = df_h4.iloc[-2]
        h4_candle_prev = df_h4.iloc[-3]
        entry_price = h4_candle_last['close']

        atr_h4 = h4_candle_last['atr'] if not np.isnan(h4_candle_last['atr']) else (entry_price * 0.015)
        if current_realtime_price > entry_price + (1.2 * atr_h4):
            return False, {}, 0, "Giá real-time quá xa Entry H4"

        # BẮT BỘC 100%: GIÁ ĐÓNG CỬA H4 PHẢI TRÊN EMA200 H4 (CHECK FOLLOW UPTREND H4)
        ema200_h4 = h4_candle_last.get('ema200', 0)
        if entry_price < ema200_h4:
            return False, {}, 0, "H4 nằm dưới EMA200 (Không thỏa mãn Follow Trend H4)"

        ema21_h4 = h4_candle_last['ema21']
        is_h4_green = h4_candle_last['close'] > h4_candle_last['open']
        is_h4_reclaim_ema21 = h4_candle_last['close'] > ema21_h4

        if not (is_h4_green and is_h4_reclaim_ema21):
            return False, {}, 0, "Nến H4 chưa đóng xanh dứt khoát vượt EMA21 H4"

        swing_low_h4 = min(h4_candle_prev['low'], h4_candle_last['low'])
        sl_price = round(swing_low_h4 - (0.15 * atr_h4), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn hoặc bằng Entry"

        risk = entry_price - sl_price
        tp1 = round(entry_price + (1.8 * risk), 4)
        tp2 = round(entry_price + (3.0 * risk), 4)

        score = 85
        details = {
            "setup_type": "H4_EMA21_RECLAIM_PA",
            "signal_status": "CONFIRMED",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.8,
            "tp2_rr": 3.0,
            "score": score,
            "candle_timestamp": int(h4_candle_last['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 5 (H4 EMA21 RECLAIM PA)"

    @staticmethod
    def evaluate_setup_8_h1_3candle_reclaim_pa(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        c1 = df_h1.iloc[-2]
        c2 = df_h1.iloc[-3]
        c3 = df_h1.iloc[-4]
        entry_price = c1['close']

        atr_h1 = c1['atr'] if not np.isnan(c1['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.2 * atr_h1):
            return False, {}, 0, "Giá real-time quá xa Entry"

        ema200_h1 = c1.get('ema200', 0)
        if entry_price < ema200_h1 * 0.998:
            return False, {}, 0, "H1 nằm dưới EMA200"

        ema21_h1 = c1['ema21']
        is_c1_green = c1['close'] > c1['open']
        is_c1_above_ema21 = c1['close'] > ema21_h1
        is_dip_below_ema21 = (c3['low'] < c3['ema21']) or (c2['low'] < c2['ema21'])

        if not (is_c1_green and is_c1_above_ema21 and is_dip_below_ema21):
            return False, {}, 0, "Chưa đúng cấu trúc 3 nến H1 nhúng và vọt vượt EMA21"

        swing_low = min(c1['low'], c2['low'], c3['low'])
        sl_price = round(swing_low - (0.15 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn hoặc bằng Entry"

        risk = entry_price - sl_price
        tp1 = round(entry_price + (1.8 * risk), 4)
        tp2 = round(entry_price + (2.8 * risk), 4)

        score = 88
        details = {
            "setup_type": "H1_3CANDLE_EMA21_RECLAIM_PA",
            "signal_status": "CONFIRMED",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.8,
            "tp2_rr": 2.8,
            "score": score,
            "candle_timestamp": int(c1['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 6 (H1 3-CANDLE RECLAIM PA)"

    @staticmethod
    def evaluate_setup_9_beatusdt_pullback_reaccumulation(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_pct: float, taker_buy_pct: Optional[float], cvd_val: Optional[float]
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP 7 - H1 BEATUSDT PULLBACK (BẮT BỘC CẢ H1 VÀ H4 PHẢI TRÊN EMA200)"""
        if df_h1 is None or len(df_h1) < 35:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        candle_trigger = df_h1.iloc[-2]
        entry_price = candle_trigger['close']

        atr_h1 = candle_trigger['atr'] if not np.isnan(candle_trigger['atr']) else (entry_price * 0.01)
        if current_realtime_price > entry_price + (1.2 * atr_h1):
            return False, {}, 0, "Giá real-time quá xa Entry"

        # 1. BẮT BỘC H1 NẰM TRÊN EMA200 H1
        ema200_h1 = candle_trigger.get('ema200', 0)
        if entry_price < ema200_h1:
            return False, {}, 0, "H1 nằm dưới EMA200 (Không thỏa mãn Follow Trend H1)"

        # 2. BẮT BỘC H4 NẰM TRÊN EMA200 H4 (NẾU CÓ DỮ LIỆU H4)
        if df_h4 is not None and len(df_h4) >= 20:
            h4_last = df_h4.iloc[-2]
            ema200_h4 = h4_last.get('ema200', 0)
            if h4_last['close'] < ema200_h4:
                return False, {}, 0, "H4 nằm dưới EMA200 (Không thỏa mãn Follow Trend H4)"

        pullback_low = df_h1['low'].iloc[-10:-2].min()
        ema21_h1 = candle_trigger['ema21']
        is_green = candle_trigger['close'] > candle_trigger['open']
        is_reclaim_ema21 = candle_trigger['close'] > ema21_h1

        if not (is_green and is_reclaim_ema21):
            return False, {}, 0, "Nến Trigger chưa đóng xanh vượt EMA21 H1"

        sl_price = round(pullback_low - (0.15 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn hoặc bằng Entry"

        risk = entry_price - sl_price
        tp1 = round(entry_price + (1.8 * risk), 4)
        tp2 = round(entry_price + (3.0 * risk), 4)

        score = 88
        details = {
            "setup_type": "H1_BEATUSDT_PULLBACK_REACCUMULATION",
            "signal_status": "CONFIRMED",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": round(sl_price, 4),
            "tp1": tp1,
            "tp2": tp2,
            "tp1_rr": 1.8,
            "tp2_rr": 3.0,
            "score": score,
            "candle_timestamp": int(candle_trigger['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP BEATUSDT PULLBACK"

# -----------------------------------------------------------------------------
# 7. TELEGRAM SERVICE (TỐI GIẢN - 1 GIÁ ENTRY, BỎ LÝ DO & DÒNG TIỀN)
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
        
        status_tag = f"[{data.get('signal_status', 'CONFIRMED')}]"
        setup_name = data["setup_type"]
        
        if setup_name == "H1_MOMENTUM_EXPANSION":
            header = f"🚀 <b>[SETUP 8 – MOMENTUM EXPANSION {status_tag}]</b>"
        elif setup_name == "H1_BEATUSDT_PULLBACK_REACCUMULATION":
            header = f"🔥 <b>[SETUP 7 – BEATUSDT PULLBACK {status_tag}]</b>"
        elif setup_name == "H1_3CANDLE_EMA21_RECLAIM_PA":
            header = f"⚡ <b>[SETUP 6 – H1 3-CANDLE EMA21 RECLAIM {status_tag}]</b>"
        elif setup_name == "H4_EMA21_RECLAIM_PA":
            header = f"👑 <b>[SETUP 5 – H4 EMA21 BREAKOUT RECLAIM {status_tag}]</b>"
        elif setup_name == "H1_EARLY_MOMENTUM_PA":
            header = f"⚡ <b>[SETUP 4 – H1 EARLY MOMENTUM {status_tag}]</b>"
        elif setup_name == "H1_PULLBACK_RECOVERY":
            header = f"🔥 <b>[SETUP 3 – H1 PULLBACK RECOVERY {status_tag}]</b>"
        elif setup_name == "H1_SIDEWAY_BREAKOUT":
            header = f"💥 <b>[SETUP 2 – H1 SIDEWAY BREAKOUT {status_tag}]</b>"
        else:
            header = f"🎯 <b>[SETUP 1 – H4 BREAKOUT MỞ SÓNG {status_tag}]</b>"
        
        time_str = datetime.now(timezone.utc).strftime('%H:%M:%S %d/%m/%Y UTC')

        box_info_str = ""
        if "box_high" in data and "box_low" in data:
            box_info_str = f"📦 <b>Box High / Low:</b> {data['box_high']} / {data['box_low']}\n"

        msg = f"""{header} <b>#{symbol}</b>

⭐ <b>Chất lượng Setup:</b> {data['score']}/100
⏱ <b>Thời gian:</b> {time_str}
{box_info_str}
📍 <b>KẾ HOẠCH BẮT LỆNH KỸ THUẬT:</b>
- <b>Entry Giá Tham Chiếu:</b> {data['price']}
- <b>Stop Loss:</b> {data['sl_price']}
- <b>TP1:</b> {data['tp1']} (R:R <b>1:{data['tp1_rr']}</b>)
- <b>TP2:</b> {data['tp2']} (R:R <b>1:{data['tp2_rr']}</b>)

🔗 <a href="{chart_url}">Mở Chart MEXC #{symbol}</a>"""
        return self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 8. MARKET SCANNER & LOGGING ENGINE
# -----------------------------------------------------------------------------
class MarketScanner:
    def scan_market(self, is_manual_scan: bool = False) -> List[Dict[str, Any]]:
        SYSTEM_STATE["is_scanning"] = True
        current_ts = int(time.time())
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.info("=== [MEXC ENGINE PRO] BẮT ĐẦU VÒNG QUÉT THỊ TRƯỜNG ===")
        
        qualified_list = mexc_api.get_qualified_universe()
        SYSTEM_STATE["qualified_symbols"] = len(qualified_list)
        
        ok_count, error_count, signals_count = 0, 0, 0
        scan_reports = []

        for idx, item in enumerate(qualified_list, 1):
            spot_sym = item["spot_symbol"]
            fut_sym = item["futures_symbol"]
            hold_vol = item["hold_vol"]
            realtime_price = item["last_price"]

            logger.info(f"🔍 [{idx}/{len(qualified_list)}] Đang kiểm tra #{spot_sym} (Giá: {realtime_price})...")

            try:
                db.save_oi_snapshot(fut_sym, current_ts, hold_vol)
                oi_1h_ago = db.get_oi_1h_ago(fut_sym, current_ts)
                oi_change_pct = (((hold_vol - oi_1h_ago) / oi_1h_ago) * 100.0) if oi_1h_ago and oi_1h_ago > 0 else 0.0

                df_h1 = mexc_api.fetch_spot_klines(spot_sym, "1h", 60)
                df_h4 = mexc_api.fetch_spot_klines(spot_sym, "4h", 60)

                if df_h1 is None or len(df_h1) < 25:
                    logger.info(f"   ❌ [LOẠI #{spot_sym}]: Lỗi lấy dữ liệu nến H1")
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                if df_h4 is not None and len(df_h4) >= 25:
                    df_h4 = TechnicalAnalyzer.add_indicators(df_h4)

                ok_count += 1
                candle_ts = int(df_h1['open_time'].iloc[-2])

                candidate_signals = []
                rejection_reasons = []

                # SETUP 8: H1 MOMENTUM EXPANSION (CHỈ BÁO NẾN XANH THỨ 2)
                if not db.is_cooldown_active(spot_sym, "H1_MOMENTUM_EXPANSION", candle_ts):
                    is_s8, d8, score8, reason8 = SetupEngine.evaluate_setup_momentum_expansion(
                        df_h1, realtime_price, oi_change_pct, None, None
                    )
                    if is_s8: candidate_signals.append((score8, d8, "H1_MOMENTUM_EXPANSION"))
                    else: rejection_reasons.append(f"S8: {reason8}")

                # SETUP 7: H1 BEATUSDT PULLBACK RE-ACCUMULATION
                if not db.is_cooldown_active(spot_sym, "H1_BEATUSDT_PULLBACK_REACCUMULATION", candle_ts):
                    is_s7, d7, score7, reason7 = SetupEngine.evaluate_setup_9_beatusdt_pullback_reaccumulation(
                        df_h1, df_h4, realtime_price, oi_change_pct, None, None
                    )
                    if is_s7: candidate_signals.append((score7, d7, "H1_BEATUSDT_PULLBACK_REACCUMULATION"))
                    else: rejection_reasons.append(f"S7: {reason7}")

                # SETUP 6: H1 3-CANDLE EMA21 RECLAIM
                if not db.is_cooldown_active(spot_sym, "H1_3CANDLE_EMA21_RECLAIM_PA", candle_ts):
                    is_s6, d6, score6, reason6 = SetupEngine.evaluate_setup_8_h1_3candle_reclaim_pa(
                        df_h1, df_h4, realtime_price, oi_change_pct, None, None
                    )
                    if is_s6: candidate_signals.append((score6, d6, "H1_3CANDLE_EMA21_RECLAIM_PA"))
                    else: rejection_reasons.append(f"S6: {reason6}")

                # SETUP 5: H4 EMA21 BREAKOUT RECLAIM
                if not db.is_cooldown_active(spot_sym, "H4_EMA21_RECLAIM_PA", candle_ts):
                    is_s5, d5, score5, reason5 = SetupEngine.evaluate_setup_7_h4_ema21_reclaim_pa(
                        df_h4, df_h1, realtime_price, oi_change_pct, None, None
                    )
                    if is_s5: candidate_signals.append((score5, d5, "H4_EMA21_RECLAIM_PA"))
                    else: rejection_reasons.append(f"S5: {reason5}")

                # SETUP 4: H1 EARLY MOMENTUM RECLAIM
                if not db.is_cooldown_active(spot_sym, "H1_EARLY_MOMENTUM_PA", candle_ts):
                    is_s4, d4, score4, reason4 = SetupEngine.evaluate_setup_6_early_momentum(
                        df_h1, df_h4, realtime_price, oi_change_pct, None, None
                    )
                    if is_s4: candidate_signals.append((score4, d4, "H1_EARLY_MOMENTUM_PA"))
                    else: rejection_reasons.append(f"S4: {reason4}")

                # SETUP 1: H4 BREAKOUT MỞ SÓNG
                if not db.is_cooldown_active(spot_sym, "H4 BREAKOUT MỞ SÓNG", candle_ts):
                    is_s1, d1, score1, reason1 = SetupEngine.evaluate_setup_1_h4_breakout(
                        df_h4, df_h1, realtime_price, oi_change_pct, None, None
                    )
                    if is_s1: candidate_signals.append((score1, d1, "H4 BREAKOUT MỞ SÓNG"))
                    else: rejection_reasons.append(f"S1: {reason1}")

                # SETUP 2: H1 SIDEWAY BREAKOUT
                if not db.is_cooldown_active(spot_sym, "H1_SIDEWAY_BREAKOUT", candle_ts):
                    is_s2, d2, score2, reason2 = SetupEngine.evaluate_setup_4_sideway_breakout(
                        df_h1, df_h4, realtime_price, oi_change_pct, None, None
                    )
                    if is_s2: candidate_signals.append((score2, d2, "H1_SIDEWAY_BREAKOUT"))
                    else: rejection_reasons.append(f"S2: {reason2}")

                # SETUP 3: H1 PULLBACK RECOVERY
                if not db.is_cooldown_active(spot_sym, "H1_PULLBACK_RECOVERY", candle_ts):
                    is_s3, d3, score3, reason3 = SetupEngine.evaluate_setup_5_pullback_recovery(
                        df_h1, df_h4, realtime_price, oi_change_pct, None, None
                    )
                    if is_s3: candidate_signals.append((score3, d3, "H1_PULLBACK_RECOVERY"))
                    else: rejection_reasons.append(f"S3: {reason3}")

                if candidate_signals:
                    candidate_signals.sort(key=lambda x: x[0], reverse=True)
                    best_score, best_data, best_setup_name = candidate_signals[0]

                    best_data["symbol"] = spot_sym
                    row_id = db.record_signal(best_data)
                    if telegram_service.notify_signal(best_data):
                        db.update_telegram_status(row_id, 1)
                    signals_count += 1
                    logger.info(f"   🎯 [BẮT TÍN HIỆU #{spot_sym}] Thỏa mãn {best_setup_name} (Score: {best_score})")
                    scan_reports.append({"symbol": spot_sym, "status": "SIGNAL", "score": best_score, "details": best_setup_name})
                else:
                    top_reason = rejection_reasons[0] if rejection_reasons else "Chưa tích lũy đủ"
                    logger.info(f"   ❌ [LOẠI #{spot_sym}]: {top_reason}")
                    scan_reports.append({"symbol": spot_sym, "status": "REJECTED", "reason": top_reason})

            except Exception as e:
                logger.error(f"Lỗi xử lý symbol {spot_sym}: {e}")
                error_count += 1
            
            time.sleep(0.1)

        db.cleanup_old_oi_snapshots()

        SYSTEM_STATE["scanned_ok"] = ok_count
        SYSTEM_STATE["api_errors"] = error_count
        SYSTEM_STATE["signals_found"] += signals_count
        SYSTEM_STATE["is_scanning"] = False

        db.log_scan(len(qualified_list), signals_count, error_count)
        logger.info(f"=== [MEXC ENGINE] HOÀN TẤT VÒNG QUÉT: Đạt chuẩn={len(qualified_list)}, Tìm thấy={signals_count} ===")
        return scan_reports

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
        "🚀 <b>MEXC MULTI-ENGINE LONG SIGNAL BOT (PRO 8 SETUPS FUTURES)</b>\n"
        "Bot quét tự động 24/7 phát hiện 8 Setup Long thuần Price Action.\n"
        "/status - Thống kê trạng thái hệ thống\n"
        "/scan - Kích hoạt quét & In bảng tổng hợp chi tiết", 
        parse_mode="HTML"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = 'Đang chạy' if SYSTEM_STATE['is_scanning'] else 'Đang chờ (Idle)'
    msg = f"""📊 <b>THỐNG KÊ HỆ THỐNG MEXC ENGINE PRO</b>
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
    
    await update.message.reply_text("🔄 <b>Kích hoạt quét toàn bộ danh mục Futures MEXC...</b>\n<i>Đang phân tích từng đồng coin, vui lòng đợi trong giây lát...</i>", parse_mode="HTML")
    
    async with SCAN_LOCK:
        reports = await asyncio.to_thread(scanner.scan_market, True)
    
    signals_list = [r for r in reports if r["status"] == "SIGNAL"]
    rejected_list = [r for r in reports if r["status"] == "REJECTED"]

    report_msg = f"📊 <b>KẾT QUẢ PHÂN TÍCH TỔNG HỢP ({len(reports)} COIN FUTURES)</b>\n\n"
    
    if signals_list:
        report_msg += "🎯 <b>TÍN HIỆU KÍCH HOẠT VÀO LỆNH:</b>\n"
        for s in signals_list:
            report_msg += f"• <b>#{s['symbol']}</b>: {s['details']} (Score: {s['score']})\n"
        report_msg += "\n"
    else:
        report_msg += "🎯 <b>Tín hiệu kích hoạt:</b> 0 (Chưa có coin thỏa mãn 100% Setup)\n\n"

    report_msg += "📋 <b>MẪU COIN TIỀM NĂNG (ĐANG CHỜ TÍCH LŨY):</b>\n"
    for r in rejected_list[:7]:
        report_msg += f"• <b>#{r['symbol']}</b>: <i>{r['reason']}</i>\n"

    report_msg += f"\n✅ <i>Đã quét xong {len(reports)} coin đạt chuẩn. Log tiến trình chi tiết đã được cập nhật trên Server.</i>"
    
    await update.message.reply_text(report_msg, parse_mode="HTML")

# -----------------------------------------------------------------------------
# 10. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG MEXC MULTI ENGINE 24/7 BOT ")
    logger.info("==================================================")

    threading.Thread(target=run_flask_server, daemon=True).start()

    if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR_"):
        logger.error("CHƯA CẤU HÌNH BOT_TOKEN!")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("scan", cmd_scan))

    async def post_init(application):
        asyncio.create_task(full_auto_scheduler())
        asyncio.create_task(asyncio.to_thread(scanner.scan_market))

    app.post_init = post_init
    app.run_polling()

if __name__ == "__main__":
    main()
