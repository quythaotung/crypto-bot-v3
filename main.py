#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
MEXC MULTI-ENGINE LONG SIGNAL BOT & BACKTEST SYSTEM (PRO 3 SETUPS)
- SETUP 8: H1 MOMENTUM EXPANSION (PA NẾN XANH THỨ 2)
- SETUP 6: H1 3-CANDLE EMA21 RECLAIM PA
- SETUP MỚI: LONG H1 SQUEEZE EXPLOSION (DÒNG TIỀN + CẤU TRÚC DỘNG)
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
logger = logging.getLogger("MexcSqueezeEngine")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")
PORT = int(os.environ.get("PORT", "10000"))
DB_PATH = os.environ.get("DB_PATH", "mexc_squeeze_signals.db")

MEXC_SPOT_BASE_URL = "https://api.mexc.com"
MEXC_FUTURES_BASE_URL = "https://contract.mexc.com"

# Hard Filters cho MEXC Futures
MIN_FUTURES_TURNOVER_24H = float(os.environ.get("MIN_FUTURES_TURNOVER_24H", "1000000")) # 1M USDT
MAX_SPREAD_PCT = 0.25  # Spread tối đa 0.25%
MIN_SCORE_TELEGRAM = 88  # Chỉ gửi Telegram khi Score >= 88
COOLDOWN_HOURS = 0

# Blacklist Cổ phiếu, ETF, Stablecoin
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
    "signals_found": 0,
    "missed_opportunities": 0  # Số tín hiệu bị loại do Score < 88
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
        "system": "MEXC SQUEEZE & PA SIGNAL PLATFORM (MIN SCORE 88)",
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
                score INTEGER NOT NULL,
                vol_ratio_h1 REAL,
                taker_buy_pct REAL,
                cvd_val REAL,
                oi_change_h1 REAL,
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
                missed_count INTEGER,
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
                score, vol_ratio_h1, taker_buy_pct, cvd_val, oi_change_h1,
                candle_timestamp, telegram_sent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["symbol"], data["setup_type"], data.get("signal_status", "CONFIRMED"),
                data["price"], data.get("entry_zone", str(data["price"])), data["sl_price"],
                data.get("tp1", 0.0), data.get("tp2", 0.0), data["score"],
                data.get("vol_ratio_h1", 1.0), data.get("taker_buy_pct", 50.0),
                data.get("cvd_val", 0.0), data.get("oi_change_h1", 0.0),
                data["candle_timestamp"], data.get("telegram_sent", 0)
            ))
            conn.commit()
            return cursor.lastrowid

    def update_telegram_status(self, row_id: int, status: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE signals SET telegram_sent = ? WHERE id = ?", (status, row_id))
            conn.commit()

    def log_scan(self, qualified: int, signals: int, missed: int, errors: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO scan_logs (qualified_count, signals_found, missed_count, errors_count)
            VALUES (?, ?, ?, ?)
            """, (qualified, signals, missed, errors))
            conn.commit()

db = DatabaseManager()

# -----------------------------------------------------------------------------
# 4. MEXC API CLIENT
# -----------------------------------------------------------------------------
class MexcClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MexcSqueezeBot/2.0"})

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
            logger.error("Không thể tải thông tin Futures từ MEXC API!")
            return []

        fut_ticker_map = {}
        fut_ticker_data = futures_tickers.get("data", []) if isinstance(futures_tickers, dict) else futures_tickers
        if isinstance(fut_ticker_data, list):
            for ft in fut_ticker_data:
                sym = ft.get("symbol", "").replace("_", "").upper()
                fut_ticker_map[sym] = ft

        # Từ khóa lọc bỏ Cổ phiếu / ETF / Index
        STOCK_AND_INDEX_KEYWORDS = {
            "STOCK", "INDEX", "ETF", "KORU", "TESLA", "APPLE", "NVDA", "AMZN",
            "US500", "US30", "US100", "NAS100", "SPX", "DJI", "BULL", "BEAR"
        }

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

                if (base_coin in STABLE_AND_ETF_BLACKLIST or 
                    any(base_coin.endswith(suffix) for suffix in ["3L", "3S", "4L", "4S", "5L", "5S", "2L", "2S"]) or
                    any(kw in base_coin for kw in STOCK_AND_INDEX_KEYWORDS)):
                    continue

                ft_info = fut_ticker_map.get(sym_clean, {})
                futures_turnover_24h = float(ft_info.get("amount24", 0.0))
                last_price = float(ft_info.get("lastPrice", 0.0))
                bid1 = float(ft_info.get("bid1", last_price))
                ask1 = float(ft_info.get("ask1", last_price))
                hold_vol = float(ft_info.get("holdVol", 0.0))

                # Lọc Spread <= 0.25%
                spread_pct = (((ask1 - bid1) / last_price) * 100.0) if last_price > 0 else 0.0
                if spread_pct > MAX_SPREAD_PCT:
                    continue

                # Lọc Vol 24h >= 1M USDT
                if futures_turnover_24h < MIN_FUTURES_TURNOVER_24H:
                    continue

                qualified.append({
                    "spot_symbol": sym_clean,
                    "futures_symbol": raw_symbol,
                    "futures_turnover_24h": futures_turnover_24h,
                    "spread_pct": round(spread_pct, 4),
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

    def fetch_orderflow_metrics(self, symbol: str) -> Tuple[float, float]:
        """Tính toán ước lượng Taker Buy % và CVD dựa trên nến H1"""
        fut_symbol = symbol if "_" in symbol else f"{symbol[:-4]}_USDT"
        df_h1 = self.fetch_spot_klines(symbol, "1h", 15)
        if df_h1 is None or len(df_h1) < 5:
            return 54.0, 10000.0  # Mặc định an toàn
        
        # Ước tính Taker Buy Ratio dựa trên vị thế đóng nến so với High-Low
        total_vol = df_h1['volume'].iloc[-3:-1].sum()
        buy_vol = 0.0
        for idx, row in df_h1.iloc[-3:-1].iterrows():
            rng = row['high'] - row['low']
            factor = (row['close'] - row['low']) / rng if rng > 0 else 0.5
            buy_vol += row['volume'] * factor
            
        taker_buy_pct = (buy_vol / total_vol * 100.0) if total_vol > 0 else 54.0
        cvd_val = (buy_vol - (total_vol - buy_vol))
        return round(taker_buy_pct, 1), round(cvd_val, 2)

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
        df['vol_sma20'] = df['volume'].rolling(window=20).mean()
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
# 6. SETUP ENGINES (3 SETUPS GIỮ LẠI & MỚI)
# -----------------------------------------------------------------------------
class SetupEngine:
    @staticmethod
    def evaluate_setup_squeeze_explosion(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float,
        oi_change_h1: float, taker_buy_pct: float, cvd_val: float,
        turnover_24h: float, spread_pct: float
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP MỚI: LONG H1 SQUEEZE EXPLOSION (CHẤM ĐIỂM ĐỘNG NGHStrict 6 TIÊU CHÍ)"""
        if df_h1 is None or len(df_h1) < 30 or df_h4 is None or len(df_h4) < 20:
            return False, {}, 0, "Dữ liệu H1/H4 không đủ"

        h1_last = df_h1.iloc[-2]
        c1_close = h1_last['close']
        atr_h1 = h1_last['atr'] if not np.isnan(h1_last['atr']) else (c1_close * 0.01)

        # 1. ĐIỀU KIỆN BẮT BỘC (HARD FILTERS)
        if taker_buy_pct < 53.0:
            return False, {}, 0, f"Spot Taker Buy ({taker_buy_pct}%) < 53%"
        if oi_change_h1 < 0.5:
            return False, {}, 0, f"OI H1 ({oi_change_h1}%) < 0.5%"

        # Check 2/3 yếu tố: Volume H1 >= 1.2x, Cấu trúc H1 tích cực, H4 xu hướng tăng
        vol_sma20 = h1_last['vol_sma20'] if h1_last['vol_sma20'] > 0 else 1.0
        vol_ratio_h1 = h1_last['volume'] / vol_sma20
        cond_vol = vol_ratio_h1 >= 1.2

        h1_resistance = df_h1['high'].iloc[-15:-2].max()
        is_breakout = c1_close > h1_resistance
        is_near_res = (c1_close >= h1_resistance * 0.995)
        cond_h1 = is_breakout or is_near_res or (c1_close > h1_last['ema21'])

        h4_last = df_h4.iloc[-2]
        cond_h4 = (h4_last['close'] > h4_last['ema21']) or (h4_last['close'] > h4_last.get('ema200', 0))

        factors_met = sum([cond_vol, cond_h1, cond_h4])
        if factors_met < 2:
            return False, {}, 0, f"Chỉ đạt {factors_met}/3 yếu tố bổ trợ (yêu cầu >= 2)"

        # 2. CHẤM ĐIỂM ĐỘNG (TOÀN BỘ TÍNH TỪ DỮ LIỆU THỰC TẾ)
        # a. Spot Buy & CVD (Tối đa 30 điểm)
        score_buy = min(30.0, max(0.0, (taker_buy_pct - 50.0) * 2.0 + (10.0 if cvd_val > 0 else 0.0)))
        
        # b. OI (Tối đa 20 điểm)
        score_oi = min(20.0, max(0.0, oi_change_h1 * 4.0))

        # c. Cấu trúc giá H1 (Tối đa 20 điểm)
        score_h1 = 10.0
        if is_breakout: score_h1 += 10.0
        elif is_near_res: score_h1 += 5.0
        if c1_close > h1_last['ema21']: score_h1 += 3.0

        # d. Volume H1 (Tối đa 15 điểm)
        score_vol = min(15.0, max(0.0, vol_ratio_h1 * 7.5))

        # e. Xu hướng H4 (Tối đa 10 điểm)
        score_h4 = 0.0
        if h4_last['close'] > h4_last['ema21']: score_h4 += 5.0
        if h4_last['close'] > h4_last.get('ema200', 0): score_h4 += 5.0

        # f. Thanh khoản (Tối đa 5 điểm)
        score_liq = min(5.0, max(0.0, (turnover_24h / 2000000.0) * 2.5 + (2.5 if spread_pct <= 0.1 else 1.0)))

        total_score = int(round(score_buy + score_oi + score_h1 + score_vol + score_h4 + score_liq))
        total_score = min(100, total_score)

        # Trạng thái
        signal_status = "CONFIRMED" if is_breakout else "EARLY"

        # Tính Stop Loss (Dưới đáy Swing Low + ATR) & TP
        h1_highs, h1_lows = TechnicalAnalyzer.find_pivots(df_h1.iloc[:-1], left=2, right=2)
        h1_swing_low = h1_lows[-1]['price'] if h1_lows else df_h1['low'].iloc[-10:-1].min()
        sl_price = round(h1_swing_low - (0.15 * atr_h1), 4)

        if sl_price >= c1_close:
            sl_price = round(c1_close - (1.5 * atr_h1), 4)

        risk = c1_close - sl_price
        
        # TP1 (Kháng cự H1), TP2 (Kháng cự H4)
        tp1 = round(h1_resistance if h1_resistance > c1_close else c1_close + (1.5 * risk), 4)
        h4_highs, _ = TechnicalAnalyzer.find_pivots(df_h4.iloc[:-1], left=2, right=2)
        tp2 = round(h4_highs[-1]['price'] if h4_highs and h4_highs[-1]['price'] > tp1 else c1_close + (2.8 * risk), 4)

        entry_zone = f"{round(h1_resistance * 0.998, 4)} - {round(c1_close, 4)}" if not is_breakout else f"{round(c1_close, 4)}"

        details = {
            "setup_type": "LONG H1 SQUEEZE EXPLOSION",
            "signal_status": signal_status,
            "price": c1_close,
            "entry_zone": entry_zone,
            "sl_price": sl_price,
            "tp1": tp1,
            "tp2": tp2,
            "score": total_score,
            "vol_ratio_h1": round(vol_ratio_h1, 2),
            "taker_buy_pct": taker_buy_pct,
            "cvd_val": cvd_val,
            "oi_change_h1": round(oi_change_h1, 2),
            "candle_timestamp": int(h1_last['open_time'])
        }
        return True, details, total_score, "ĐẠT CHUẨN LONG H1 SQUEEZE EXPLOSION"

    @staticmethod
    def evaluate_setup_momentum_expansion(
        df_h1: pd.DataFrame, current_realtime_price: float
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP 8: H1 MOMENTUM EXPANSION (PA NẾN XANH THỨ 2)"""
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu nến H1 không đủ"

        c1 = df_h1.iloc[-2]
        c2 = df_h1.iloc[-3]
        entry_price = c1['close']
        atr_h1 = c1['atr'] if not np.isnan(c1['atr']) else (entry_price * 0.01)

        if not (c2['close'] > c2['open']):
            return False, {}, 0, "Nến trước không phải nến xanh"

        is_c1_green = c1['close'] > c1['open']
        candle_body = abs(c1['close'] - c1['open'])
        if not is_c1_green or candle_body < (1.0 * atr_h1):
            return False, {}, 0, "Nến thứ 2 chưa đủ bùng nổ"

        if c1['close'] < c1['ema21']:
            return False, {}, 0, "Giá chưa nằm trên EMA21"

        sl_price = round(min(c1['low'], c2['low']) - (0.15 * atr_h1), 4)
        if entry_price <= sl_price: return False, {}, 0, "SL lỗi"

        score = 95
        details = {
            "setup_type": "H1_MOMENTUM_EXPANSION",
            "signal_status": "CONFIRMED_2ND_CANDLE",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": sl_price,
            "tp1": round(entry_price + (1.5 * (entry_price - sl_price)), 4),
            "tp2": round(entry_price + (2.5 * (entry_price - sl_price)), 4),
            "score": score,
            "vol_ratio_h1": round(c1['volume'] / (c1['vol_sma20'] if c1['vol_sma20'] > 0 else 1), 2),
            "candle_timestamp": int(c1['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 8"

    @staticmethod
    def evaluate_setup_3candle_reclaim(
        df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_realtime_price: float
    ) -> Tuple[bool, Dict[str, Any], int, str]:
        """SETUP 6: H1 3-CANDLE EMA21 RECLAIM PA"""
        if df_h1 is None or len(df_h1) < 25:
            return False, {}, 0, "Dữ liệu H1 không đủ"

        c1 = df_h1.iloc[-2]
        c2 = df_h1.iloc[-3]
        c3 = df_h1.iloc[-4]
        entry_price = c1['close']
        atr_h1 = c1['atr'] if not np.isnan(c1['atr']) else (entry_price * 0.01)

        ema200_h1 = c1.get('ema200', 0)
        if entry_price < ema200_h1 * 0.998:
            return False, {}, 0, "H1 nằm dưới EMA200"

        is_c1_green = c1['close'] > c1['open']
        is_c1_above_ema21 = c1['close'] > c1['ema21']
        is_dip_below_ema21 = (c3['low'] < c3['ema21']) or (c2['low'] < c2['ema21'])

        if not (is_c1_green and is_c1_above_ema21 and is_dip_below_ema21):
            return False, {}, 0, "Chưa đúng cấu trúc 3 nến H1 nhúng và vọt vượt EMA21"

        swing_low = min(c1['low'], c2['low'], c3['low'])
        sl_price = round(swing_low - (0.15 * atr_h1), 4)
        if sl_price >= entry_price:
            return False, {}, 0, "SL cao hơn Entry"

        score = 88
        details = {
            "setup_type": "H1_3CANDLE_EMA21_RECLAIM_PA",
            "signal_status": "CONFIRMED",
            "price": entry_price,
            "entry_zone": str(entry_price),
            "sl_price": sl_price,
            "tp1": round(entry_price + (1.8 * (entry_price - sl_price)), 4),
            "tp2": round(entry_price + (2.8 * (entry_price - sl_price)), 4),
            "score": score,
            "vol_ratio_h1": round(c1['volume'] / (c1['vol_sma20'] if c1['vol_sma20'] > 0 else 1), 2),
            "candle_timestamp": int(c1['open_time'])
        }
        return True, details, score, "ĐẠT CHUẨN SETUP 6"

# -----------------------------------------------------------------------------
# 7. TELEGRAM SERVICE (GỬI ĐÚNG ĐIỀU KIỆN SCORE >= 88)
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
            }, timeout=10)
            return resp.status_code == 200
        except Exception as e:
            logger.error(f"Lỗi gửi Telegram: {e}")
            return False

    def notify_batch_signals(self, signals_list: List[Dict[str, Any]]) -> bool:
        """GOM CÁC TÍN HIỆU SCORE >= 88 VÀO 1 TIN NHẮN TỔNG HỢP CHI TIẾT"""
        if not signals_list:
            return True

        time_str = datetime.now(timezone.utc).strftime('%H:%M:%S %d/%m/%Y UTC')
        
        msg = f"🚀 <b>[MEXC LONG SIGNAL ENGINE - PUSH SCORE ≥ 88]</b>\n"
        msg += f"⏱ <b>Thời gian:</b> {time_str}\n"
        msg += f"🔥 <b>Phát hiện:</b> {len(signals_list)} coin đạt chuẩn cực cao\n"
        msg += "-----------------------------------------\n\n"

        for idx, data in enumerate(signals_list, 1):
            symbol = data["symbol"]
            chart_url = f"https://www.mexc.com/exchange/{symbol}"
            status_tag = f"[{data.get('signal_status', 'CONFIRMED')}]"
            setup_name = data["setup_type"]
            
            msg += f"{idx}. 📌 <b>#{symbol}</b> | <b>{setup_name}</b> {status_tag}\n"
            msg += f"   ⭐ <b>Score:</b> {data['score']}/100\n"
            msg += f"   📊 <b>Vol Spot H1 / SMA20:</b> {data.get('vol_ratio_h1', 1.0)}x\n"
            msg += f"   🛒 <b>Spot Taker Buy:</b> {data.get('taker_buy_pct', 50.0)}% | <b>CVD:</b> {data.get('cvd_val', 0.0)}\n"
            msg += f"   📈 <b>OI H1 Change:</b> +{data.get('oi_change_h1', 0.0)}%\n"
            msg += f"   📍 <b>Vùng Entry / Breakout:</b> {data.get('entry_zone', data['price'])}\n"
            msg += f"   🛡 <b>Mốc Vô Hiệu (SL):</b> {data['sl_price']}\n"
            msg += f"   🎯 <b>Mục tiêu Kháng cự:</b> TP1={data.get('tp1', 0.0)} (H1) | TP2={data.get('tp2', 0.0)} (H4)\n"
            msg += f"   👉 <a href='{chart_url}'>Mở Chart #{symbol}</a>\n\n"

        return self.send_message_sync(msg)

telegram_service = TelegramService()

# -----------------------------------------------------------------------------
# 8. MARKET SCANNER ENGINE
# -----------------------------------------------------------------------------
class MarketScanner:
    def scan_market(self, is_manual_scan: bool = False) -> List[Dict[str, Any]]:
        SYSTEM_STATE["is_scanning"] = True
        current_ts = int(time.time())
        SYSTEM_STATE["last_scan_time"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.info("=== [MEXC ENGINE PRO] BẮT ĐẦU VÒNG QUÉT THỊ TRƯỜNG ===")
        
        qualified_list = mexc_api.get_qualified_universe()
        SYSTEM_STATE["qualified_symbols"] = len(qualified_list)
        
        ok_count, error_count, missed_count = 0, 0, 0
        batch_signals = []
        scan_reports = []

        for idx, item in enumerate(qualified_list, 1):
            spot_sym = item["spot_symbol"]
            fut_sym = item["futures_symbol"]
            hold_vol = item["hold_vol"]
            realtime_price = item["last_price"]
            turnover_24h = item["futures_turnover_24h"]
            spread_pct = item["spread_pct"]

            logger.info(f"🔍 [{idx}/{len(qualified_list)}] Đang kiểm tra #{spot_sym} (Giá: {realtime_price})...")

            try:
                db.save_oi_snapshot(fut_sym, current_ts, hold_vol)
                oi_1h_ago = db.get_oi_1h_ago(fut_sym, current_ts)
                oi_change_h1 = (((hold_vol - oi_1h_ago) / oi_1h_ago) * 100.0) if oi_1h_ago and oi_1h_ago > 0 else 0.0

                df_h1 = mexc_api.fetch_spot_klines(spot_sym, "1h", 60)
                df_h4 = mexc_api.fetch_spot_klines(spot_sym, "4h", 60)

                if df_h1 is None or len(df_h1) < 25:
                    continue

                df_h1 = TechnicalAnalyzer.add_indicators(df_h1)
                if df_h4 is not None and len(df_h4) >= 25:
                    df_h4 = TechnicalAnalyzer.add_indicators(df_h4)

                ok_count += 1
                candle_ts = int(df_h1['open_time'].iloc[-2])
                taker_buy_pct, cvd_val = mexc_api.fetch_orderflow_metrics(spot_sym)

                candidate_signals = []
                rejection_reasons = []

                # 1. SETUP MỚI: LONG H1 SQUEEZE EXPLOSION
                if not db.is_cooldown_active(spot_sym, "LONG H1 SQUEEZE EXPLOSION", candle_ts):
                    is_sq, d_sq, score_sq, reason_sq = SetupEngine.evaluate_setup_squeeze_explosion(
                        df_h1, df_h4, realtime_price, oi_change_h1, taker_buy_pct, cvd_val, turnover_24h, spread_pct
                    )
                    if is_sq: candidate_signals.append((score_sq, d_sq, "LONG H1 SQUEEZE EXPLOSION"))
                    else: rejection_reasons.append(f"Squeeze: {reason_sq}")

                # 2. SETUP 8: H1 MOMENTUM EXPANSION
                if not db.is_cooldown_active(spot_sym, "H1_MOMENTUM_EXPANSION", candle_ts):
                    is_s8, d8, score8, reason8 = SetupEngine.evaluate_setup_momentum_expansion(df_h1, realtime_price)
                    if is_s8: candidate_signals.append((score8, d8, "H1_MOMENTUM_EXPANSION"))
                    else: rejection_reasons.append(f"S8: {reason8}")

                # 3. SETUP 6: H1 3-CANDLE EMA21 RECLAIM PA
                if not db.is_cooldown_active(spot_sym, "H1_3CANDLE_EMA21_RECLAIM_PA", candle_ts):
                    is_s6, d6, score6, reason6 = SetupEngine.evaluate_setup_3candle_reclaim(df_h1, df_h4, realtime_price)
                    if is_s6: candidate_signals.append((score6, d6, "H1_3CANDLE_EMA21_RECLAIM_PA"))
                    else: rejection_reasons.append(f"S6: {reason6}")

                if candidate_signals:
                    candidate_signals.sort(key=lambda x: x[0], reverse=True)
                    best_score, best_data, best_setup_name = candidate_signals[0]
                    best_data["symbol"] = spot_sym

                    # LƯU VÀO DATABASE CHO MỌI SCORE ĐỂ BACKTEST
                    row_id = db.record_signal(best_data)
                    best_data["row_id"] = row_id

                    # BỘ LỌC ĐIỂM CỨNG: CHỈ GỬI TELEGRAM KHI SCORE >= 88
                    if best_score >= MIN_SCORE_TELEGRAM:
                        batch_signals.append(best_data)
                        logger.info(f"   🎯 [KÍCH HOẠT PUSH #{spot_sym}] Thỏa mãn {best_setup_name} (Score: {best_score} >= 88)")
                        scan_reports.append({"symbol": spot_sym, "status": "SIGNAL", "score": best_score, "details": best_setup_name})
                    else:
                        missed_count += 1
                        logger.info(f"   ⚠️ [BỎ QUA PUSH #{spot_sym}] {best_setup_name} Score = {best_score} < 88")
                        scan_reports.append({"symbol": spot_sym, "status": "REJECTED_LOW_SCORE", "reason": f"Score {best_score} < 88"})
                else:
                    top_reason = rejection_reasons[0] if rejection_reasons else "Chưa tích lũy đủ"
                    logger.info(f"   ❌ [LOẠI #{spot_sym}]: {top_reason}")
                    scan_reports.append({"symbol": spot_sym, "status": "REJECTED", "reason": top_reason})

            except Exception as e:
                logger.error(f"Lỗi xử lý symbol {spot_sym}: {e}")
                error_count += 1
            
            time.sleep(0.1)

        # GỬI TIN NHẮN TỔNG HỢP TELEGRAM BATCH
        if batch_signals:
            if telegram_service.notify_batch_signals(batch_signals):
                for sig in batch_signals:
                    db.update_telegram_status(sig["row_id"], 1)

        db.cleanup_old_oi_snapshots()

        SYSTEM_STATE["scanned_ok"] = ok_count
        SYSTEM_STATE["api_errors"] = error_count
        SYSTEM_STATE["signals_found"] += len(batch_signals)
        SYSTEM_STATE["missed_opportunities"] += missed_count
        SYSTEM_STATE["is_scanning"] = False

        db.log_scan(len(qualified_list), len(batch_signals), missed_count, error_count)
        logger.info(f"=== [MEXC ENGINE] HOÀN TẤT VÒNG QUÉT: Đạt chuẩn={len(qualified_list)}, Push Telegram={len(batch_signals)}, Bỏ lỡ (<88)={missed_count} ===")
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
        "🚀 <b>MEXC LONG SIGNAL ENGINE (CHỈ PUSH SCORE ≥ 88)</b>\n"
        "Tự động quét MEXC & phát hiện các Setup Long bùng nổ dòng tiền.\n"
        "/status - Thống kê trạng thái hệ thống\n"
        "/scan - Kích hoạt quét ngay lập tức", 
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
🎯 <b>Tín hiệu PUSH Telegram (Score ≥ 88):</b> {SYSTEM_STATE['signals_found']}
⚠️ <b>Cơ hội bỏ lỡ (Score < 88):</b> {SYSTEM_STATE['missed_opportunities']}"""
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if SCAN_LOCK.locked():
        await update.message.reply_text("⏳ Vòng quét đang chạy, vui lòng chờ...")
        return
    
    await update.message.reply_text("🔄 <b>Kích hoạt quét toàn bộ danh mục Futures MEXC...</b>\n<i>Đang phân tích từng đồng coin, vui lòng đợi trong giây lát...</i>", parse_mode="HTML")
    
    async with SCAN_LOCK:
        reports = await asyncio.to_thread(scanner.scan_market, True)
    
    signals_list = [r for r in reports if r["status"] == "SIGNAL"]
    missed_list = [r for r in reports if r["status"] == "REJECTED_LOW_SCORE"]

    report_msg = f"📊 <b>KẾT QUẢ PHÂN TÍCH TỔNG HỢP ({len(reports)} COIN FUTURES)</b>\n\n"
    
    if signals_list:
        report_msg += "🎯 <b>TÍN HIỆU ĐẠT CHUẨN SCORE ≥ 88 (ĐÃ PUSH):</b>\n"
        for s in signals_list:
            report_msg += f"• <b>#{s['symbol']}</b>: {s['details']} (Score: {s['score']})\n"
        report_msg += "\n"
    else:
        report_msg += "🎯 <b>Tín hiệu PUSH:</b> 0 (Không có coin nào đạt Score ≥ 88)\n\n"

    if missed_list:
        report_msg += "📋 <b>MẪU COIN BỎ LỠ (SCORE 80 - 87 LƯU DATABASE):</b>\n"
        for m in missed_list[:5]:
            report_msg += f"• <b>#{m['symbol']}</b>: <i>{m['reason']}</i>\n"

    report_msg += f"\n✅ <i>Hoàn tất kiểm tra {len(reports)} coin.</i>"
    await update.message.reply_text(report_msg, parse_mode="HTML")

# -----------------------------------------------------------------------------
# 10. MAIN ENTRY POINT
# -----------------------------------------------------------------------------
def main():
    logger.info("==================================================")
    logger.info(" KHỞI ĐỘNG HỆ THỐNG MEXC SQUEEZE ENGINE BOT 24/7 ")
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
