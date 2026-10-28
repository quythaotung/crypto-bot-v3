import os
import time
import requests
import json
import numpy as np
import pandas as pd
import sqlite3
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", 10000))

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "5000000"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.35"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "200"))
CATEGORY = os.environ.get("CATEGORY", "linear")

H1_ACCUMULATION_CANDLES = 12
H4_PIVOT_LEFT = 3
H4_PIVOT_RIGHT = 3
TRAILING_ATR_H1_MULT = 2.5
TRAILING_ATR_H4_MULT = 0.8

DB_PATH = os.environ.get("DB_PATH", "smart_wave_pro.db")
BYBIT_BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 2. KEEP-ALIVE SERVER (FLASK)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit H1 Smart Wave Pro Engine Active 24/7!"

def run_flask():
    app.run(host='0.0.0.0', port=PORT)

# ==============================================================================
# 3. DATABASE MANAGER
# ==============================================================================
class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.init_db()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sent_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    setup_name TEXT NOT NULL,
                    candle_timestamp INTEGER NOT NULL,
                    telegram_msg_id INTEGER,
                    created_at INTEGER NOT NULL,
                    UNIQUE(symbol, setup_name, candle_timestamp)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS active_setups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setup_id TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_timestamp INTEGER NOT NULL,
                    reference_price REAL NOT NULL,
                    breakout_price REAL NOT NULL,
                    atr_h1 REAL NOT NULL,
                    atr_h4 REAL NOT NULL,
                    initial_score INTEGER NOT NULL,
                    h4_structure TEXT NOT NULL,
                    invalidation_price REAL NOT NULL,
                    highest_high REAL NOT NULL,
                    lowest_low REAL NOT NULL,
                    trailing_stop REAL NOT NULL,
                    status TEXT DEFAULT 'ACTIVE',
                    last_score INTEGER NOT NULL,
                    tp_status TEXT DEFAULT 'MONITOR',
                    updated_at INTEGER NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS setup_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setup_id TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_timestamp INTEGER NOT NULL,
                    reference_price REAL NOT NULL,
                    entry_price_next REAL NOT NULL,
                    max_high REAL NOT NULL,
                    min_low REAL NOT NULL,
                    max_gain_pct REAL NOT NULL,
                    max_drawdown_pct REAL NOT NULL,
                    wave_duration_hours REAL NOT NULL,
                    exit_reason TEXT NOT NULL,
                    final_score INTEGER NOT NULL,
                    closed_at INTEGER NOT NULL
                )
            """)
            conn.commit()

    def is_signal_sent(self, symbol: str, setup_name: str, candle_ts: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM sent_signals WHERE symbol = ? AND setup_name = ? AND candle_timestamp = ?",
                (symbol, setup_name, candle_ts)
            )
            return cursor.fetchone() is not None

    def record_signal(self, symbol: str, setup_name: str, candle_ts: int, msg_id: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO sent_signals (symbol, setup_name, candle_timestamp, telegram_msg_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (symbol, setup_name, candle_ts, msg_id, int(time.time()))
            )
            conn.commit()

    def save_active_setup(self, data: dict):
        now = int(time.time())
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO active_setups (
                    setup_id, symbol, signal_timestamp, reference_price, breakout_price,
                    atr_h1, atr_h4, initial_score, h4_structure, invalidation_price,
                    highest_high, lowest_low, trailing_stop, status, last_score, tp_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['setup_id'], data['symbol'], data['signal_timestamp'], data['reference_price'],
                data['breakout_price'], data['atr_h1'], data['atr_h4'], data['initial_score'],
                data['h4_structure'], data['invalidation_price'], data['highest_high'],
                data['lowest_low'], data['trailing_stop'], data.get('status', 'ACTIVE'),
                data['initial_score'], data.get('tp_status', 'MONITOR'), now
            ))
            conn.commit()

    def get_active_setups(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM active_setups WHERE status IN ('ACTIVE', 'SMART_TP', 'EXTENDED')")
            return [dict(row) for row in cursor.fetchall()]

    def update_setup_progress(self, setup_id: str, highest_high: float, lowest_low: float, trailing_stop: float, last_score: int, status: str, tp_status: str):
        now = int(time.time())
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE active_setups
                SET highest_high = MAX(highest_high, ?),
                    lowest_low = MIN(lowest_low, ?),
                    trailing_stop = MAX(trailing_stop, ?),
                    last_score = ?,
                    status = ?,
                    tp_status = ?,
                    updated_at = ?
                WHERE setup_id = ?
            """, (highest_high, lowest_low, trailing_stop, last_score, status, tp_status, now, setup_id))
            conn.commit()

    def archive_setup(self, setup_id: str, exit_reason: str):
        now = int(time.time())
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM active_setups WHERE setup_id = ?", (setup_id,))
            setup = cursor.fetchone()
            if setup:
                s = dict(setup)
                ref = s['reference_price']
                max_h = s['highest_high']
                min_l = s['lowest_low']
                max_gain = ((max_h - ref) / ref) * 100 if ref > 0 else 0
                max_dd = ((min_l - ref) / ref) * 100 if ref > 0 else 0
                duration = (now - s['signal_timestamp']) / 3600.0

                cursor.execute("""
                    INSERT OR REPLACE INTO setup_history (
                        setup_id, symbol, signal_timestamp, reference_price, entry_price_next,
                        max_high, min_low, max_gain_pct, max_drawdown_pct, wave_duration_hours,
                        exit_reason, final_score, closed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    s['setup_id'], s['symbol'], s['signal_timestamp'], ref, ref,
                    max_h, min_l, max_gain, max_dd, duration, exit_reason, s['last_score'], now
                ))
                cursor.execute("DELETE FROM active_setups WHERE setup_id = ?", (setup_id,))
                conn.commit()

    def get_stats(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total, AVG(max_gain_pct) as avg_gain, MAX(max_gain_pct) as peak_gain FROM setup_history")
            res = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) as win10 FROM setup_history WHERE max_gain_pct >= 10.0")
            w10 = cursor.fetchone()['win10']
            cursor.execute("SELECT COUNT(*) as win15 FROM setup_history WHERE max_gain_pct >= 15.0")
            w15 = cursor.fetchone()['win15']
            return {
                'total': res['total'] or 0,
                'avg_gain': res['avg_gain'] or 0.0,
                'peak_gain': res['peak_gain'] or 0.0,
                'win10_pct': (w10 / res['total'] * 100) if res['total'] else 0.0,
                'win15_pct': (w15 / res['total'] * 100) if res['total'] else 0.0,
            }

# ==============================================================================
# 4. BYBIT CLIENT
# ==============================================================================
class BybitClient:
    def __init__(self, base_url=BYBIT_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()

    def get_tickers(self, category=CATEGORY):
        url = f"{self.base_url}/v5/market/tickers?category={category}"
        try:
            res = self.session.get(url, timeout=8).json()
            if res.get("retCode") == 0:
                return res["result"]["list"]
        except Exception as e:
            print(f"[API ERROR] get_tickers: {e}")
        return []

    def filter_qualified_symbols(self, min_turnover=MIN_TURNOVER_24H, max_spread=MAX_SPREAD_PCT):
        tickers = self.get_tickers()
        qualified = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            turnover = float(t.get("turnover24h", 0))
            bid = float(t.get("bid1Price", 0))
            ask = float(t.get("ask1Price", 0))
            spread = ((ask - bid) / bid * 100) if bid > 0 else 99.0

            if turnover >= min_turnover and spread <= max_spread:
                qualified.append({
                    "symbol": sym,
                    "turnover": turnover,
                    "last_price": float(t.get("lastPrice", 0)),
                    "price_change_24h": float(t.get("price24hPcnt", 0)) * 100
                })
        return sorted(qualified, key=lambda x: x["turnover"], reverse=True)

    def get_klines(self, symbol: str, interval: str, limit: int = 150) -> pd.DataFrame:
        url = f"{self.base_url}/v5/market/kline?category={CATEGORY}&symbol={symbol}&interval={interval}&limit={limit+1}"
        try:
            res = self.session.get(url, timeout=6).json()
            if res.get("retCode") == 0 and res.get("result", {}).get("list"):
                raw = res["result"]["list"][1:] # Exclude unclosed current candle
                df = pd.DataFrame(raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                df = df.iloc[::-1].reset_index(drop=True).astype(float)
                df['timestamp'] = df['timestamp'].astype(int)
                return df
        except Exception:
            pass
        return pd.DataFrame()

# ==============================================================================
# 5. TECHNICAL ANALYZER
# ==============================================================================
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return df
    
    closes = df['close']
    highs = df['high']
    lows = df['low']
    vols = df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['ema50'] = closes.ewm(span=50, adjust=False).mean()
    df['ema200'] = closes.ewm(span=200, adjust=False).mean()

    hl = highs - lows
    hc = (highs - closes.shift()).abs()
    lc = (lows - closes.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()

    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss.replace(0, 1e-9))
    df['rsi14'] = 100 - (100 / (1 + rs))

    up_move = highs - highs.shift()
    down_move = lows.shift() - lows
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
    tr_smooth = tr.rolling(14).sum()
    plus_di = 100 * (pd.Series(plus_dm).rolling(14).sum() / tr_smooth.replace(0, 1e-9))
    minus_di = 100 * (pd.Series(minus_dm).rolling(14).sum() / tr_smooth.replace(0, 1e-9))
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-9))
    
    df['plus_di'] = plus_di
    df['minus_di'] = minus_di
    df['adx14'] = dx.rolling(14).mean()

    df['vol_sma20'] = vols.rolling(20).mean()
    df['rvol'] = vols / (df['vol_sma20'].replace(0, 1e-9))

    return df

def find_pivots(df: pd.DataFrame, left: int = H4_PIVOT_LEFT, right: int = H4_PIVOT_RIGHT):
    pivots_high = []
    pivots_low = []
    
    for i in range(left, len(df) - right):
        current_h = df['high'].iloc[i]
        current_l = df['low'].iloc[i]
        
        if all(current_h > x for x in df['high'].iloc[i-left:i]) and all(current_h > x for x in df['high'].iloc[i+1:i+1+right]):
            pivots_high.append((i, df['timestamp'].iloc[i], current_h))
            
        if all(current_l < x for x in df['low'].iloc[i-left:i]) and all(current_l < x for x in df['low'].iloc[i+1:i+1+right]):
            pivots_low.append((i, df['timestamp'].iloc[i], current_l))
            
    return pivots_high, pivots_low

def analyze_h4_structure(df_h4: pd.DataFrame):
    if df_h4.empty or len(df_h4) < 30:
        return {"structure": "NEUTRAL", "choch": False, "bos": False, "last_swing_high": 0.0, "last_swing_low": 0.0}

    pivots_h, pivots_l = find_pivots(df_h4)
    if not pivots_h or not pivots_l:
        return {"structure": "NEUTRAL", "choch": False, "bos": False, "last_swing_high": 0.0, "last_swing_low": 0.0}

    last_high = pivots_h[-1][2]
    last_low = pivots_l[-1][2]
    latest_close = df_h4['close'].iloc[-1]
    atr_h4 = df_h4['atr14'].iloc[-1] if 'atr14' in df_h4 else 0.0

    is_choch = latest_close > (last_high + 0.1 * atr_h4)
    ema20 = df_h4['ema20'].iloc[-1] if 'ema20' in df_h4 else 0.0
    ema50 = df_h4['ema50'].iloc[-1] if 'ema50' in df_h4 else 0.0
    
    structure = "NEUTRAL"
    if is_choch or (latest_close > ema20 and ema20 > ema50):
        structure = "BULLISH"
    elif latest_close < last_low:
        structure = "BEARISH"

    return {
        "structure": structure, "choch": is_choch, "bos": latest_close > last_high,
        "last_swing_high": last_high, "last_swing_low": last_low
    }

def analyze_h1_breakout(df_h1: pd.DataFrame):
    if df_h1.empty or len(df_h1) < 25:
        return {"breakout": False, "accumulation": False, "breakout_price": 0.0, "atr_h1": 0.0}

    latest = df_h1.iloc[-1]
    atr_h1 = latest['atr14']
    
    acc_high = df_h1['high'].iloc[-13:-1].max()
    acc_low = df_h1['low'].iloc[-13:-1].min()
    acc_range = acc_high - acc_low
    is_acc = acc_range <= (4.0 * atr_h1)

    close, open_p, high, low = latest['close'], latest['open'], latest['high'], latest['low']
    body = abs(close - open_p)
    candle_range = high - low if (high - low) > 0 else 1.0
    
    is_body_60 = (body / candle_range) >= 0.60
    is_vol_1_8x = latest['rvol'] >= 1.8
    is_above_acc = close > acc_high
    is_rsi_valid = 55.0 <= latest['rsi14'] <= 72.0
    is_ema_up = latest['ema20'] > df_h1['ema20'].iloc[-3]
    dist_to_acc = (close - acc_high) / (atr_h1 if atr_h1 > 0 else 1.0)
    is_dist_valid = dist_to_acc <= 1.5

    is_breakout = is_acc and is_above_acc and is_vol_1_8x and is_body_60 and is_rsi_valid and is_ema_up and is_dist_valid

    return {
        "breakout": is_breakout, "accumulation": is_acc, "breakout_price": acc_high,
        "atr_h1": atr_h1, "close": close, "acc_low": acc_low
    }

# ==============================================================================
# 6. WAVE STRENGTH ENGINE
# ==============================================================================
def calculate_wave_strength(df_h1: pd.DataFrame, df_h4: pd.DataFrame, btc_df_h1: pd.DataFrame = None) -> int:
    if df_h1.empty or df_h4.empty or len(df_h1) < 20 or len(df_h4) < 20:
        return 0

    h1_last = df_h1.iloc[-1]
    h4_last = df_h4.iloc[-1]
    score = 0

    if h4_last['close'] > h4_last['ema20'] and h4_last['ema20'] >= h4_last['ema50']:
        score += 25
    elif h4_last['close'] > h4_last['ema200']:
        score += 15

    if h4_last['adx14'] >= 25.0 and h4_last['plus_di'] > h4_last['minus_di']:
        score += 20
    elif h4_last['plus_di'] > h4_last['minus_di']:
        score += 10

    rsi_h1, rsi_h4 = h1_last['rsi14'], h4_last['rsi14']
    if (55.0 <= rsi_h1 <= 72.0) and (rsi_h4 >= 55.0):
        score += 15
    elif rsi_h1 >= 50.0:
        score += 8

    rvol = h1_last['rvol']
    if rvol >= 2.0:
        score += 20
    elif rvol >= 1.5:
        score += 12

    if h4_last['ema20'] > df_h4['ema20'].iloc[-3]:
        score += 10

    if btc_df_h1 is not None and not btc_df_h1.empty and len(btc_df_h1) >= 12:
        btc_ret = (btc_df_h1['close'].iloc[-1] - btc_df_h1['close'].iloc[-12]) / btc_df_h1['close'].iloc[-12]
        sym_ret = (h1_last['close'] - df_h1['close'].iloc[-12]) / df_h1['close'].iloc[-12]
        if (sym_ret - btc_ret) >= 0.02:
            score += 10
        elif sym_ret > btc_ret:
            score += 5
    else:
        score += 5

    return min(score, 100)

# ==============================================================================
# 7. SMART TP ENGINE
# ==============================================================================
class SmartTPEngine:
    @staticmethod
    def evaluate_setup(setup: dict, df_h1: pd.DataFrame, df_h4: pd.DataFrame, current_score: int):
        ref_price = setup['reference_price']
        current_price = df_h1['close'].iloc[-1]
        atr_h1, atr_h4 = df_h1['atr14'].iloc[-1], df_h4['atr14'].iloc[-1]

        current_gain_pct = ((current_price - ref_price) / ref_price) * 100.0 if ref_price > 0 else 0
        new_highest = max(setup['highest_high'], current_price)
        new_lowest = min(setup['lowest_low'], current_price)

        trail_buffer = max(TRAILING_ATR_H1_MULT * atr_h1, TRAILING_ATR_H4_MULT * atr_h4)
        computed_trailing = new_highest - trail_buffer
        new_trailing_stop = max(setup['trailing_stop'], computed_trailing)

        if current_price <= setup['invalidation_price'] or current_price <= new_trailing_stop:
            return {
                "action": "CLOSE",
                "exit_reason": "TRAILING_STOP_HIT" if current_price <= new_trailing_stop else "INVALIDATED",
                "gain_pct": current_gain_pct,
                "highest_high": new_highest,
                "lowest_low": new_lowest,
                "trailing_stop": new_trailing_stop,
                "tp_status": "CLOSED"
            }

        new_tp_status = setup['tp_status']
        if current_gain_pct >= 10.0:
            if current_score < 60:
                new_tp_status = "TP_NEAR_WEAK"
            elif 60 <= current_score <= 79:
                new_tp_status = "TP_MONITOR_MODERATE"
            else:
                new_tp_status = "TP_EXTEND_STRONG"

        return {
            "action": "UPDATE",
            "gain_pct": current_gain_pct,
            "highest_high": new_highest,
            "lowest_low": new_lowest,
            "trailing_stop": new_trailing_stop,
            "tp_status": new_tp_status,
            "score": current_score
        }

# ==============================================================================
# 8. TELEGRAM SERVICE
# ==============================================================================
class TelegramService:
    def __init__(self, token=BOT_TOKEN, chat_id=CHAT_ID):
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.token}"

    def send_message(self, text: str, reply_markup=None) -> dict:
        if not self.token or not self.chat_id:
            return {}

        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)

        try:
            res = requests.post(url, json=payload, timeout=8).json()
            return res
        except Exception as e:
            print(f"[TELEGRAM ERROR] send_message: {e}")
        return {}

    def get_updates(self, offset=None):
        if not self.token:
            return []
        url = f"{self.api_url}/getUpdates?timeout=2"
        if offset:
            url += f"&offset={offset}"
        try:
            res = requests.get(url, timeout=5).json()
            if res.get("ok"):
                return res.get("result", [])
        except Exception:
            pass
        return []

    def format_confirmed_alert(self, symbol: str, ref_price: float, h4_struct: str, score: int, vol_rvol: float, rsi_h4: float, adx_h4: float, inv_price: float):
        bybit_url = f"https://www.bybit.com/trade/usdt/{symbol}"
        keyboard = {"inline_keyboard": [[{"text": f"📈 Mở Chart Bybit #{symbol}", "url": bybit_url}]]}

        msg = f"""🚀 <b>BYBIT SMART WAVE – CONFIRMED</b>
🪙 <b>Coin:</b> #{symbol}
⏱ <b>Timeframe:</b> H1
💰 <b>Reference Price:</b> <code>{ref_price:.4f}</code>
📈 <b>H4 Structure:</b> {h4_struct}
📊 <b>Wave Strength:</b> <b>{score}/100</b>
🔥 <b>Volume:</b> <code>{vol_rvol:.1f}x SMA20</code>
📉 <b>RSI H4:</b> {rsi_h4:.1f} | <b>ADX H4:</b> {adx_h4:.1f}
🌊 <b>Wave Phase:</b> EARLY EXPANSION
🎯 <b>TP Watch Zone:</b> Kháng cự H4 kế tiếp
⚠️ <b>Invalidation:</b> <code>{inv_price:.4f}</code>
📌 <b>Status:</b> CONFIRMED – PAPER SIGNAL
<i>Thời gian: GMT+7</i>"""
        return msg, keyboard

    def format_tp_near_alert(self, symbol: str, gain_pct: float, score: int):
        return f"""⚠️ <b>SMART TP ALERT – TP NEAR</b>
🪙 <b>Coin:</b> #{symbol}
📈 <b>Mức tăng mô phỏng:</b> <code>+{gain_pct:.2f}%</code>
📊 <b>Wave Strength:</b> <b>{score}/100 (Yếu)</b>
📉 <b>Động lượng:</b> Có dấu hiệu suy yếu / Kháng cự gần
🎯 <b>TP STATUS:</b> NEAR / WEAK
📌 <b>Cảnh báo nguy cơ sóng kết thúc. Cân nhắc bảo toàn lợi nhuận!</b>"""

    def format_tp_extended_alert(self, symbol: str, gain_pct: float, score: int):
        return f"""🔥 <b>STRONG WAVE DETECTED – TP EXTENDED</b>
🪙 <b>Coin:</b> #{symbol}
🚀 <b>Mức tăng mô phỏng:</b> <code>+{gain_pct:.2f}%</code>
💪 <b>Wave Strength:</b> <b>{score}/100 (Strong)</b>
📈 <b>H4 Structure:</b> BULLISH
🔥 <b>Momentum:</b> VERY STRONG
🎯 <b>TP STATUS:</b> EXTENDED
📌 <b>Hệ thống tiếp tục gồng lãi theo Trailing Stop!</b>"""

# ==============================================================================
# 9. MAIN ORCHESTRATOR
# ==============================================================================
class SmartWaveBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.bybit = BybitClient()
        self.telegram = TelegramService()
        self.is_paused = False
        self.last_update_id = None

    def handle_telegram_commands(self):
        updates = self.telegram.get_updates(offset=self.last_update_id)
        for update in updates:
            self.last_update_id = update["update_id"] + 1
            msg = update.get("message", {})
            text = msg.get("text", "").strip()
            
            if not text:
                continue

            if text in ["/start", "/help"]:
                menu = """<b>🚀 BYBIT H1 SMART WAVE PRO BOT</b>
Available Commands:
/scan - Quét ngay lập tức toàn thị trường
/active - Xem các setup đang theo dõi
/stats - Xem thống kê hiệu suất mô phỏng
/pause - Tạm dừng gửi cảnh báo
/resume - Bật lại cảnh báo"""
                self.telegram.send_message(menu)

            elif text == "/scan":
                self.telegram.send_message("🔍 Đang khởi chạy lệnh quét nhanh toàn thị trường...")
                self.run_scan_cycle()
                self.telegram.send_message("✅ Đã quét xong toàn thị trường!")

            elif text == "/active":
                active = self.db.get_active_setups()
                if not active:
                    self.telegram.send_message("📌 Hiện chưa có setup nào đang theo dõi.")
                else:
                    lines = [f"<b>📊 ACTIVE SETUPS ({len(active)}):</b>"]
                    for a in active:
                        lines.append(f"• <b>#{a['symbol']}</b> | Ref: <code>{a['reference_price']}</code> | Score: {a['last_score']} | Status: {a['tp_status']}")
                    self.telegram.send_message("\n".join(lines))

            elif text == "/stats":
                stats = self.db.get_stats()
                msg = f"""<b>📊 HỆ THỐNG THỐNG KÊ TÍN HIỆU MÔ PHỎNG</b>
• Tổng tín hiệu: <b>{stats['total']}</b>
• Đạt mốc +10%: <b>{stats['win10_pct']:.1f}%</b>
• Đạt mốc +15%: <b>{stats['win15_pct']:.1f}%</b>
• Mức tăng trung bình: <b>+{stats['avg_gain']:.2f}%</b>
• Đỉnh tăng cao nhất: <b>+{stats['peak_gain']:.2f}%</b>"""
                self.telegram.send_message(msg)

            elif text == "/pause":
                self.is_paused = True
                self.telegram.send_message("⏸ Đã tạm dừng phát cảnh báo Telegram.")

            elif text == "/resume":
                self.is_paused = False
                self.telegram.send_message("▶️ Đã bật lại cảnh báo Telegram.")

    def run_scan_cycle(self):
        print("🔍 [SCAN] Gathering market tickers...", flush=True)
        qualified_symbols = self.bybit.filter_qualified_symbols()

        btc_df_h1 = self.bybit.get_klines("BTCUSDT", "60", limit=100)
        if not btc_df_h1.empty:
            btc_df_h1 = add_indicators(btc_df_h1)

        for item in qualified_symbols[:MAX_SYMBOLS_SCAN]:
            symbol = item['symbol']
            
            df_h1 = self.bybit.get_klines(symbol, "60", limit=100)
            df_h4 = self.bybit.get_klines(symbol, "240", limit=100)
            
            if df_h1.empty or df_h4.empty or len(df_h1) < 30 or len(df_h4) < 30:
                continue

            df_h1 = add_indicators(df_h1)
            df_h4 = add_indicators(df_h4)

            h4_analysis = analyze_h4_structure(df_h4)
            h1_analysis = analyze_h1_breakout(df_h1)

            score = calculate_wave_strength(df_h1, df_h4, btc_df_h1)

            if h1_analysis['breakout'] and h4_analysis['structure'] == 'BULLISH' and score >= 70:
                h1_last = df_h1.iloc[-1]
                candle_ts = int(h1_last['timestamp'])

                if not self.db.is_signal_sent(symbol, "CONFIRMED_LONG", candle_ts):
                    setup_id = f"{symbol}_{candle_ts}"
                    ref_price = h1_analysis['close']
                    inv_price = h1_analysis['acc_low'] - (0.3 * h1_analysis['atr_h1'])

                    setup_data = {
                        "setup_id": setup_id,
                        "symbol": symbol,
                        "signal_timestamp": candle_ts,
                        "reference_price": ref_price,
                        "breakout_price": h1_analysis['breakout_price'],
                        "atr_h1": h1_analysis['atr_h1'],
                        "atr_h4": df_h4['atr14'].iloc[-1],
                        "initial_score": score,
                        "h4_structure": h4_analysis['structure'],
                        "invalidation_price": inv_price,
                        "highest_high": ref_price,
                        "lowest_low": ref_price,
                        "trailing_stop": inv_price,
                        "status": "ACTIVE",
                        "tp_status": "MONITOR"
                    }
                    self.db.save_active_setup(setup_data)

                    if not self.is_paused:
                        msg, keyboard = self.telegram.format_confirmed_alert(
                            symbol, ref_price, h4_analysis['structure'], score,
                            h1_last['rvol'], df_h4['rsi14'].iloc[-1], df_h4['adx14'].iloc[-1], inv_price
                        )
                        res = self.telegram.send_message(msg, reply_markup=keyboard)
                        msg_id = res.get("result", {}).get("message_id")
                        self.db.record_signal(symbol, "CONFIRMED_LONG", candle_ts, msg_id)

    def update_active_setups_progress(self):
        active_list = self.db.get_active_setups()
        if not active_list:
            return

        btc_df_h1 = self.bybit.get_klines("BTCUSDT", "60", limit=100)
        if not btc_df_h1.empty:
            btc_df_h1 = add_indicators(btc_df_h1)

        for setup in active_list:
            sym = setup['symbol']
            df_h1 = self.bybit.get_klines(sym, "60", limit=100)
            df_h4 = self.bybit.get_klines(sym, "240", limit=100)
            if df_h1.empty or df_h4.empty:
                continue

            df_h1 = add_indicators(df_h1)
            df_h4 = add_indicators(df_h4)

            current_score = calculate_wave_strength(df_h1, df_h4, btc_df_h1)
            eval_res = SmartTPEngine.evaluate_setup(setup, df_h1, df_h4, current_score)

            if eval_res["action"] == "CLOSE":
                self.db.archive_setup(setup['setup_id'], eval_res['exit_reason'])
                if not self.is_paused:
                    self.telegram.send_message(f"⏹ <b>SETUP CLOSED #{sym}</b>\n• Mức tăng cuối: <code>+{eval_res['gain_pct']:.2f}%</code>\n• Lý do: {eval_res['exit_reason']}")
            else:
                self.db.update_setup_progress(
                    setup['setup_id'],
                    eval_res['highest_high'],
                    eval_res['lowest_low'],
                    eval_res['trailing_stop'],
                    current_score,
                    "ACTIVE",
                    eval_res['tp_status']
                )

                if eval_res['tp_status'] != setup['tp_status'] and not self.is_paused:
                    if eval_res['tp_status'] == "TP_NEAR_WEAK":
                        alert_msg = self.telegram.format_tp_near_alert(sym, eval_res['gain_pct'], current_score)
                        self.telegram.send_message(alert_msg)
                    elif eval_res['tp_status'] == "TP_EXTEND_STRONG":
                        alert_msg = self.telegram.format_tp_extended_alert(sym, eval_res['gain_pct'], current_score)
                        self.telegram.send_message(alert_msg)

    def start(self):
        Thread(target=run_flask, daemon=True).start()
        print("⚡️ Bybit H1 Smart Wave Pro Engine initialized 24/7...", flush=True)

        while True:
            try:
                self.handle_telegram_commands()
                self.run_scan_cycle()
                self.update_active_setups_progress()
            except Exception as e:
                print(f"[ENGINE ERROR]: {e}", flush=True)

            time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    bot = SmartWaveBot()
    bot.start()
