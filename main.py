import os
import time
import requests
import numpy as np
import pandas as pd
import sqlite3
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. KHỞI TẠO FLASK KEEP-ALIVE SERVER
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Multi-Setup Scanner (5 Setups - Pre-Pump Engine Active) 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BIẾN MÔI TRƯỜNG & THÔNG SỐ SÀN
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "2000000"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.25"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "150"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))

MAX_DROP_24H_PCT = -12.0  # Lọc Anti-Dump

MAX_ACTIVE_TRADES = 5
DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE (BẢO TOÀN SETUP CŨ & CHỐNG GỬI TRÙNG)
# ==============================================================================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sent_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                setup_name TEXT NOT NULL,
                candle_timestamp INTEGER NOT NULL,
                last_sent_at INTEGER NOT NULL,
                UNIQUE(symbol, setup_name, candle_timestamp)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                status TEXT DEFAULT 'OPEN',
                setup_name TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)
        conn.commit()

def is_symbol_currently_active(symbol: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM active_trades WHERE symbol = ? AND status = 'OPEN'", (symbol,))
        return cursor.fetchone() is not None

def get_active_trades_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM active_trades WHERE status = 'OPEN'")
        return cursor.fetchone()[0]

def is_signal_already_sent(symbol: str, setup_name: str, candle_ts: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sent_signals WHERE symbol = ? AND setup_name = ? AND candle_timestamp = ?",
            (symbol, setup_name, candle_ts)
        )
        return cursor.fetchone() is not None

def record_sent_signal(symbol: str, setup_name: str, candle_ts: int):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO sent_signals (symbol, setup_name, candle_timestamp, last_sent_at) VALUES (?, ?, ?, ?)",
            (symbol, setup_name, candle_ts, now_ts)
        )
        conn.commit()

def save_active_trade(sig: dict):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO active_trades (symbol, entry, sl, tp1, tp2, status, setup_name, created_at)
            VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?)
        """, (sig['symbol'], sig['market_entry'], sig['sl'], sig['tp1'], sig['tp2'], sig['internal_setup_name'], now_ts))
        conn.commit()

# ==============================================================================
# 4. LẤY TICKERS BYBIT & KLINE DATA (H1/H4)
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_bybit_crypto_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto_exact = ['SOXS', 'SOXL', 'TQQQ', 'SQQQ', 'UVXY', 'SPY', 'QQQ', 'DIA', 'IWM', 'TLT', 'US30', 'US100', 'US500', 'XAU', 'XAG', 'WTI', 'USOIL', '3L', '3S']
            valid = []
            for t in tickers:
                sym = t.get("symbol", "")
                turnover = safe_float(t.get("turnover24h", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                price_change_pct = safe_float(t.get("price24hPcnt", 0)) * 100.0
                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI", "EUR")):
                    base_coin = sym.replace("USDT", "")
                    if base_coin not in non_crypto_exact and turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT and price_change_pct >= MAX_DROP_24H_PCT:
                        valid.append({"symbol": sym, "turnover24h": turnover, "priceChange24h": price_change_pct})
            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[API ERROR] Lỗi lấy Tickers Bybit: {e}", flush=True)
    return []

def get_closed_klines_df(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 3: return pd.DataFrame()
            closed_raw = raw_list[1:]
            df = pd.DataFrame(closed_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume', 'timestamp']:
                df[col] = df[col].astype(float)
            return df
    except Exception as e:
        print(f"[API ERROR] Lỗi lấy Klines {symbol} ({interval}): {e}", flush=True)
    return pd.DataFrame()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 200: return df
    closes = df['close']
    df['ema21'] = closes.ewm(span=21, adjust=False).mean()
    df['ema200'] = closes.ewm(span=200, adjust=False).mean()
    
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    return df

# ==============================================================================
# 5. CÁC HÀM NHẬN DIỆN MÔ HÌNH NẾN & 5 SETUP GIAO DỊCH
# ==============================================================================
def check_extended_bullish_reversal_patterns(df_h1: pd.DataFrame) -> tuple:
    if len(df_h1) < 4: return False, "Không đủ dữ liệu nến H1"

    c1, c2, c3 = df_h1.iloc[-1], df_h1.iloc[-2], df_h1.iloc[-3]
    c1_o, c1_c, c1_h, c1_l = c1['open'], c1['close'], c1['high'], c1['low']
    c2_o, c2_c, c2_h, c2_l = c2['open'], c2['close'], c2['high'], c2['low']
    c3_o, c3_c, c3_h, c3_l = c3['open'], c3['close'], c3['high'], c3['low']

    c1_range = c1_h - c1_l
    c1_body = abs(c1_c - c1_o)
    c1_lower_shadow = min(c1_o, c1_c) - c1_l
    c1_upper_shadow = c1_h - max(c1_o, c1_c)

    c2_range = c2_h - c2_l
    c2_body = abs(c2_c - c2_o)

    if c1_range == 0 or c1_body <= 0.03 * c1_range: return False, "Nến Doji hẹp / không biên độ"

    is_c2_bear = c2_c < c2_o
    is_c1_bull = c1_c > c1_o

    # 1. Bullish Engulfing
    if is_c2_bear and is_c1_bull and (c1_c > c2_o) and (c1_o < c2_c) and (c1_body >= 0.5 * c1_range):
        return True, "Bullish Engulfing"
    # 2. Hammer
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_body > 0.05 * c1_range) and (c1_c >= (c1_l + 0.5 * c1_range)):
        return True, "Hammer / Rút râu dưới"
    # 3. Bullish Pin Bar
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_upper_shadow <= 0.25 * c1_range) and (c1_c >= (c1_h - 0.3 * c1_range)):
        return True, "Bullish Pin Bar"
    # 4. Morning Star
    if (c3_c < c3_o) and (abs(c2_c - c2_o) <= 0.3 * (c3_h - c3_l)) and is_c1_bull and (c1_c >= (c3_o + c3_c) / 2):
        return True, "Morning Star"
    # 5. Piercing Line
    if is_c2_bear and is_c1_bull and (c1_o <= c2_c * 1.002) and (c1_c > (c2_o + c2_c)/2) and (c1_c < c2_o):
        return True, "Piercing Line"
    # 6. Tweezer Bottom
    atr_14 = c1['atr14'] if 'atr14' in c1 else (c1_range + c2_range) / 2.0
    if is_c2_bear and is_c1_bull and (abs(c1_l - c2_l) <= 0.12 * atr_14) and (c1_c > c1_o) and (c2_body <= 1.8 * atr_14) and (c1_lower_shadow >= 0.3 * c1_range):
        return True, "Tweezer Bottom"
    # 7. Three White Soldiers
    if (c3_c > c3_o) and (c2_c > c2_o) and is_c1_bull and (c1_c > c2_c > c3_c) and (c1_body >= 0.4 * c1_range):
        return True, "Three White Soldiers"

    return False, "Không có mô hình nến đảo chiều tăng"

# --- SETUP 1 ---
def analyze_setup_1(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200: return None, "Thiếu dữ liệu"
    last_h1, last_h4 = df_h1.iloc[-1], df_h4.iloc[-1]

    if last_h4['close'] <= last_h4['ema200'] or last_h1['close'] <= last_h1['ema21']: return None, "Sai xu hướng EMA"
    dist_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    if dist_pct < 1.0 or dist_pct > 3.0: return None, f"Khoảng cách EMA21 không thuộc 1-3% ({dist_pct:.2f}%)"

    if not any(row['low'] <= row['ema21'] * 1.008 for _, row in df_h1.iloc[-6:-1].iterrows()):
        return None, "Chưa hồi sát EMA21 H1"

    has_pattern, pattern_name = check_extended_bullish_reversal_patterns(df_h1)
    if not has_pattern: return None, pattern_name

    entry = last_h1['close']
    sl = min(df_h1['low'].iloc[-6:-1].min(), last_h1['low']) * 0.997
    risk_pct = ((entry - sl) / entry) * 100
    if risk_pct < 0.8: sl = entry * 0.985; risk_pct = 1.5

    tp1 = entry + (entry - sl) * 1.5
    tp2 = entry + (entry - sl) * 2.5
    rr = 1.5

    return {
        "symbol": symbol, "internal_setup_name": "H4_EMA200_H1_EMA21_REVERSAL",
        "tag": "🎯 SETUP 1: H4 EMA200 + H1 EMA21 REVERSAL LONG", "pattern_name": pattern_name,
        "candle_ts": int(last_h1['timestamp']), "market_entry": round(entry, 4), "sl": round(sl, 4),
        "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": round(rr, 2), "dist_pct": round(dist_pct, 2)
    }, "OK"

# --- SETUP 2 ---
def analyze_setup_2_pure_h4(symbol: str, df_h4: pd.DataFrame) -> tuple:
    if df_h4.empty or len(df_h4) < 200: return None, "Thiếu dữ liệu H4"
    last_h4, prev_h4 = df_h4.iloc[-1], df_h4.iloc[-2]
    h4_o, h4_c, h4_h, h4_l = last_h4['open'], last_h4['close'], last_h4['high'], last_h4['low']
    h4_ema200 = last_h4['ema200']

    if h4_c <= h4_ema200: return None, "H4 Close <= EMA200"
    dist_low_pct = abs(h4_l - h4_ema200) / h4_ema200 * 100
    if dist_low_pct < 0.2 or dist_low_pct > 3.0: return None, "Khoảng cách Low-EMA200 sai dải"

    h4_range = h4_h - h4_l
    if h4_range == 0: return None, "Biên độ = 0"
    h4_body = abs(h4_c - h4_o)
    shadow_ratio = (min(h4_o, h4_c) - h4_l) / h4_body if h4_body > 0 else 999.0
    if shadow_ratio < 2.0: return None, "Râu H4 chưa đủ dài"
    close_pos = ((h4_c - h4_l) / h4_range) * 100
    if close_pos < 75.0: return None, "Giá đóng H4 chưa thuộc Top 25%"

    entry = h4_c; sl = h4_l * 0.997
    tp1 = entry + (entry - sl) * 1.5; tp2 = entry + (entry - sl) * 2.5
    return {
        "symbol": symbol, "internal_setup_name": "H4_EMA200_WICK_REVERSAL",
        "tag": "🔥 SETUP 2: H4 EMA200 WICK REVERSAL (PURE H4)", "h4_ema200": round(h4_ema200, 4),
        "dist_low_ema200": round(dist_low_pct, 2), "shadow_ratio": round(shadow_ratio, 2),
        "close_pos_pct": round(close_pos, 1), "candle_ts": int(last_h4['timestamp']),
        "market_entry": round(entry, 4), "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": 1.5
    }, "OK"

# --- SETUP 3 ---
def analyze_setup_3_early_pullback(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200: return None, "Thiếu dữ liệu"
    last_h1, prev_h1, last_h4 = df_h1.iloc[-1], df_h1.iloc[-2], df_h4.iloc[-1]
    if last_h4['close'] <= last_h4['ema200'] or last_h1['close'] <= last_h1['ema21'] or last_h1['ema21'] <= prev_h1['ema21']:
        return None, "Sai xu hướng"

    if not any(((row['low'] - row['ema21']) / row['ema21']) * 100 <= 0.5 for _, row in df_h1.iloc[-24:-1].iterrows()):
        return None, "Chưa nhúng sát EMA21 trong 12-24 nến"

    has_pattern, pattern_name = check_extended_bullish_reversal_patterns(df_h1)
    if not has_pattern: return None, pattern_name

    dist_close_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    if dist_close_pct > 1.5 or (last_h1['atr14'] > 0 and (last_h1['close'] - last_h1['ema21']) > last_h1['atr14']):
        return None, "Mua đuổi >1.5% hoặc >1 ATR"

    entry = last_h1['close']
    sl = min(df_h1['low'].iloc[-12:-1].min(), last_h1['low']) * 0.997
    tp1 = entry + (entry - sl) * 1.5; tp2 = entry + (entry - sl) * 2.5
    return {
        "symbol": symbol, "internal_setup_name": "EARLY_EMA21_PULLBACK_LONG",
        "tag": "🚀 SETUP 3: EARLY EMA21 PULLBACK LONG", "pattern_name": pattern_name,
        "candle_ts": int(last_h1['timestamp']), "market_entry": round(entry, 4),
        "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": 1.5, "dist_pct": round(dist_close_pct, 2)
    }, "OK"

# --- SETUP 4 ---
def analyze_setup_4_sweep_reclaim(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 50 or df_h4.empty or len(df_h4) < 50: return None, "Thiếu Klines"
    c1 = df_h1.iloc[-1]
    c1_range = c1['high'] - c1['low']
    if c1_range == 0: return None, "Biên độ = 0"

    c1_lower_shadow = min(c1['open'], c1['close']) - c1['low']
    is_sweep = (c1_lower_shadow / c1_range) >= 0.45
    h4_ema200 = df_h4.iloc[-1]['ema200']
    is_reclaim = (c1['close'] >= h4_ema200 * 0.998) or (c1['close'] >= df_h1['low'].iloc[-15:-1].min())

    if not (is_sweep and is_reclaim): return None, "Chưa Sweep/Reclaim"

    avg_vol = df_h1['volume'].iloc[-21:-1].mean()
    vol_ratio = c1['volume'] / avg_vol if avg_vol > 0 else 1.0
    if vol_ratio < 1.5: return None, f"Volume bé ({vol_ratio:.1f}x)"

    entry = c1['close']; sl = c1['low'] * 0.995
    risk_pct = ((entry - sl) / entry) * 100
    tp1 = max(c1['high'], entry * 1.08); tp2 = entry * 1.18
    rr = round(((tp1 - entry) / entry) * 100 / risk_pct, 2)
    if rr < 1.5: return None, f"RR thấp ({rr})"

    return {
        "symbol": symbol, "internal_setup_name": "H1_LIQUIDITY_SWEEP_RECLAIM",
        "tag": "🧲 SETUP 4: H1 LIQUIDITY SWEEP & RECLAIM", "pattern_name": f"Sweep + Reclaim (Vol {vol_ratio:.1f}x)",
        "candle_ts": int(c1['timestamp']), "market_entry": round(entry, 4),
        "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": rr
    }, "OK"

# --- SETUP 5 (MỚI BỔ SUNG): H1 PRE-PUMP BASE & RECLAIM (BẮT ĐẦY TÍCH LŨY 0.003640) ---
def analyze_setup_5_prepump_base(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 50 or df_h4.empty or len(df_h4) < 50:
        return None, "Thiếu Klines"

    c1 = df_h1.iloc[-1]
    h4_last = df_h4.iloc[-1]
    h4_ema200 = h4_last['ema200']

    # 1. Điều kiện Khôi phục Hỗ trợ/H4 EMA200 (Reclaim)
    # Giá đóng cửa H1 nằm trên hoặc sát đường EMA200 H4 trong khoảng từ -0.3% đến +2.5%
    dist_to_h4_ema200 = ((c1['close'] - h4_ema200) / h4_ema200) * 100
    if dist_to_h4_ema200 < -0.3 or dist_to_h4_ema200 > 2.5:
        return None, f"Giá đóng chưa nằm sát/trên H4 EMA200 ({dist_to_h4_ema200:.2f}%)"

    # 2. Điều kiện Nén Biên Độ / Rũ Hàng (Compression / Sweep)
    # Râu dưới H1 chiếm >= 30% toàn nến HOẶC nến H1 trước đó là chuỗi nén biên độ sideway ở đáy
    c1_range = c1['high'] - c1['low']
    if c1_range == 0: return None, "Biên độ nến H1 bằng 0"
    c1_lower_shadow = min(c1['open'], c1['close']) - c1['low']
    shadow_pct = c1_lower_shadow / c1_range

    recent_low = df_h1['low'].iloc[-12:-1].min()
    is_sweep_low = c1['low'] <= recent_low * 1.002

    if shadow_pct < 0.30 and not is_sweep_low:
        return None, "Chưa có dấu hiệu rũ hàng / nén biên độ tại hỗ trợ"

    # 3. Điều kiện Khối lượng / Dòng tiền cạn kiệt hoặc nhích tăng chớm bùng nổ
    avg_vol_20 = df_h1['volume'].iloc[-21:-1].mean()
    vol_ratio = c1['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    if vol_ratio > 4.5:
        return None, f"Nến đã nổ Volume quá to/đã pump xa ({vol_ratio:.1f}x)"

    entry = c1['close']
    sl = min(c1['low'], recent_low) * 0.995
    risk_pct = ((entry - sl) / entry) * 100

    if risk_pct < 0.8:
        sl = entry * 0.985
        risk_pct = 1.5

    # Target TP theo tỷ lệ mở rộng sóng nổ
    tp1 = entry + (entry - sl) * 2.0
    tp2 = entry + (entry - sl) * 3.5
    rr = 2.0

    return {
        "symbol": symbol,
        "internal_setup_name": "H1_PREPUMP_BASE_RECLAIM",
        "tag": "🚀 SETUP 5: H1 PRE-PUMP BASE & RECLAIM (BUY NỀN BÙNG NỔ)",
        "pattern_name": f"Nén Đáy & Reclaim EMA200 (Risk: {risk_pct:.1f}%)",
        "candle_ts": int(c1['timestamp']),
        "market_entry": round(entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2),
        "dist_pct": round(dist_to_h4_ema200, 2)
    }, f"ĐẠT TÍN HIỆU PRE-PUMP (Entry: {entry})"

# ==============================================================================
# 6. GỬI CẢNH BÁO TELEGRAM
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    if sig['internal_setup_name'] == "H4_EMA200_WICK_REVERSAL":
        msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Entry (Close H4): <code>{sig['market_entry']:.4f}</code>
SL: <code>{sig['sl']:.4f}</code>
TP1: <code>{sig['tp1']:.4f}</code>
TP2: <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}"""
    elif sig['internal_setup_name'] == "H1_PREPUMP_BASE_RECLAIM":
        msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Mô hình: <b>{sig['pattern_name']}</b>
Entry (Nền Reclaim H1): <code>{sig['market_entry']:.4f}</code>
SL (Đáy nền): <code>{sig['sl']:.4f}</code>
TP1 (Sóng nổ 1): <code>{sig['tp1']:.4f}</code>
TP2 (Expansion): <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}

Cấu trúc Kỹ thuật Pre-Pump:
- Giá giữ vững H4 EMA200 (Cách {sig['dist_pct']}%) ✅
- Nén rũ hàng / Sweep Low thành công ✅
- Điểm mua an toàn trước sóng Pump dốc ✅"""
    else:
        msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Mô hình: <b>{sig['pattern_name']}</b>
Entry: <code>{sig['market_entry']:.4f}</code>
SL: <code>{sig['sl']:.4f}</code>
TP1: <code>{sig['tp1']:.4f}</code>
TP2: <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        print(f"[TELEGRAM ERROR]: {e}", flush=True)
        return False

# ==============================================================================
# 7. VÒNG LẶP SCANNER QUÉT BẤT TẬN 5 SETUP
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu Bybit Scanner (Song song 5 Setup Giao Dịch)...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            s1_sent = s2_sent = s3_sent = s4_sent = s5_sent = 0

            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                df_h1 = get_closed_klines_df(sym, "60", limit=220)
                df_h4 = get_closed_klines_df(sym, "240", limit=220)
                
                if df_h1.empty or df_h4.empty or len(df_h1) < 200 or len(df_h4) < 200: continue
                df_h1 = add_indicators(df_h1)
                df_h4 = add_indicators(df_h4)

                # --- 1. SETUP 1 ---
                sig1, _ = analyze_setup_1(sym, df_h1, df_h4)
                if sig1 and (get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym)):
                    if not is_signal_already_sent(sym, sig1['internal_setup_name'], sig1['candle_ts']):
                        if send_telegram_alert(sig1):
                            record_sent_signal(sym, sig1['internal_setup_name'], sig1['candle_ts']); save_active_trade(sig1)
                            s1_sent += 1

                # --- 2. SETUP 2 ---
                sig2, _ = analyze_setup_2_pure_h4(sym, df_h4)
                if sig2 and (get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym)):
                    if not is_signal_already_sent(sym, sig2['internal_setup_name'], sig2['candle_ts']):
                        if send_telegram_alert(sig2):
                            record_sent_signal(sym, sig2['internal_setup_name'], sig2['candle_ts']); save_active_trade(sig2)
                            s2_sent += 1

                # --- 3. SETUP 3 ---
                sig3, _ = analyze_setup_3_early_pullback(sym, df_h1, df_h4)
                if sig3 and (get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym)):
                    if not is_signal_already_sent(sym, sig3['internal_setup_name'], sig3['candle_ts']):
                        if send_telegram_alert(sig3):
                            record_sent_signal(sym, sig3['internal_setup_name'], sig3['candle_ts']); save_active_trade(sig3)
                            s3_sent += 1

                # --- 4. SETUP 4 ---
                sig4, _ = analyze_setup_4_sweep_reclaim(sym, df_h1, df_h4)
                if sig4 and (get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym)):
                    if not is_signal_already_sent(sym, sig4['internal_setup_name'], sig4['candle_ts']):
                        if send_telegram_alert(sig4):
                            record_sent_signal(sym, sig4['internal_setup_name'], sig4['candle_ts']); save_active_trade(sig4)
                            s4_sent += 1

                # --- 5. SETUP 5 (MỚI): PRE-PUMP BASE RECLAIM ---
                sig5, _ = analyze_setup_5_prepump_base(sym, df_h1, df_h4)
                if sig5 and (get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym)):
                    if not is_signal_already_sent(sym, sig5['internal_setup_name'], sig5['candle_ts']):
                        if send_telegram_alert(sig5):
                            record_sent_signal(sym, sig5['internal_setup_name'], sig5['candle_ts']); save_active_trade(sig5)
                            s5_sent += 1
                            print(f"✅ [SETUP 5 SENT] #{sym} | Entry Pre-Pump: {sig5['market_entry']}", flush=True)

            print(f"\n📊 --- BÁO CÁO VÒNG QUÉT (5 SETUP) ---", flush=True)
            print(f"• Setup 1 Sent: {s1_sent} | Setup 2 Sent: {s2_sent} | Setup 3 Sent: {s3_sent} | Setup 4 Sent: {s4_sent} | Setup 5 Sent: {s5_sent}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
