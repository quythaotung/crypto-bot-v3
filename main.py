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
    return "Bybit Dual-Setup (EMA21 Reversal & H4 EMA200 Wick) Active 24/7!"

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

MAX_ACTIVE_TRADES = 5
DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE (CHỐNG GỬI TRÙNG KÈO ĐỘC LẬP THEO SETUP)
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
# 4. LẤY TICKERS BYBIT & KLINE DATA (CHỈ XÉT NẾN ĐÃ ĐÓNG HOÀN TOÀN)
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
                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI", "EUR")):
                    base_coin = sym.replace("USDT", "")
                    if base_coin not in non_crypto_exact and turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                        valid.append({"symbol": sym, "turnover24h": turnover})

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
            
            closed_raw = raw_list[1:] # Bỏ nến realtime đang chạy [0]
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
# 5. CÁC HÀM PHÂN TÍCH NHẬN DIỆN NẾN VÀ SETUP
# ==============================================================================
def check_bullish_reversal_pattern(df_h1: pd.DataFrame) -> tuple:
    if len(df_h1) < 4: return False, "Không đủ dữ liệu nến"

    c1 = df_h1.iloc[-1]
    c2 = df_h1.iloc[-2]
    c3 = df_h1.iloc[-3]

    c1_o, c1_c, c1_h, c1_l = c1['open'], c1['close'], c1['high'], c1['low']
    c1_range = c1_h - c1_l
    c1_body = abs(c1_c - c1_o)
    c1_lower_shadow = min(c1_o, c1_c) - c1_l
    c1_upper_shadow = c1_h - max(c1_o, c1_c)

    if c1_range == 0: return False, "Nến Doji hẹp / không có biên độ"

    # 1. Bullish Engulfing
    is_c2_bear = c2['close'] < c2['open']
    is_c1_bull = c1_c > c1_o
    if is_c2_bear and is_c1_bull and (c1_c > c2['open']) and (c1_o < c2['close']) and (c1_body >= 0.5 * c1_range):
        return True, "Bullish Engulfing"

    # 2. Hammer / Nến rút râu dưới
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_body > 0.05 * c1_range) and (c1_c >= (c1_l + 0.5 * c1_range)):
        return True, "Hammer / Rút râu dưới"

    # 3. Bullish Pin Bar
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_upper_shadow <= 0.25 * c1_range) and (c1_c >= (c1_h - 0.3 * c1_range)):
        return True, "Bullish Pin Bar"

    # 4. Morning Star
    c2_body = abs(c2['close'] - c2['open'])
    c3_bear = c3['close'] < c3['open']
    if c3_bear and (c2_body <= 0.3 * (c3['high'] - c3['low'])) and is_c1_bull and (c1_c >= (c3['open'] + c3['close']) / 2):
        return True, "Morning Star"

    return False, "Không có mô hình nến đảo chiều tăng hợp lệ"

# --- SETUP 1: H4 EMA200 + H1 EMA21 REVERSAL ---
def analyze_setup_1(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200:
        return None, "Thiếu dữ liệu nến H1/H4"

    last_h1 = df_h1.iloc[-1]
    last_h4 = df_h4.iloc[-1]

    if last_h4['close'] <= last_h4['ema200']:
        return None, f"H4 Close <= EMA200 H4"

    if last_h1['close'] <= last_h1['ema21']:
        return None, f"H1 Close <= EMA21 H1"

    dist_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    atr_limit = (1.5 * last_h1['atr14'] / last_h1['close']) * 100 if last_h1['atr14'] > 0 else 1.5
    max_dist_allowed = max(1.5, atr_limit)

    if dist_pct > max_dist_allowed:
        return None, f"Giá đã tăng quá xa EMA21 ({dist_pct:.2f}% > {max_dist_allowed:.2f}%)"

    recent_klines = df_h1.iloc[-6:-1]
    touched_ema21 = any(row['low'] <= row['ema21'] * 1.008 for _, row in recent_klines.iterrows())
    if not touched_ema21:
        return None, "Chưa có nhịp hồi gần/chạm EMA21 H1"

    has_pattern, pattern_name = check_bullish_reversal_pattern(df_h1)
    if not has_pattern:
        return None, pattern_name

    entry = last_h1['close']
    pullback_low = min(df_h1['low'].iloc[-6:-1].min(), last_h1['low'])
    sl = pullback_low * 0.997
    
    risk_pct = ((entry - sl) / entry) * 100
    if risk_pct < 0.8:
        sl = entry * 0.985
        risk_pct = ((entry - sl) / entry) * 100

    historical_highs = df_h1['high'].iloc[-60:-1]
    valid_highs = historical_highs[historical_highs > entry]

    if valid_highs.empty:
        tp1 = entry + (entry - sl) * 1.5
        tp2 = entry + (entry - sl) * 2.5
        rr = 1.5
    else:
        tp1_idx = valid_highs.iloc[::-1].idxmax()
        tp1 = df_h1['high'].loc[tp1_idx]
        higher_highs = historical_highs[historical_highs > tp1]
        tp2 = higher_highs.max() if not higher_highs.empty else tp1 + (tp1 - entry)
        rr = ((tp1 - entry) / entry) * 100 / risk_pct

    if rr < 1.5:
        return None, f"R:R quá thấp ({rr:.2f} < 1.5)"

    return {
        "symbol": symbol,
        "internal_setup_name": "H4_EMA200_H1_EMA21_REVERSAL",
        "tag": "🎯 SETUP 1: H4 EMA200 + H1 EMA21 REVERSAL LONG",
        "pattern_name": pattern_name,
        "candle_ts": int(last_h1['timestamp']),
        "market_entry": round(entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2),
        "dist_pct": round(dist_pct, 2)
    }, f"ĐẠT TÍN HIỆU SETUP 1 ({pattern_name})"


# --- SETUP 2 (MỚI): H4 EMA200 WICK REVERSAL LONG ---
def analyze_setup_2(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200:
        return None, "Thiếu dữ liệu nến H1/H4"

    last_h4 = df_h4.iloc[-1]
    prev_h4 = df_h4.iloc[-2]
    last_h1 = df_h1.iloc[-1]
    prev_h1 = df_h1.iloc[-2]

    # 1. Nến H4 đã đóng có Close > EMA200 H4
    h4_o, h4_c, h4_h, h4_l = last_h4['open'], last_h4['close'], last_h4['high'], last_h4['low']
    h4_ema200 = last_h4['ema200']
    
    if h4_c <= h4_ema200:
        return None, f"H4 Close ({h4_c}) nằm dưới EMA200 H4 ({h4_ema200:.4f})"

    # 2. H4 Low chạm/tiến gần EMA200 H4 (Sai lệch 0.5 - 1.0%)
    dist_low_ema200_pct = abs(h4_l - h4_ema200) / h4_ema200 * 100
    if dist_low_ema200_pct > 1.0:
        return None, f"H4 Low chưa tiến sát EMA200 (Khoảng cách {dist_low_ema200_pct:.2f}% > 1.0%)"

    # 3. Cấu trúc râu H4: Râu dưới >= 2x Thân nến, Close nằm trong 25% trên của nến
    h4_range = h4_h - h4_l
    if h4_range == 0: return None, "Nến H4 không có biên độ"

    h4_body = abs(h4_c - h4_o)
    h4_lower_shadow = min(h4_o, h4_c) - h4_l

    shadow_ratio = h4_lower_shadow / h4_body if h4_body > 0 else 999.0
    if shadow_ratio < 2.0:
        return None, f"Râu dưới H4 chưa đủ dài (Tỷ lệ Râu/Thân: {shadow_ratio:.2f}x < 2.0x)"

    is_close_top25 = h4_c >= (h4_l + 0.75 * h4_range)
    if not is_close_top25:
        return None, "Close H4 không nằm trong 25% phía trên của nến"

    # 4. Slope EMA200 H4 không dốc xuống mạnh
    ema200_slope = (h4_ema200 - prev_h4['ema200']) / prev_h4['ema200'] * 100
    if ema200_slope < -0.15:
        return None, f"EMA200 H4 đang dốc xuống mạnh ({ema200_slope:.3f}%)"

    # 5. H1 xác nhận sau H4 rút chân: Nến H1 đóng xanh HOẶC Break đỉnh nến tín hiệu / Engulfing
    h1_confirm = False
    confirm_reason = ""
    
    is_h1_green = last_h1['close'] > last_h1['open']
    is_engulfing = (last_h1['close'] > last_h1['open']) and (prev_h1['close'] < prev_h1['open']) and (last_h1['close'] > prev_h1['open'])
    is_break_wick_high = last_h1['close'] > h4_h

    if is_break_wick_high:
        h1_confirm = True
        confirm_reason = "H1 Breakout vượt đỉnh nến rút râu H4"
    elif is_engulfing:
        h1_confirm = True
        confirm_reason = "H1 xuất hiện Bullish Engulfing"
    elif is_h1_green and (last_h1['close'] > prev_h1['high']):
        h1_confirm = True
        confirm_reason = "H1 đóng nến xanh phá vỡ đỉnh nến H1 trước"

    if not h1_confirm:
        return None, "Chưa có H1 nến xanh / Bullish Engulfing / Breakout xác nhận"

    # 6. Entry, SL và TP cho SETUP 2
    entry = last_h1['close']
    
    # SL đặt dưới đáy râu H4 + buffer 0.3%
    sl = h4_l * 0.997
    risk_pct = ((entry - sl) / entry) * 100

    if risk_pct < 0.8:
        sl = entry * 0.985
        risk_pct = ((entry - sl) / entry) * 100

    historical_highs = df_h1['high'].iloc[-60:-1]
    valid_highs = historical_highs[historical_highs > entry]

    if valid_highs.empty:
        tp1 = entry + (entry - sl) * 1.5
        tp2 = entry + (entry - sl) * 2.5
        rr = 1.5
    else:
        tp1_idx = valid_highs.iloc[::-1].idxmax()
        tp1 = df_h1['high'].loc[tp1_idx]
        higher_highs = historical_highs[historical_highs > tp1]
        tp2 = higher_highs.max() if not higher_highs.empty else tp1 + (tp1 - entry)
        rr = ((tp1 - entry) / entry) * 100 / risk_pct

    if rr < 1.5:
        return None, f"R:R Setup 2 quá thấp ({rr:.2f} < 1.5)"

    return {
        "symbol": symbol,
        "internal_setup_name": "H4_EMA200_WICK_REVERSAL",
        "tag": "🔥 SETUP 2: H4 EMA200 WICK REVERSAL LONG",
        "confirm_reason": confirm_reason,
        "shadow_ratio": round(shadow_ratio, 2),
        "dist_low_ema200": round(dist_low_ema200_pct, 2),
        "candle_ts": int(last_h1['timestamp']),
        "market_entry": round(entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2)
    }, f"ĐẠT TÍN HIỆU SETUP 2 ({confirm_reason})"

# ==============================================================================
# 6. GỬI CẢNH BÁO TELEGRAM
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM ERROR] Chưa cấu hình BOT_TOKEN hoặc CHAT_ID", flush=True)
        return False

    if sig['internal_setup_name'] == "H4_EMA200_WICK_REVERSAL":
        msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Entry: <code>{sig['market_entry']:.4f}</code>
SL: <code>{sig['sl']:.4f}</code>
TP1: <code>{sig['tp1']:.4f}</code>
TP2: <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}

Cấu trúc H4 Wick Reversal:
- H4 Low cách EMA200: {sig['dist_low_ema200']}% ✅
- Tỷ lệ Râu/Thân H4: {sig['shadow_ratio']}x (Close Top 25%) ✅
- H1 Xác nhận: {sig['confirm_reason']} ✅

💡 Lý do: Nến H4 rút râu dưới cực mạnh tại EMA200 H4, H1 xuất hiện nến xanh xác nhận đà đảo chiều tăng, tỷ lệ R:R đạt chuẩn."""
    else:
        msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Mô hình nến: <b>{sig['pattern_name']}</b>
Entry: <code>{sig['market_entry']:.4f}</code>
SL: <code>{sig['sl']:.4f}</code>
TP1: <code>{sig['tp1']:.4f}</code>
TP2: <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}

Cấu trúc kỹ thuật:
- H4 Trend: Close > EMA200 H4 ✅
- H1 Position: Close > EMA21 H1 (Cách {sig['dist_pct']}%) ✅
- Nến H1 xác nhận: {sig['pattern_name']} ✅

💡 Lý do: Giá H4 thuộc xu hướng tăng trên EMA200, H1 hồi về EMA21 xuất hiện nến đảo chiều tăng hợp lệ, R:R đạt tiêu chuẩn."""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        print(f"[TELEGRAM ERROR] Lỗi gửi Telegram: {e}", flush=True)
        return False

# ==============================================================================
# 7. VÒNG LẶP SCANNER TOÀN DIỆN VÀ THỐNG KÊ LOG
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu Bybit Dual-Setup (EMA21 Reversal & H4 EMA200 Wick) Scanner...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            
            # Khởi tạo đếm thống kê riêng cho từng Setup
            s1_data = s1_h4_pass = s1_h1_pass = s1_pattern = s1_rr = s1_sent = 0
            s2_data = s2_h4_pass = s2_wick_pass = s2_h1_pass = s2_rr = s2_sent = 0

            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins (2 Setup độc lập)...", flush=True)

            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']

                df_h1 = get_closed_klines_df(sym, "60", limit=220)
                df_h4 = get_closed_klines_df(sym, "240", limit=220)
                
                if df_h1.empty or df_h4.empty or len(df_h1) < 200 or len(df_h4) < 200:
                    continue
                
                df_h1 = add_indicators(df_h1)
                df_h4 = add_indicators(df_h4)

                # --- ĐÁNH GIÁ SETUP 1 ---
                s1_data += 1
                sig1, reason1 = analyze_setup_1(sym, df_h1, df_h4)
                if sig1:
                    s1_rr += 1
                    if get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym):
                        if not is_signal_already_sent(sym, sig1['internal_setup_name'], sig1['candle_ts']):
                            if send_telegram_alert(sig1):
                                record_sent_signal(sym, sig1['internal_setup_name'], sig1['candle_ts'])
                                save_active_trade(sig1)
                                s1_sent += 1
                                print(f"✅ [SETUP 1 SENT] #{sym} | {sig1['pattern_name']}", flush=True)
                else:
                    print(f"[{sym}] [SETUP 1 REJECT] {reason1}", flush=True)

                # --- ĐÁNH GIÁ SETUP 2 (MỚI) ---
                s2_data += 1
                sig2, reason2 = analyze_setup_2(sym, df_h1, df_h4)
                if sig2:
                    s2_rr += 1
                    if get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym):
                        if not is_signal_already_sent(sym, sig2['internal_setup_name'], sig2['candle_ts']):
                            if send_telegram_alert(sig2):
                                record_sent_signal(sym, sig2['internal_setup_name'], sig2['candle_ts'])
                                save_active_trade(sig2)
                                s2_sent += 1
                                print(f"✅ [SETUP 2 SENT] #{sym} | {sig2['confirm_reason']}", flush=True)
                else:
                    print(f"[{sym}] [SETUP 2 REJECT] {reason2}", flush=True)

            # --- IN BÁO CÁO THỐNG KÊ LOG ---
            print(f"\n📊 --- BÁO CÁO VÒNG QUÉT SCANNER ---", flush=True)
            print(f"• SETUP 1 (H1 EMA21 Reversal): Quét {s1_data} coin | Đạt R:R: {s1_rr} | Gửi Tele: {s1_sent}", flush=True)
            print(f"• SETUP 2 (H4 EMA200 Wick)    : Quét {s2_data} coin | Đạt R:R: {s2_rr} | Gửi Tele: {s2_sent}", flush=True)
            print(f"-----------------------------------------\n", flush=True)

        except Exception as e:
            print(f"[ERROR] Vòng lặp Scanner gặp sự cố: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
