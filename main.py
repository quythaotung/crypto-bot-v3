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
    return "Bybit Proprietary Signal Engine (Setup 4 Removed) Active 24/7!"

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

MAX_DROP_24H_PCT = -12.0
MIN_QUALITY_SCORE = 80  # Chỉ báo tín hiệu từ 80/100 điểm trở lên

MAX_ACTIVE_TRADES = 5
DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE
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
# 4. LẤY TICKERS BYBIT & KLINE DATA
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

def check_extended_bullish_reversal_patterns(df_h1: pd.DataFrame) -> tuple:
    if len(df_h1) < 4: return False, 0

    c1, c2, c3 = df_h1.iloc[-1], df_h1.iloc[-2], df_h1.iloc[-3]
    c1_o, c1_c, c1_h, c1_l = c1['open'], c1['close'], c1['high'], c1['low']
    c2_o, c2_c, c2_h, c2_l = c2['open'], c2['close'], c2['high'], c2['low']
    c3_o, c3_c, c3_h, c3_l = c3['open'], c3['close'], c3['high'], c3['low']

    c1_range = c1_h - c1_l
    c1_body = abs(c1_c - c1_o)
    c1_lower_shadow = min(c1_o, c1_c) - c1_l
    c1_upper_shadow = c1_h - max(c1_o, c1_c)
    c2_body = abs(c2_c - c2_o)

    if c1_range == 0 or c1_body <= 0.03 * c1_range: return False, 0
    is_c2_bear = c2_c < c2_o
    is_c1_bull = c1_c > c1_o

    if is_c2_bear and is_c1_bull and (c1_c > c2_o) and (c1_o < c2_c) and (c1_body >= 0.5 * c1_range): return True, 25
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_body > 0.05 * c1_range) and (c1_c >= (c1_l + 0.5 * c1_range)): return True, 22
    if (c1_lower_shadow >= 2.0 * c1_body) and (c1_upper_shadow <= 0.25 * c1_range) and (c1_c >= (c1_h - 0.3 * c1_range)): return True, 24
    if (c3_c < c3_o) and (abs(c2_c - c2_o) <= 0.3 * (c3_h - c3_l)) and is_c1_bull and (c1_c >= (c3_o + c3_c) / 2): return True, 20
    if is_c2_bear and is_c1_bull and (c1_o <= c2_c * 1.002) and (c1_c > (c2_o + c2_c)/2) and (c1_c < c2_o): return True, 18
    atr_14 = c1['atr14'] if 'atr14' in c1 else (c1_range + (c2_h - c2_l)) / 2.0
    if is_c2_bear and is_c1_bull and (abs(c1_l - c2_l) <= 0.12 * atr_14) and (c1_c > c1_o) and (c2_body <= 1.8 * atr_14) and (c1_lower_shadow >= 0.3 * c1_range): return True, 20
    if (c3_c > c3_o) and (c2_c > c2_o) and is_c1_bull and (c1_c > c2_c > c3_c) and (c1_body >= 0.4 * c1_range): return True, 23

    return False, 0

# ==============================================================================
# 5. CÁC THUẬT TOÁN ĐÁNH GIÁ (CHỈ CÒN ALGORITHMS 1, 2, 3, 5)
# ==============================================================================
def evaluate_algorithm_1(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200: return None
    last_h1, last_h4 = df_h1.iloc[-1], df_h4.iloc[-1]

    if last_h4['close'] <= last_h4['ema200'] or last_h1['close'] <= last_h1['ema21']: return None
    dist_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    if dist_pct < 1.0 or dist_pct > 3.0: return None

    if not any(row['low'] <= row['ema21'] * 1.008 for _, row in df_h1.iloc[-6:-1].iterrows()): return None

    has_pattern, pattern_weight = check_extended_bullish_reversal_patterns(df_h1)
    if not has_pattern: return None

    entry = last_h1['close']
    sl = min(df_h1['low'].iloc[-6:-1].min(), last_h1['low']) * 0.997
    risk_pct = ((entry - sl) / entry) * 100
    if risk_pct < 0.8: sl = entry * 0.985; risk_pct = 1.5

    tp1 = entry + (entry - sl) * 1.5; tp2 = entry + (entry - sl) * 2.5
    rr = 1.5

    score = min(60 + pattern_weight + int(min(rr * 5, 15)), 98)
    return {"symbol": symbol, "internal_setup_name": "S1_ENGINE", "candle_ts": int(last_h1['timestamp']), "market_entry": round(entry, 4), "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": round(rr, 2), "score": score}

def evaluate_algorithm_2(symbol: str, df_h4: pd.DataFrame) -> tuple:
    if df_h4.empty or len(df_h4) < 200: return None
    last_h4 = df_h4.iloc[-1]
    h4_o, h4_c, h4_h, h4_l = last_h4['open'], last_h4['close'], last_h4['high'], last_h4['low']
    h4_ema200 = last_h4['ema200']

    if h4_c <= h4_ema200: return None
    dist_low_pct = abs(h4_l - h4_ema200) / h4_ema200 * 100
    if dist_low_pct < 0.2 or dist_low_pct > 3.0: return None

    h4_range = h4_h - h4_l
    if h4_range == 0: return None
    h4_body = abs(h4_c - h4_o)
    shadow_ratio = (min(h4_o, h4_c) - h4_l) / h4_body if h4_body > 0 else 999.0
    if shadow_ratio < 2.0: return None
    close_pos = ((h4_c - h4_l) / h4_range) * 100
    if close_pos < 75.0: return None

    entry = h4_c; sl = h4_l * 0.997
    tp1 = entry + (entry - sl) * 1.5; tp2 = entry + (entry - sl) * 2.5

    score = min(70 + int(min(shadow_ratio * 4, 18)) + int((close_pos - 75) * 0.4), 99)
    return {"symbol": symbol, "internal_setup_name": "S2_ENGINE", "candle_ts": int(last_h4['timestamp']), "market_entry": round(entry, 4), "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": 1.5, "score": score}

def evaluate_algorithm_3(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200: return None
    last_h1, prev_h1, last_h4 = df_h1.iloc[-1], df_h1.iloc[-2], df_h4.iloc[-1]
    if last_h4['close'] <= last_h4['ema200'] or last_h1['close'] <= last_h1['ema21'] or last_h1['ema21'] <= prev_h1['ema21']: return None

    if not any(((row['low'] - row['ema21']) / row['ema21']) * 100 <= 0.5 for _, row in df_h1.iloc[-24:-1].iterrows()): return None

    has_pattern, pattern_weight = check_extended_bullish_reversal_patterns(df_h1)
    if not has_pattern: return None

    dist_close_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    if dist_close_pct > 1.5 or (last_h1['atr14'] > 0 and (last_h1['close'] - last_h1['ema21']) > last_h1['atr14']): return None

    entry = last_h1['close']
    sl = min(df_h1['low'].iloc[-12:-1].min(), last_h1['low']) * 0.997
    tp1 = entry + (entry - sl) * 1.5; tp2 = entry + (entry - sl) * 2.5

    score = min(65 + pattern_weight + int((1.5 - dist_close_pct) * 8), 96)
    return {"symbol": symbol, "internal_setup_name": "S3_ENGINE", "candle_ts": int(last_h1['timestamp']), "market_entry": round(entry, 4), "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": 1.5, "score": score}

def evaluate_algorithm_5(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 50 or df_h4.empty or len(df_h4) < 50: return None

    c1 = df_h1.iloc[-1]
    h4_ema200 = df_h4.iloc[-1]['ema200']

    # BẮT BUỘC: GIÁ ĐÓNG CỬA PHẢI NẰM HOÀN TOÀN TRÊN HOẶC SÁT ĐƯỜNG H4 EMA200 (TỪ 0.0% ĐẾN +2.5%)
    dist_to_h4_ema200 = ((c1['close'] - h4_ema200) / h4_ema200) * 100
    if dist_to_h4_ema200 < 0.0 or dist_to_h4_ema200 > 2.5: return None

    c1_range = c1['high'] - c1['low']
    if c1_range == 0: return None
    c1_lower_shadow = min(c1['open'], c1['close']) - c1['low']
    shadow_pct = c1_lower_shadow / c1_range

    recent_low = df_h1['low'].iloc[-12:-1].min()
    is_sweep_low = c1['low'] <= recent_low * 1.002

    if shadow_pct < 0.30 and not is_sweep_low: return None

    avg_vol_20 = df_h1['volume'].iloc[-21:-1].mean()
    vol_ratio = c1['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    if vol_ratio > 4.5: return None

    entry = c1['close']
    sl = min(c1['low'], recent_low) * 0.995
    tp1 = entry + (entry - sl) * 2.0
    tp2 = entry + (entry - sl) * 3.5

    score = min(75 + int(shadow_pct * 25) + int(min(vol_ratio * 4, 10)), 99)
    return {"symbol": symbol, "internal_setup_name": "S5_ENGINE", "candle_ts": int(c1['timestamp']), "market_entry": round(entry, 4), "sl": round(sl, 4), "tp1": round(tp1, 4), "tp2": round(tp2, 4), "rr": 2.0, "score": score}

# ==============================================================================
# 6. GỬI CẢNH BÁO TELEGRAM (ẨN NỘI DUNG PHƯƠNG PHÁP)
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    msg = f"""🚀 <b>[HIGH QUALITY LONG SIGNAL] #{sig['symbol']}</b>
-------------------------------------
⭐️ <b>ĐIỂM UY TÍN KỸ THUẬT: {sig['score']}/100</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>

📍 <b>Entry:</b> <code>{sig['market_entry']:.4f}</code>
🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code>
🎯 <b>Take Profit 1:</b> <code>{sig['tp1']:.4f}</code>
🚀 <b>Take Profit 2:</b> <code>{sig['tp2']:.4f}</code>
📐 <b>Tỷ lệ R:R Tham khảo:</b> 1:{sig['rr']}
-------------------------------------
👉 <b>CHECK CHART & QUẢN TRỊ RỦI RO ĐẦY ĐỦ!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        print(f"[TELEGRAM ERROR]: {e}", flush=True)
        return False

# ==============================================================================
# 7. VÒNG LẶP SCANNER QUÉT BẤT TẬN (ĐÁNH GIÁ 4 ALGORITHMS)
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu Bybit Scanner (Setup 4 Removed - Strict EMA - Score >= 80)...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            sent_count = 0

            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                df_h1 = get_closed_klines_df(sym, "60", limit=220)
                df_h4 = get_closed_klines_df(sym, "240", limit=220)
                
                if df_h1.empty or df_h4.empty or len(df_h1) < 200 or len(df_h4) < 200: continue
                df_h1 = add_indicators(df_h1)
                df_h4 = add_indicators(df_h4)

                candidates = []
                # Đánh giá 4 thuật toán còn lại (1, 2, 3 và 5)
                for eval_fn in [
                    lambda: evaluate_algorithm_1(sym, df_h1, df_h4),
                    lambda: evaluate_algorithm_2(sym, df_h4),
                    lambda: evaluate_algorithm_3(sym, df_h1, df_h4),
                    lambda: evaluate_algorithm_5(sym, df_h1, df_h4)
                ]:
                    res = eval_fn()
                    if res and res['score'] >= MIN_QUALITY_SCORE:
                        candidates.append(res)

                if candidates:
                    best_sig = max(candidates, key=lambda x: x['score'])
                    if get_active_trades_count() < MAX_ACTIVE_TRADES or is_symbol_currently_active(sym):
                        if not is_signal_already_sent(sym, best_sig['internal_setup_name'], best_sig['candle_ts']):
                            if send_telegram_alert(best_sig):
                                record_sent_signal(sym, best_sig['internal_setup_name'], best_sig['candle_ts'])
                                save_active_trade(best_sig)
                                sent_count += 1
                                print(f"✅ [SIGNAL SENT] #{sym} | Score: {best_sig['score']}/100", flush=True)

            print(f"\n📊 --- BÁO CÁO VÒNG QUÉT (4 ALGORITHMS) ---", flush=True)
            print(f"• Tín hiệu gửi thành công: {sent_count}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
