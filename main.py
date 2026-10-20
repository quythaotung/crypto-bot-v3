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
    return "Bybit Multi-Setup Quality LONG Scanner Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BIẾN MÔI TRƯỜNG & ĐIỀU KIỆN
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "2000000"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.25"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "150"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "7200"))

MAX_ALERTS_PER_CYCLE = 2
MAX_ACTIVE_TRADES = 5

DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE (CHỐNG TRÙNG THEO TIMESTAMP NẾN ĐÓNG)
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
                score INTEGER,
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
                setup_name TEXT DEFAULT 'UNKNOWN',
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
    """Kiểm tra chống trùng theo (symbol + setup + timestamp nến xác nhận H1)"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sent_signals WHERE symbol = ? AND setup_name = ? AND candle_timestamp = ?",
            (symbol, setup_name, candle_ts)
        )
        return cursor.fetchone() is not None

def record_sent_signal(symbol: str, setup_name: str, candle_ts: int, score: int):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO sent_signals (symbol, setup_name, candle_timestamp, score, last_sent_at) VALUES (?, ?, ?, ?, ?)",
            (symbol, setup_name, candle_ts, score, now_ts)
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
# 4. LẤY TICKERS & KLINE DATA (CHỈ XÉT NẾN ĐÃ ĐÓNG HOÀN TOÀN)
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
                last_price = safe_float(t.get("lastPrice", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                
                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI", "EUR")):
                    base_coin = sym.replace("USDT", "")
                    if base_coin not in non_crypto_exact and turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                        valid.append({"symbol": sym, "turnover24h": turnover, "lastPrice": last_price})

            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[API ERROR] Lỗi lấy danh sách Tickers Bybit: {e}", flush=True)
    return []

def get_closed_klines_df(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    """Chỉ lấy các nến ĐÃ ĐÓNG HOÀN TOÀN bằng cách loại bỏ nến realtime [0]"""
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 3: return pd.DataFrame()
            
            closed_raw = raw_list[1:] # Bỏ nến realtime đang chạy
            df = pd.DataFrame(closed_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume', 'timestamp']:
                df[col] = df[col].astype(float)
            return df
    except Exception as e:
        print(f"[API ERROR] Lỗi lấy Kline {symbol} ({interval}): {e}", flush=True)
    return pd.DataFrame()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['ema21'] = closes.ewm(span=21, adjust=False).mean()
    df['ema89'] = closes.ewm(span=89, adjust=False).mean()
    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)
    return df

# ==============================================================================
# 5. ENGINE ĐÁNH GIÁ 3 SETUP LONG ĐỘC LẬP
# ==============================================================================
def analyze_setups_for_symbol(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> list:
    signals = []
    if df_h1.empty or len(df_h1) < 40 or df_h4.empty or len(df_h4) < 35: return signals
    if 'ema21' not in df_h1.columns or 'ema21' not in df_h4.columns: return signals

    last_h1 = df_h1.iloc[-1]
    prev_h1 = df_h1.iloc[-2]
    h4_last = df_h4.iloc[-1]
    h4_prev = df_h4.iloc[-2]

    c_open, c_close, c_high, c_low = last_h1['open'], last_h1['close'], last_h1['high'], last_h1['low']
    c_range = c_high - c_low
    c_body = c_close - c_open
    c_ts = int(last_h1['timestamp'])

    ema21_h1_now = last_h1['ema21']
    ema21_h1_prev = prev_h1['ema21']
    ema21_h4_now = h4_last['ema21']
    ema21_h4_prev = h4_prev['ema21']
    vol_ratio = last_h1['vol_ratio']

    # Lọc chung: H4 giảm mạnh (Bearish mạnh) => LOẠI TẤT CẢ SETUP
    is_h4_bearish_manh = (h4_last['close'] < h4_last['ema89']) and (h4_last['close'] < h4_last['ema20'])
    if is_h4_bearish_manh:
        print(f"[{symbol}] [LOẠI] H4 giảm mạnh (Close < EMA20 & EMA89).", flush=True)
        return signals

    # --------------------------------------------------------------------------
    # 🔥 SETUP 3 (NEW): EMA21 TREND PULLBACK LONG
    # --------------------------------------------------------------------------
    setup3_valid = False
    s3_logs = []

    # 1. H1 đóng trên EMA21 & EMA21 H1 dốc lên
    s3_h1_above = c_close > ema21_h1_now
    s3_h1_slope = ema21_h1_now > ema21_h1_prev
    
    # 2. H4 đóng trên EMA21
    s3_h4_above = h4_last['close'] > ema21_h4_now
    s3_h4_slope = ema21_h4_now >= ema21_h4_prev

    # 3. Kiểm tra nhịp hồi về gần/chạm EMA21 trong 1-5 nến trước đó
    recent_pullback_klines = df_h1.iloc[-6:-1]
    touched_ema21 = any(row['low'] <= row['ema21'] * 1.006 for _, row in recent_pullback_klines.iterrows())

    # 4. Nến H1 xác nhận: Nến xanh đóng cửa trên EMA21, thân nến rõ ràng
    is_green_confirm = (c_close > c_open) and (c_body >= 0.45 * c_range) if c_range > 0 else False
    is_vol_ok = vol_ratio >= 1.0

    # 5. Giá chưa tăng quá xa EMA21 H1 (khoảng cách <= 3.5%)
    dist_ema21_pct = ((c_close - ema21_h1_now) / ema21_h1_now) * 100
    is_not_far = dist_ema21_pct <= 3.5

    if s3_h1_above and s3_h1_slope and s3_h4_above and touched_ema21 and is_green_confirm and is_vol_ok and is_not_far:
        setup3_valid = True
    else:
        if not s3_h1_above or not s3_h1_slope: s3_logs.append("EMA21 H1 chưa dốc lên hoặc Close < EMA21")
        if not s3_h4_above: s3_logs.append("H4 Close < EMA21")
        if not touched_ema21: s3_logs.append("Chưa có nhịp hồi gần/chạm EMA21")
        if not is_green_confirm: s3_logs.append("Nến xác nhận chưa rõ ràng/thân nhỏ")
        if not is_vol_ok: s3_logs.append(f"Volume yếu ({vol_ratio:.2f}x MA20)")
        if not is_not_far: s3_logs.append(f"Giá đã tăng quá xa EMA21 ({dist_ema21_pct:.2f}%)")

    if setup3_valid:
        # Tính toán Entry, SL, TP kỹ thuật
        entry = c_close
        pullback_low = min(df_h1['low'].iloc[-6:-1].min(), c_low)
        sl = pullback_low * 0.995 # Buffer 0.5%
        
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

        if rr >= 1.5:
            score = 80
            if vol_ratio >= 1.3: score += 5
            if s3_h4_slope: score += 5
            if c_body >= 0.6 * c_range: score += 5
            if rr >= 2.0: score += 5
            score = min(score, 100)

            if score >= 85:
                signals.append({
                    "symbol": symbol,
                    "internal_setup_name": "EMA21 TREND PULLBACK LONG",
                    "tag": "📈 EMA21 PULLBACK LONG",
                    "candle_ts": c_ts,
                    "market_entry": round(entry, 4),
                    "sl": round(sl, 4),
                    "tp1": round(tp1, 4),
                    "tp2": round(tp2, 4),
                    "rr": round(rr, 2),
                    "vol_ratio": round(vol_ratio, 2),
                    "score": score,
                    "h4_trend": "Bullish (EMA21 ↑)" if s3_h4_slope else "Neutral"
                })
                print(f"[{symbol}] [ĐẠT SETUP 3] Score: {score} | Entry: {entry} | RR: {rr:.2f}", flush=True)
            else:
                print(f"[{symbol}] [LOẠI SETUP 3] Score không đủ 85 ({score}/100)", flush=True)
        else:
            print(f"[{symbol}] [LOẠI SETUP 3] Kháng cự quá gần / RR thấp ({rr:.2f} < 1.5)", flush=True)
    else:
        print(f"[{symbol}] [LOẠI SETUP 3] {', '.join(s3_logs)}", flush=True)

    # --------------------------------------------------------------------------
    # ⚡️ SETUP 1: H1 BREAKOUT MOMENTUM (CẮT MA20/EMA20)
    # --------------------------------------------------------------------------
    is_prev_below = prev_h1['close'] < prev_h1['ema20']
    is_curr_above = (c_close > last_h1['ema20']) and (c_close > c_open)
    is_s1_mom = (c_body >= 0.4 * c_range) and (vol_ratio >= 1.0) if c_range > 0 else False

    if is_prev_below and is_curr_above and is_s1_mom:
        entry = c_close
        recent_low = df_h1['low'].iloc[-12:-1].min()
        sl = min(recent_low, c_low) * 0.995
        risk_pct = ((entry - sl) / entry) * 100
        if risk_pct < 0.8: sl = entry * 0.985; risk_pct = 1.5

        historical_highs = df_h1['high'].iloc[-60:-1]
        valid_highs = historical_highs[historical_highs > entry]
        if valid_highs.empty:
            tp1 = entry + (entry - sl) * 1.5
            tp2 = entry + (entry - sl) * 2.5
            rr = 1.5
        else:
            tp1 = df_h1['high'].loc[valid_highs.iloc[::-1].idxmax()]
            tp2 = tp1 + (tp1 - entry)
            rr = ((tp1 - entry) / entry) * 100 / risk_pct

        if rr >= 1.5:
            score = 85
            if vol_ratio >= 1.3: score += 5
            if h4_last['close'] > h4_last['ema89']: score += 5
            score = min(score, 100)

            if score >= 85:
                signals.append({
                    "symbol": symbol,
                    "internal_setup_name": "H1 BREAKOUT MOMENTUM",
                    "tag": "🔥 H1 BREAKOUT LONG",
                    "candle_ts": c_ts,
                    "market_entry": round(entry, 4),
                    "sl": round(sl, 4),
                    "tp1": round(tp1, 4),
                    "tp2": round(tp2, 4),
                    "rr": round(rr, 2),
                    "vol_ratio": round(vol_ratio, 2),
                    "score": score,
                    "h4_trend": "Bullish" if h4_last['close'] > h4_last['ema89'] else "Neutral"
                })

    # --------------------------------------------------------------------------
    # 🟢 SETUP 2: H1 PULLBACK MA20
    # --------------------------------------------------------------------------
    is_h1_uptrend = (c_close > last_h1['ema89']) and (last_h1['ema20'] > prev_h1['ema20'])
    lower_shadow = min(c_open, c_close) - c_low
    is_touched_ema20 = (c_low <= last_h1['ema20'] * 1.008) and (c_close >= last_h1['ema20'])
    is_rejection_tail = (lower_shadow >= 0.3 * c_range) and (c_close > c_open) if c_range > 0 else False

    if is_h1_uptrend and is_touched_ema20 and is_rejection_tail and (vol_ratio >= 0.9):
        entry = c_close
        recent_low = df_h1['low'].iloc[-12:-1].min()
        sl = min(recent_low, c_low) * 0.995
        risk_pct = ((entry - sl) / entry) * 100
        if risk_pct < 0.8: sl = entry * 0.985; risk_pct = 1.5

        historical_highs = df_h1['high'].iloc[-60:-1]
        valid_highs = historical_highs[historical_highs > entry]
        if valid_highs.empty:
            tp1 = entry + (entry - sl) * 1.5
            tp2 = entry + (entry - sl) * 2.5
            rr = 1.5
        else:
            tp1 = df_h1['high'].loc[valid_highs.iloc[::-1].idxmax()]
            tp2 = tp1 + (tp1 - entry)
            rr = ((tp1 - entry) / entry) * 100 / risk_pct

        if rr >= 1.5:
            score = 85
            if vol_ratio >= 1.2: score += 5
            score = min(score, 100)
            if score >= 85:
                signals.append({
                    "symbol": symbol,
                    "internal_setup_name": "H1 PULLBACK MA20",
                    "tag": "🟢 H1 PULLBACK LONG",
                    "candle_ts": c_ts,
                    "market_entry": round(entry, 4),
                    "sl": round(sl, 4),
                    "tp1": round(tp1, 4),
                    "tp2": round(tp2, 4),
                    "rr": round(rr, 2),
                    "vol_ratio": round(vol_ratio, 2),
                    "score": score,
                    "h4_trend": "Bullish" if h4_last['close'] > h4_last['ema89'] else "Neutral"
                })

    return signals

# ==============================================================================
# 6. GỬI TELEGRAM & KIỂM TRA LỖI KẾT NỐI
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM ERROR] Chưa cấu hình BOT_TOKEN hoặc CHAT_ID", flush=True)
        return False

    msg = f"""{sig['tag']}

Symbol: #{sig['symbol']}
Setup: {sig['internal_setup_name']}
Entry: <code>{sig['market_entry']:.4f}</code>
SL: <code>{sig['sl']:.4f}</code>
TP1: <code>{sig['tp1']:.4f}</code>
TP2: <code>{sig['tp2']:.4f}</code>
RR: 1:{sig['rr']}
Score: <b>{sig['score']}/100</b>

EMA21/MA20 Confirm: ✅ (Nến H1 đã đóng)
Volume: {sig['vol_ratio']}x MA20
H4 Trend: {sig['h4_trend']}

Lý do kích hoạt:
"Xác nhận tín hiệu {sig['internal_setup_name']} trên nến H1 đã đóng hoàn toàn, xu hướng H4 ủng hộ, tỷ lệ RR đạt chuẩn." """

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        if res.status_code == 200:
            return True
        else:
            print(f"[TELEGRAM ERROR] Lỗi gửi Telegram API ({res.status_code}): {res.text}", flush=True)
            return False
    except Exception as e:
        print(f"[TELEGRAM ERROR] Không thể kết nối tới Telegram: {e}", flush=True)
        return False

# ==============================================================================
# 7. VÒNG LẶP SCANNER BẤT TẬN
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu Bybit Multi-Setup Quality Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins (Xét 3 Setup độc lập)...", flush=True)

            scanned_signals = []
            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']

                df_h1 = get_closed_klines_df(sym, "60", limit=60)
                df_h4 = get_closed_klines_df(sym, "240", limit=60)
                
                if not df_h1.empty and not df_h4.empty:
                    df_h1 = add_indicators(df_h1)
                    df_h4 = add_indicators(df_h4)
                    
                    sigs = analyze_setups_for_symbol(sym, df_h1, df_h4)
                    for s in sigs:
                        scanned_signals.append(s)

            # Lọc ưu tiên Score cao nhất
            scanned_signals.sort(key=lambda x: x['score'], reverse=True)
            top_signals = scanned_signals[:MAX_ALERTS_PER_CYCLE]

            sent_count = 0
            if top_signals:
                for sig in top_signals:
                    if get_active_trades_count() >= MAX_ACTIVE_TRADES and not is_symbol_currently_active(sig['symbol']):
                        print(f"[{sig['symbol']}] Bỏ qua do vượt quá giới hạn active trades ({MAX_ACTIVE_TRADES})", flush=True)
                        continue

                    # Kiểm tra chống trùng theo Symbol + Setup + Timestamp nến H1
                    if not is_signal_already_sent(sig['symbol'], sig['internal_setup_name'], sig['candle_ts']):
                        if send_telegram_alert(sig):
                            record_sent_signal(sig['symbol'], sig['internal_setup_name'], sig['candle_ts'], sig['score'])
                            save_active_trade(sig)
                            sent_count += 1
                            print(f"✅ BÁO THÀNH CÔNG: [{sig['tag']}] #{sig['symbol']} | Score: {sig['score']}", flush=True)
                    else:
                        print(f"[{sig['symbol']}] [{sig['internal_setup_name']}] Tín hiệu đã gửi trước đó trên nến H1 này. Bỏ qua.", flush=True)

        except Exception as e:
            print(f"[ERROR] Vòng lặp Scanner gặp sự cố: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
