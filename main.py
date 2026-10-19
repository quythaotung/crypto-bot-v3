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
    return "Bybit H1 Dual-Setup Quality Scanner Active 24/7!"

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
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300")) # Quét 5 phút/lần
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "7200"))

MAX_ALERTS_PER_CYCLE = 2
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
                status TEXT NOT NULL,
                score INTEGER,
                call_count INTEGER DEFAULT 1,
                last_sent_at INTEGER NOT NULL,
                UNIQUE(symbol, status)
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
                tp3 REAL NOT NULL,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                status TEXT DEFAULT 'OPEN',
                setup_name TEXT DEFAULT 'UNKNOWN',
                created_at INTEGER NOT NULL,
                updated_at INTEGER DEFAULT 0
            )
        """)
        
        cursor.execute("PRAGMA table_info(active_trades)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'setup_name' not in columns:
            cursor.execute("ALTER TABLE active_trades ADD COLUMN setup_name TEXT DEFAULT 'UNKNOWN'")
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

def process_signal_dedup(symbol: str, status: str, score: int) -> tuple:
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT call_count, last_sent_at FROM sent_signals WHERE symbol = ? AND status = ?",
            (symbol, status)
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                "INSERT INTO sent_signals (symbol, status, score, call_count, last_sent_at) VALUES (?, ?, ?, 1, ?)",
                (symbol, status, score, now_ts)
            )
            conn.commit()
            return True, 1
        else:
            prev_call_count, last_sent_at = row
            time_passed = now_ts - last_sent_at

            if prev_call_count >= 2 or time_passed < COOLDOWN_SECONDS:
                return False, prev_call_count

            new_call_count = prev_call_count + 1
            cursor.execute(
                "UPDATE sent_signals SET call_count = ?, last_sent_at = ?, score = ? WHERE symbol = ? AND status = ?",
                (new_call_count, now_ts, score, symbol, status)
            )
            conn.commit()
            return True, new_call_count

def save_active_trade(sig: dict):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO active_trades (symbol, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, status, setup_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'OPEN', ?, ?, ?)
        """, (sig['symbol'], sig['market_entry'], sig['sl'], sig['tp1'], sig['tp2'], sig['tp2'], sig['internal_setup_name'], now_ts, now_ts))
        conn.commit()

# ==============================================================================
# 4. LẤY TICKERS & KLINE DATA (CHỈ XÉT NẾN H1 ĐÃ ĐÓNG HOÀN TOÀN)
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_bybit_crypto_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
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
    except Exception:
        pass
    return []

def get_closed_klines_df(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 3: return pd.DataFrame()
            
            closed_raw = raw_list[1:] # Bỏ qua nến realtime đang chạy
            df = pd.DataFrame(closed_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            return df
    except Exception:
        pass
    return pd.DataFrame()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['ema89'] = closes.ewm(span=89, adjust=False).mean()
    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)
    return df

# ==============================================================================
# 5. ENGINE QUÉT 2 SETUP H1 CHẤT LƯỢNG CAO
# ==============================================================================
def analyze_h1_dual_setups(df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> dict:
    if df_h1.empty or len(df_h1) < 40 or df_h4.empty or len(df_h4) < 35: return None
    if 'ema89' not in df_h1.columns or 'ema89' not in df_h4.columns: return None

    last_candle = df_h1.iloc[-1]
    prev_candle = df_h1.iloc[-2]
    
    open_p = last_candle['open']
    close_p = last_candle['close']
    high_p = last_candle['high']
    low_p = last_candle['low']
    
    candle_range = high_p - low_p
    body = close_p - open_p

    ema20_now = last_candle['ema20']
    ema20_prev = prev_candle['ema20']
    vol_ratio = last_candle['vol_ratio']

    # Kiểm tra H4 không bearish mạnh
    h4_last = df_h4.iloc[-1]
    is_h4_bearish_manh = (h4_last['close'] < h4_last['ema89']) and (h4_last['close'] < h4_last['ema20'])
    if is_h4_bearish_manh: return None

    setup_type = None

    # --- SETUP 1: BREAKOUT MOMENTUM (Giá dưới MA20 -> Đóng nến vượt MA20 mạnh mẽ) ---
    prev_under_klines = df_h1.iloc[-8:-1]
    under_ma20_count = sum(1 for _, row in prev_under_klines.iterrows() if row['close'] < row['ema20'])
    
    is_s1_breakout = (under_ma20_count >= 3) and (close_p > ema20_now) and (ema20_now > ema20_prev)
    is_s1_momentum = (body >= 0.6 * candle_range) and ((high_p - close_p) <= 0.2 * candle_range) and (vol_ratio > 1.0)
    
    if is_s1_breakout and is_s1_momentum:
        setup_type = "SETUP_1_BREAKOUT"

    # --- SETUP 2: PULLBACK MA20 (Sóng tăng hồi về MA20 + Rút râu giữ trên MA20) ---
    # Kiểm tra xu hướng H1 trước đó đang tăng (giá nằm trên EMA89 và MA20 dốc lên)
    is_h1_uptrend = (close_p > last_candle['ema89']) and (ema20_now > ema20_prev)
    
    # Kiểm tra nến hiện tại nhúng râu chạm/gần sát MA20 nhưng đóng cửa nằm trên MA20 và rút râu dưới rõ ràng
    lower_shadow = min(open_p, close_p) - low_p
    is_touched_ma20 = (low_p <= ema20_now * 1.008) and (close_p >= ema20_now)
    is_rejection_tail = (lower_shadow >= 0.3 * candle_range) and (close_p > open_p) # Nến xanh rút râu
    is_s2_vol = vol_ratio >= 0.9

    if not setup_type and is_h1_uptrend and is_touched_ma20 and is_rejection_tail and is_s2_vol:
        setup_type = "SETUP_2_PULLBACK"

    if not setup_type:
        return None

    # ================= QUẢN TRỊ SL & TP THEO KỸ THUẬT THANH KHOẢN =================
    market_entry = close_p
    
    # Tìm Swing Low gần nhất để đặt SL
    recent_swing_low = df_h1['low'].iloc[-12:-1].min()
    sl = min(recent_swing_low, low_p) * 0.995 # Buffer 0.5% chống quét râu
    
    risk_pct = ((market_entry - sl) / market_entry) * 100
    if risk_pct < 1.0:
        sl = market_entry * 0.99
        risk_pct = 1.0

    # Tìm kháng cự liquidity phía trên làm TP1
    historical_highs = df_h1['high'].iloc[-60:-2]
    valid_highs = historical_highs[historical_highs > market_entry]

    if valid_highs.empty:
        tp1 = market_entry + (market_entry - sl) * 1.5
        tp2 = market_entry + (market_entry - sl) * 2.5
        dist_tp1 = ((tp1 - market_entry) / market_entry) * 100
        rr = 1.5
    else:
        tp1_struct = valid_highs.iloc[::-1].idxmax()
        tp1 = df_h1['high'].loc[tp1_struct]
        
        higher_highs = historical_highs[historical_highs > tp1]
        tp2 = higher_highs.max() if not higher_highs.empty else tp1 + (tp1 - market_entry)
        
        dist_tp1 = ((tp1 - market_entry) / market_entry) * 100
        rr = dist_tp1 / risk_pct
        
        # Nếu RR quá thấp (< 1.5) thì loại bỏ
        if rr < 1.5: return None

    # Chấm điểm Score (>= 85 mới báo)
    score = 85
    if setup_type == "SETUP_1_BREAKOUT": score += 5
    if vol_ratio >= 1.5: score += 5
    if h4_last['close'] > h4_last['ema89']: score += 5
    if rr >= 2.0: score += 3
    
    score = min(score, 100)
    if score < 85: return None

    tag = "🔥 STRONG LONG (BREAKOUT)" if setup_type == "SETUP_1_BREAKOUT" else "🟢 LONG (MA20 PULLBACK)"
    setup_name = "H1 BREAKOUT MOMENTUM" if setup_type == "SETUP_1_BREAKOUT" else "H1 PULLBACK MA20"

    return {
        "timeframe": "H1",
        "tag": tag,
        "status": "LONG",
        "internal_setup_name": setup_name,
        "score": int(score),
        "market_entry": round(market_entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2),
        "dist_tp1": round(dist_tp1, 2),
        "vol_ratio": round(vol_ratio, 2),
        "timestamp": last_candle['timestamp']
    }

# ==============================================================================
# 6. GỬI TELEGRAM ĐỊNH DẠNG CHUẨN
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    msg = f"""{sig['tag']} — #{sig['symbol']}

<b>Setup:</b> {sig['internal_setup_name']}
<b>Entry:</b> <code>{sig['market_entry']:.4f}</code>
<b>SL:</b> <code>{sig['sl']:.4f}</code>
<b>TP1:</b> <code>{sig['tp1']:.4f}</code>
<b>TP2:</b> <code>{sig['tp2']:.4f}</code>
<b>RR:</b> {sig['rr']}

MA20: Xác nhận nến đóng ✅
Volume: {sig['vol_ratio']}x
Liquidity: Cách đỉnh {sig['dist_tp1']}%
Score: <b>{sig['score']}/100</b>

💡 <b>Lý do kích hoạt:</b>
Thỏa mãn điều kiện dòng tiền bùng nổ hoặc hồi phục giữ vững MA20 với thanh khoản xác nhận, cấu trúc thanh khoản phía trên còn rộng để tiếp diễn đà tăng."""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception:
        return False

# ==============================================================================
# 7. VÒNG LẶP SCANNER
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu H1 Dual-Setup Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins (Xét 2 Setup H1)...", flush=True)

            scanned_signals = []
            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']

                df_h1 = get_closed_klines_df(sym, "60", limit=60)
                df_h4 = get_closed_klines_df(sym, "240", limit=60)
                
                if not df_h1.empty and not df_h4.empty:
                    df_h1 = add_indicators(df_h1)
                    df_h4 = add_indicators(df_h4)
                    
                    sig = analyze_h1_dual_setups(df_h1, df_h4)
                    if sig:
                        sig['symbol'] = sym
                        scanned_signals.append(sig)

            scanned_signals.sort(key=lambda x: x['score'], reverse=True)
            top_signals = scanned_signals[:MAX_ALERTS_PER_CYCLE]

            sent_count = 0
            if top_signals:
                for sig in top_signals:
                    if get_active_trades_count() >= MAX_ACTIVE_TRADES and not is_symbol_currently_active(sig['symbol']):
                        break

                    should_send, call_count = process_signal_dedup(sig['symbol'], sig['status'], sig['score'])
                    if should_send:
                        sig['call_count'] = call_count
                        if send_telegram_alert(sig):
                            save_active_trade(sig)
                            sent_count += 1
                            print(f"✅ [{sig['tag']}] #{sig['symbol']} | Setup: {sig['internal_setup_name']} | Entry: {sig['market_entry']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
