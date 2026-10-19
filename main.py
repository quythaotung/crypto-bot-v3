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
    return "Bybit H1 High-Quality LONG Engine Active 24/7!"

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
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300")) # 5 phút/lần
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "7200")) # Cooldown 2h cho 1 coin

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
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            
            non_crypto_exact = [
                'SOXS', 'SOXL', 'TQQQ', 'SQQQ', 'UVXY', 'SPY', 'QQQ', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                'US30', 'US100', 'US500', 'UK100', 'GER30', 'JPN225', 'SPCX', 'SXXP',
                'XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD', 'PLATINUM', 'PALLADIUM',
                'JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BLK', 'AXP', 'V', 'MA', 'PYPL', 'SCHW',
                'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', 'AMD', 'INTC', 'BABA', 'COIN',
                'PLTR', 'MSTR', 'CRWD', 'HOOD', 'DIS', 'NKE', 'PFE', 'JNJ', 'PG', 'XOM', 'CVX',
                '3L', '3S', 'BEAR', 'BULL', 'HALF', 'HEDGE'
            ]
            
            valid = []
            for t in tickers:
                sym = t.get("symbol", "")
                turnover = safe_float(t.get("turnover24h", 0))
                last_price = safe_float(t.get("lastPrice", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                
                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI", "EUR", "GBP")):
                    base_coin = sym.replace("USDT", "")
                    is_non_crypto = (
                        base_coin in non_crypto_exact or 
                        any(k in sym for k in non_crypto_exact) or
                        base_coin.startswith(("SOX", "TQQ", "SQQ", "USD", "EUR"))
                    )

                    if not is_non_crypto and turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                        valid.append({
                            "symbol": sym,
                            "turnover24h": turnover,
                            "lastPrice": last_price,
                        })

            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Tickers: {e}", flush=True)
    return []

def get_closed_klines_df(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 2: return pd.DataFrame()
            closed_raw = raw_list[1:]
            df = pd.DataFrame(closed_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            return df
    except Exception:
        pass
    return pd.DataFrame()

def get_open_interest_change(symbol: str, interval: str = "60") -> float:
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval}&limit=3"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = safe_float(oi_list[0].get("openInterest", 0))
                prev_oi = safe_float(oi_list[1].get("openInterest", 0))
                if prev_oi > 0:
                    return round(((curr_oi - prev_oi) / prev_oi) * 100, 2)
    except Exception:
        pass
    return 0.0

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 30: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['ema89'] = closes.ewm(span=89, adjust=False).mean()
    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)
    return df

# ==============================================================================
# 5. ENGINE PHÁT HIỆN SETUP H1 HIGH-QUALITY
# ==============================================================================
def analyze_h1_strict_long(df_h1: pd.DataFrame, df_h4: pd.DataFrame, realtime_price: float, oi_change: float) -> dict:
    if df_h1.empty or len(df_h1) < 40 or df_h4.empty: return None

    last_candle = df_h1.iloc[-1]
    prev_candle = df_h1.iloc[-2]
    
    close_now = realtime_price if realtime_price > 0 else last_candle['close']
    open_now = last_candle['open']
    high_now = max(last_candle['high'], close_now)
    low_now = min(last_candle['low'], close_now)
    
    ema20_now = last_candle['ema20']
    ema20_prev = prev_candle['ema20']
    ema89_now = last_candle['ema89']

    # 1. Điều kiện 1: Trước đó nằm dưới MA20
    prev_under_klines = df_h1.iloc[-7:-1]
    under_ma20_count = sum(1 for _, row in prev_under_klines.iterrows() if row['close'] < row['ema20'])
    if under_ma20_count < 4: return None

    # 2. Điều kiện 2: Breakout rõ ràng & MA20 hướng lên
    if close_now <= ema20_now or close_now <= open_now: return None
    if ema20_now <= ema20_prev: return None # MA20 phải đang hướng lên hoặc bẻ ngang lên

    # 3. Điều kiện 3: Đủ lực tăng (Body & Volume & Rejection)
    body_pct = ((close_now - open_now) / open_now) * 100 if open_now > 0 else 0.0
    upper_shadow_pct = ((high_now - close_now) / close_now) * 100
    
    if body_pct < 1.0: return None # Thân nến phải tăng mạnh (>= 1.0%)
    if upper_shadow_pct > body_pct * 0.7: return None # Bị từ chối (râu trên dài) -> BỎ
    
    vol_ratio = last_candle['vol_ratio']
    if vol_ratio < 1.2: return None # Volume yếu -> BỎ

    # 4. Điều kiện Trend H1/H4
    if close_now < ema89_now: return None # Trend H1 phải Bullish
    h4_last = df_h4.iloc[-1]
    h4_trend = "Bullish ✅" if h4_last['close'] > h4_last['ema89'] else "Neutral ⚠️"

    # 5. Lọc FOMO (Giá đi quá xa MA20)
    dist_from_ma20_pct = ((close_now - ema20_now) / ema20_now) * 100
    if dist_from_ma20_pct > 2.5: return None # Cách MA20 quá 2.5% -> Chờ retest, KHÔNG báo đuổi.

    # 6. Xác định Vùng Thanh Khoản (Liquidity) làm SL & TP
    market_entry = close_now
    
    # Tìm Swing Low gần nhất (Đáy tạo bởi nhịp tích lũy trước đó)
    recent_lows = df_h1['low'].iloc[-15:-1]
    swing_low = recent_lows.min()
    sl = swing_low * 0.995 # SL dưới swing low kèm buffer 0.5%
    
    risk_pct = ((market_entry - sl) / market_entry) * 100
    if risk_pct < 1.0: # Nếu risk quá hẹp, nới SL ra tối thiểu 1.0% để tránh râu
        sl = market_entry * 0.99
        risk_pct = 1.0
    
    # Tìm Swing High gần nhất phía trên làm Liquidity TP1
    historical_highs = df_h1['high'].iloc[-50:-1]
    valid_highs = historical_highs[historical_highs > market_entry]
    
    if valid_highs.empty:
        # Nếu đang phá ATH cục bộ, TP1 mặc định R:R 1.5, TP2 2.5
        tp1 = market_entry + (market_entry - sl) * 1.5
        tp2 = market_entry + (market_entry - sl) * 2.5
        dist_tp1 = ((tp1 - market_entry) / market_entry) * 100
        rr = 1.5
    else:
        # Lấy đỉnh gần nhất làm vùng thanh khoản mục tiêu
        tp1_struct = valid_highs.iloc[::-1].idxmax() # Vị trí đỉnh gần nhất
        tp1 = df_h1['high'].loc[tp1_struct]
        
        # Tìm đỉnh tiếp theo cao hơn làm TP2
        higher_highs = historical_highs[historical_highs > tp1]
        tp2 = higher_highs.max() if not higher_highs.empty else tp1 * 1.03
        
        dist_tp1 = ((tp1 - market_entry) / market_entry) * 100
        rr = dist_tp1 / risk_pct
        
        # Nếu cản (TP1) quá gần, R:R < 1.5 -> Setup xấu, BỎ QUA
        if rr < 1.5: return None

    # 7. CHẤM ĐIỂM XẾP HẠNG (SCORE)
    score = 80
    if vol_ratio >= 1.8: score += 5
    if h4_trend == "Bullish ✅": score += 5
    if rr >= 2.0: score += 5
    if oi_change > 2.0: score += 3 # OI tăng mạnh hỗ trợ giá
    if body_pct >= 1.8: score += 2
    
    score = min(score, 100)
    
    # CHỈ BÁO KHI SCORE >= 85 (Ưu tiên chất lượng)
    if score < 85: return None
    
    tag = "🔥 STRONG LONG" if score >= 90 else "🟢 LONG"

    return {
        "timeframe": "H1",
        "tag": tag,
        "status": "LONG",
        "internal_setup_name": "H1 QUALITY LONG",
        "score": score,
        "market_entry": round(market_entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2),
        "dist_tp1": round(dist_tp1, 2),
        "vol_ratio": round(vol_ratio, 2),
        "trend_h4": h4_trend,
        "timestamp": last_candle['timestamp']
    }

# ==============================================================================
# 6. GỬI TELEGRAM & QUẢN LÝ DATABASE
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    msg = f"""{sig['tag']} — #{sig['symbol']}

<b>Entry:</b> <code>{sig['market_entry']:.4f}</code>
<b>SL:</b> <code>{sig['sl']:.4f}</code>
<b>TP1:</b> <code>{sig['tp1']:.4f}</code>
<b>TP2:</b> <code>{sig['tp2']:.4f}</code>
<b>RR:</b> {sig['rr']}

MA20: Break + Confirm ✅
Momentum: Strong ✅
Volume: {sig['vol_ratio']}x
Trend H1: Bullish ✅
Trend H4: {sig['trend_h4']}
Liquidity: Cách đỉnh {sig['dist_tp1']}%
Score: <b>{sig['score']}/100</b>

💡 <b>Lý do:</b>
Giá trước đó nằm dưới MA20 → H1 đóng vượt MA20 → xuất hiện lực tăng mạnh → cấu trúc tăng được xác nhận → phía trên còn thanh khoản để chạy."""

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

    print("⚡️ Bắt đầu H1 High-Quality Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins...", flush=True)

            scanned_signals = []
            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                realtime_price = t['lastPrice']

                df_h1 = get_closed_klines_df(sym, "60", limit=60)
                df_h4 = get_closed_klines_df(sym, "240", limit=40)
                
                if not df_h1.empty and not df_h4.empty:
                    df_h1 = add_indicators(df_h1)
                    df_h4 = add_indicators(df_h4)
                    
                    oi_change = get_open_interest_change(sym, "60")
                    
                    sig = analyze_h1_strict_long(df_h1, df_h4, realtime_price, oi_change)
                    if sig:
                        sig['symbol'] = sym
                        scanned_signals.append(sig)

            # Lọc ưu tiên Score cao nhất
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
                            print(f"✅ [{sig['tag']}] #{sig['symbol']} | Entry: {sig['market_entry']} | Score: {sig['score']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
