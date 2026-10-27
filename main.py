import os
import time
import requests
import numpy as np
import pandas as pd
import sqlite3
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. KHỞI TẠO FLASK SERVER (KEEP-ALIVE)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Pro Quant Engine (ATR Normalized & BTC Regime) Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BIẾN MÔI TRƯỜNG & HỆ THỐNG
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "1000000")) # Lọc thanh khoản > 1M USD
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "300")) # Quét tối đa 300 coin

MAX_ACTIVE_TRADES = 3  # Giới hạn rủi ro: Tối đa 3 lệnh mở cùng lúc
DB_PATH = "quant_signals.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE (QUẢN LÝ LỆNH & WATCHLIST)
# ==============================================================================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sent_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                setup_name TEXT NOT NULL,
                candle_timestamp INTEGER NOT NULL,
                UNIQUE(symbol, setup_name, candle_timestamp)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                entry REAL NOT NULL,
                status TEXT DEFAULT 'OPEN'
            )
        ''')
        conn.commit()

def is_signal_sent(symbol: str, setup_name: str, candle_ts: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sent_signals WHERE symbol = ? AND setup_name = ? AND candle_timestamp = ?", (symbol, setup_name, candle_ts))
        return cursor.fetchone() is not None

def record_signal(symbol: str, setup_name: str, candle_ts: int):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO sent_signals (symbol, setup_name, candle_timestamp) VALUES (?, ?, ?)", (symbol, setup_name, candle_ts))
        conn.commit()

def get_active_trades_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM active_trades WHERE status = 'OPEN'")
        return cursor.fetchone()[0]

# ==============================================================================
# 4. TẢI DỮ LIỆU & TÍNH TOÁN KỸ THUẬT (ATR, ADX, RVOL)
# ==============================================================================
def get_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0:
            tickers = res["result"]["list"]
            valid = []
            for t in tickers:
                sym = t.get("symbol", "")
                turnover = float(t.get("turnover24h", 0))
                price_chg = float(t.get("price24hPcnt", 0)) * 100
                if sym.endswith("USDT") and turnover >= MIN_TURNOVER_24H and price_chg <= 20.0:
                    valid.append({"symbol": sym, "turnover": turnover})
            return sorted(valid, key=lambda x: x["turnover"], reverse=True)
    except: pass
    return []

def get_klines(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw = res["result"]["list"][1:] 
            df = pd.DataFrame(raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True).astype(float)
            return df
    except: pass
    return pd.DataFrame()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 200: return df
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    
    df['vol_sma20'] = df['volume'].rolling(20).mean()
    df['rvol'] = df['volume'] / df['vol_sma20']
    
    return df

def get_btc_regime(df_btc_h1: pd.DataFrame) -> str:
    if df_btc_h1.empty: return "SAFE"
    last = df_btc_h1.iloc[-1]
    if last['close'] < last['ema50']: return "DEFENSIVE"
    return "SAFE"

# ==============================================================================
# 5. CÁC SETUP LỌC ĐỊNH LƯỢNG (QUANT ENGINES)
# ==============================================================================

def quant_setup_pullback(sym, df_h1, df_h4, btc_regime):
    h4, h1 = df_h4.iloc[-1], df_h1.iloc[-1]
    
    trend_score = 0
    if h4['close'] > h4['ema200'] and h4['ema50'] >= h4['ema200']: trend_score = 25
    elif h4['close'] > h4['ema200']: trend_score = 15
    if trend_score == 0: return None
    
    dist_atr = abs(h1['close'] - h1['ema21']) / h1['atr14']
    loc_score = 0
    if dist_atr <= 0.5 and h1['close'] >= h1['ema21']: loc_score = 25
    elif dist_atr <= 1.0 and h1['close'] >= h1['ema21']: loc_score = 15
    elif dist_atr <= 1.5: loc_score = 5
    if loc_score == 0: return None
    
    body = abs(h1['close'] - h1['open'])
    lower_wick = min(h1['close'], h1['open']) - h1['low']
    conf_score = 0
    if lower_wick >= 2 * body and h1['close'] > h1['open']: conf_score = 20 
    elif h1['close'] > h1['open'] and h1['close'] > df_h1.iloc[-2]['high']: conf_score = 15 
    
    vol_score = 15 if h1['rvol'] >= 1.2 else (5 if h1['rvol'] >= 0.8 else 0)
    mom_score = 15 if h1['ema21'] > df_h1.iloc[-5]['ema21'] else 0
    
    total_score = trend_score + loc_score + conf_score + vol_score + mom_score
    if total_score < 60: return None
    
    entry = h1['close']
    swing_low = df_h1['low'].iloc[-6:-1].min()
    sl = min(swing_low, h1['low']) - (0.5 * h1['atr14'])
    
    risk = entry - sl
    if risk <= 0 or (risk / entry * 100 > 10): return None 
    
    tp1 = entry + (1.5 * risk)
    tp2 = entry + (3.0 * risk)
    
    return {"symbol": sym, "setup": "S1_ATR_PULLBACK", "score": total_score, "class": "A" if total_score>=75 else "B", "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": 1.5, "ts": int(h1['timestamp'])}

def quant_setup_base_breakout(sym, df_h1, df_h4, btc_regime):
    h4, h1 = df_h4.iloc[-1], df_h1.iloc[-1]
    
    base_high = df_h1['high'].iloc[-25:-1].max()
    base_low = df_h1['low'].iloc[-25:-1].min()
    base_range_atr = (base_high - base_low) / h1['atr14']
    
    if base_range_atr > 4.0: return None 
    if h4['ema200'] < df_h4.iloc[-5]['ema200']: return None 
    
    if h1['close'] <= base_high: return None 
    if h1['rvol'] < 1.5 or h1['rvol'] > 4.5: return None 
    
    if (h1['close'] - base_high) / h1['atr14'] > 1.5: return None
    
    entry = h1['close']
    sl = base_low - (0.3 * h1['atr14'])
    risk = entry - sl
    tp1 = entry + (1.5 * risk)
    tp2 = entry + (3.0 * risk)
    
    total_score = 85 
    return {"symbol": sym, "setup": "S5_BASE_BREAKOUT", "score": total_score, "class": "A", "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": 1.5, "ts": int(h1['timestamp'])}

def quant_setup_rs_leader(sym, df_h1, df_btc_h1, btc_regime):
    h1, btc_h1 = df_h1.iloc[-1], df_btc_h1.iloc[-1]
    h1_12, btc_12 = df_h1.iloc[-12], df_btc_h1.iloc[-12]
    
    sym_ret = (h1['close'] - h1_12['close']) / h1_12['close']
    btc_ret = (btc_h1['close'] - btc_12['close']) / btc_12['close']
    
    if sym_ret - btc_ret < 0.02: return None 
    
    dist_atr = (h1['close'] - h1['ema21']) / h1['atr14']
    if dist_atr > 2.0 or dist_atr < 0: return None
    
    if h1['rvol'] < 1.5: return None
    
    entry = h1['close']
    sl = df_h1['low'].iloc[-6:-1].min() - (0.5 * h1['atr14'])
    risk = entry - sl
    tp1 = entry + (1.5 * risk)
    tp2 = entry + (3.0 * risk)
    
    return {"symbol": sym, "setup": "S6_RS_LEADER", "score": 90, "class": "A", "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": 1.5, "ts": int(h1['timestamp'])}

# ==============================================================================
# 6. TELEGRAM ALERT
# ==============================================================================
def send_telegram_alert(sig: dict, btc_regime: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False
    
    size_advice = "Vào 100% Vol (Hạng A)" if sig['class'] == 'A' else "Vào 50% Vol (Hạng B)"
    if btc_regime == "DEFENSIVE": size_advice = "⚠️ BTC ĐANG XẤU - Chỉ vào 30-50% Vol hoặc Bỏ qua!"

    msg = f"""🚀 <b>[QUANT SYSTEM SIGNAL] #{sig['symbol']}</b>
-------------------------------------
⭐️ <b>HẠNG TÍN HIỆU: CLASS {sig['class']} (Score: {sig['score']}/100)</b>
📊 <b>TRẠNG THÁI BTC: {btc_regime}</b>
💡 <b>KHUYẾN NGHỊ VỐN:</b> <i>{size_advice}</i>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>

📍 <b>Entry (Thị trường):</b> <code>{sig['entry']:.4f}</code>
🛑 <b>Stop Loss (Đệm ATR):</b> <code>{sig['sl']:.4f}</code>
🎯 <b>Take Profit 1 (1.5R):</b> <code>{sig['tp1']:.4f}</code> <i>(Chốt 50%, dời SL hòa vốn)</i>
🚀 <b>Take Profit 2 (Trailing):</b> <code>{sig['tp2']:.4f}</code> <i>(Gồng theo EMA21 H1)</i>
-------------------------------------
👉 <b>{sig['setup']} - STRICT RISK MANAGEMENT!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=8)
        return res.status_code == 200
    except: return False

# ==============================================================================
# 7. VÒNG LẶP QUÉT ĐỊNH LƯỢNG (MAIN LOOP)
# ==============================================================================
def run_quant_scanner():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu Quant Engine (Quét 300 Coin, Turnover > 1M)...", flush=True)
    while True:
        try:
            df_btc_h1 = get_klines("BTCUSDT", "60", limit=220)
            if df_btc_h1.empty:
                time.sleep(10)
                continue
            
            df_btc_h1 = add_indicators(df_btc_h1)
            btc_regime = get_btc_regime(df_btc_h1)
            print(f"\n🌍 Trạng thái thị trường (BTC Regime): {btc_regime}", flush=True)
            
            active_trades = get_active_trades_count()
            if active_trades >= MAX_ACTIVE_TRADES:
                print(f"⏸ Đã đạt max {MAX_ACTIVE_TRADES} lệnh mở. Tạm ngưng quét...", flush=True)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            tickers = get_tickers()
            sent_count = 0
            print(f"🔍 [SCANNER] Đang rà soát {len(tickers[:MAX_SYMBOLS_SCAN])} đồng có thanh khoản tốt...", flush=True)

            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                print(f"  ➤ Đang soi Quant Model: {sym}...", end="\r", flush=True) 

                df_h1 = get_klines(sym, "60", limit=220)
                df_h4 = get_klines(sym, "240", limit=220)
                if df_h1.empty or df_h4.empty: continue
                    
                df_h1 = add_indicators(df_h1)
                df_h4 = add_indicators(df_h4)

                candidates = []
                for eval_fn in [
                    lambda: quant_setup_pullback(sym, df_h1, df_h4, btc_regime),
                    lambda: quant_setup_base_breakout(sym, df_h1, df_h4, btc_regime),
                    lambda: quant_setup_rs_leader(sym, df_h1, df_btc_h1, btc_regime)
                ]:
                    res = eval_fn()
                    if res: candidates.append(res)

                if candidates:
                    best_sig = max(candidates, key=lambda x: x['score'])
                    
                    if btc_regime == "DEFENSIVE" and best_sig['class'] == 'B':
                        continue

                    if not is_signal_sent(sym, best_sig['setup'], best_sig['ts']):
                        if send_telegram_alert(best_sig, btc_regime):
                            record_signal(sym, best_sig['setup'], best_sig['ts'])
                            sent_count += 1
                            print(f"\n✅ [SIGNAL FIRED] #{sym} | Hạng {best_sig['class']} | Setup: {best_sig['setup']}", flush=True)
                            
                            print(f"⏳ Cooldown 5 phút sau khi bắn lệnh...", flush=True)
                            time.sleep(300)
                            break 

            print(f"\n📊 --- BÁO CÁO VÒNG QUÉT ---", flush=True)
            print(f"• Tín hiệu nổ thành công: {sent_count}")
            print(f"⏳ Bot tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)

        except Exception as e:
            print(f"\n[ERROR] Quant Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_quant_scanner()
