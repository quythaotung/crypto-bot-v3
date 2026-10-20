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
    return "Bybit H4 EMA200 + H1 EMA21 Reversal Scanner Active 24/7!"

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
# 3. SQLITE DATABASE (CHỐNG GỬI TRÙNG KÈO)
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
                setup_name TEXT DEFAULT 'H4_EMA200_H1_EMA21_REVERSAL',
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
    
    # Tính ATR(14) cho bộ lọc giá cách xa EMA21
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()
    return df

# ==============================================================================
# 5. NHẬN DIỆN MÔ HÌNH NẾN ĐẢO CHIỀU TĂNG HƠP LỆ (BULLISH CANDLESTICKS)
# ==============================================================================
def check_bullish_reversal_pattern(df_h1: pd.DataFrame) -> tuple:
    if len(df_h1) < 4: return False, "Không đủ dữ liệu nến"

    c1 = df_h1.iloc[-1] # Nến vừa đóng cửa
    c2 = df_h1.iloc[-2] # Nến trước đó
    c3 = df_h1.iloc[-3] # Nến 2 bước trước

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

    # 4. Morning Star (Mô hình 3 nến)
    c2_body = abs(c2['close'] - c2['open'])
    c3_bear = c3['close'] < c3['open']
    if c3_bear and (c2_body <= 0.3 * (c3['high'] - c3['low'])) and is_c1_bull and (c1_c >= (c3['open'] + c3['close']) / 2):
        return True, "Morning Star"

    return False, "Không có mô hình nến đảo chiều tăng hợp lệ"

# ==============================================================================
# 6. ENGINE PHÁT HIỆN SETUP DUY NHẤT: H4 EMA200 + H1 EMA21 REVERSAL
# ==============================================================================
def analyze_single_setup(symbol: str, df_h1: pd.DataFrame, df_h4: pd.DataFrame) -> tuple:
    if df_h1.empty or len(df_h1) < 200 or df_h4.empty or len(df_h4) < 200:
        return None, "Thiếu dữ liệu nến H1 hoặc H4 (<200 nến)"

    last_h1 = df_h1.iloc[-1]
    last_h4 = df_h4.iloc[-1]

    # 1. Điều kiện xu hướng H4: Nến H4 đóng NẰM TRÊN EMA200 H4
    if last_h4['close'] <= last_h4['ema200']:
        return None, f"H4 Close ({last_h4['close']}) nằm dưới EMA200 H4 ({last_h4['ema200']:.4f})"

    # 2. Điều kiện vị trí giá H1: Nến H1 đóng NẰM TRÊN EMA21 H1
    if last_h1['close'] <= last_h1['ema21']:
        return None, f"H1 Close ({last_h1['close']}) nằm dưới EMA21 H1 ({last_h1['ema21']:.4f})"

    # Lọc giá chưa tăng quá xa EMA21 (Cách EMA21 <= 1.5% HOẶC <= 1.5 * ATR14)
    dist_pct = ((last_h1['close'] - last_h1['ema21']) / last_h1['ema21']) * 100
    atr_limit = (1.5 * last_h1['atr14'] / last_h1['close']) * 100 if last_h1['atr14'] > 0 else 1.5
    max_dist_allowed = max(1.5, atr_limit)

    if dist_pct > max_dist_allowed:
        return None, f"Giá đã tăng quá xa EMA21 ({dist_pct:.2f}% > {max_dist_allowed:.2f}%)"

    # Lọc giá phải vừa chạm/hồi về gần EMA21 trong 1-5 nến vừa qua
    recent_klines = df_h1.iloc[-6:-1]
    touched_ema21 = any(row['low'] <= row['ema21'] * 1.008 for _, row in recent_klines.iterrows())
    if not touched_ema21:
        return None, "Chưa có nhịp hồi gần/chạm EMA21 H1 trước đó"

    # 3. Điều kiện Mô hình Nến đảo chiều tăng hợp lệ
    has_pattern, pattern_name = check_bullish_reversal_pattern(df_h1)
    if not has_pattern:
        return None, pattern_name

    # 4. XÁC ĐỊNH ENTRY, STOP LOSS VÀ TAKE PROFIT KỸ THUẬT
    entry = last_h1['close']
    
    # SL đặt dưới đáy nhịp hồi gần nhất hoặc đáy nến xác nhận + buffer 0.3%
    pullback_low = min(df_h1['low'].iloc[-6:-1].min(), last_h1['low'])
    sl = pullback_low * 0.997
    
    risk_pct = ((entry - sl) / entry) * 100
    if risk_pct < 0.8:
        sl = entry * 0.985
        risk_pct = ((entry - sl) / entry) * 100

    # TP dựa trên kháng cự / swing high gần nhất phía trên
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

    # Lọc tỷ lệ Risk:Reward tối thiểu >= 1:1.5
    if rr < 1.5:
        return None, f"Tỷ lệ R:R quá thấp / Kháng cự quá gần (RR: {rr:.2f} < 1.5)"

    return {
        "symbol": symbol,
        "internal_setup_name": "H4_EMA200_H1_EMA21_REVERSAL",
        "tag": "🎯 H4 EMA200 + H1 EMA21 REVERSAL LONG",
        "pattern_name": pattern_name,
        "candle_ts": int(last_h1['timestamp']),
        "market_entry": round(entry, 4),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "rr": round(rr, 2),
        "dist_pct": round(dist_pct, 2)
    }, f"ĐẠT TÍN HIỆU ({pattern_name})"

# ==============================================================================
# 7. GỬI CẢNH BÁO TELEGRAM
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM ERROR] Chưa cấu hình BOT_TOKEN hoặc CHAT_ID", flush=True)
        return False

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
# 8. VÒNG LẶP SCANNER TOÀN DIỆN VÀ THỐNG KÊ LOG
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=run_flask, daemon=True).start()

    print("⚡️ Bắt đầu H4 EMA200 + H1 EMA21 Reversal Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            
            # Khởi tạo các biến đếm thống kê cho từng vòng quét
            stat_total_data = 0
            stat_pass_h4_ema200 = 0
            stat_pass_h1_ema21 = 0
            stat_pass_pattern = 0
            stat_pass_rr = 0
            stat_sent_success = 0
            stat_sent_fail = 0

            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins (Duy nhất 1 Setup mới)...", flush=True)

            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']

                df_h1 = get_closed_klines_df(sym, "60", limit=220)
                df_h4 = get_closed_klines_df(sym, "240", limit=220)
                
                if df_h1.empty or df_h4.empty or len(df_h1) < 200 or len(df_h4) < 200:
                    continue
                
                stat_total_data += 1
                df_h1 = add_indicators(df_h1)
                df_h4 = add_indicators(df_h4)

                # Kiểm tra từng phễu điều kiện để thống kê
                if df_h4.iloc[-1]['close'] > df_h4.iloc[-1]['ema200']:
                    stat_pass_h4_ema200 += 1
                    
                    if df_h1.iloc[-1]['close'] > df_h1.iloc[-1]['ema21']:
                        stat_pass_h1_ema21 += 1
                        
                        has_pat, pat_name = check_bullish_reversal_pattern(df_h1)
                        if has_pat:
                            stat_pass_pattern += 1

                # Chạy phân tích full setup
                sig, reason = analyze_single_setup(sym, df_h1, df_h4)
                
                if sig:
                    stat_pass_rr += 1
                    
                    # Kiểm tra giới hạn số lệnh đang mở
                    if get_active_trades_count() >= MAX_ACTIVE_TRADES and not is_symbol_currently_active(sym):
                        print(f"[{sym}] [BỎ QUA] Vượt quá giới hạn active trades ({MAX_ACTIVE_TRADES})", flush=True)
                        continue

                    # Chống gửi trùng theo symbol + setup + timestamp nến H1
                    if not is_signal_already_sent(sym, sig['internal_setup_name'], sig['candle_ts']):
                        if send_telegram_alert(sig):
                            record_sent_signal(sym, sig['internal_setup_name'], sig['candle_ts'])
                            save_active_trade(sig)
                            stat_sent_success += 1
                            print(f"✅ [GỬI TELEGRAM] #{sym} | Mô hình: {sig['pattern_name']} | Entry: {sig['market_entry']}", flush=True)
                        else:
                            stat_sent_fail += 1
                    else:
                        print(f"[{sym}] [TRÙNG LẶP] Tín hiệu đã gửi trước đó cho nến H1 này.", flush=True)
                else:
                    print(f"[{sym}] [LOẠI] {reason}", flush=True)

            # In báo cáo thống kê đầy đủ sau mỗi vòng quét
            print(f"\n📊 --- BÁO CÁO THỐNG KÊ VÒNG QUÉT ---", flush=True)
            print(f"• Số coin đủ dữ liệu (>=200 nến): {stat_total_data}", flush=True)
            print(f"• Đạt điều kiện H4 Close > EMA200: {stat_pass_h4_ema200}", flush=True)
            print(f"• Đạt điều kiện H1 Close > EMA21:  {stat_pass_h1_ema21}", flush=True)
            print(f"• Có mô hình nến đảo chiều tăng:  {stat_pass_pattern}", flush=True)
            print(f"• Đạt bộ lọc R:R >= 1:1.5 (Full):   {stat_pass_rr}", flush=True)
            print(f"• Gửi Telegram thành công:       {stat_sent_success}", flush=True)
            print(f"• Gửi Telegram thất bại:         {stat_sent_fail}", flush=True)
            print(f"-----------------------------------------\n", flush=True)

        except Exception as e:
            print(f"[ERROR] Vòng lặp Scanner gặp sự cố: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_scanner_loop()
