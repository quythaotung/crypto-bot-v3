import os
import time
import requests
import numpy as np
import sqlite3
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from flask import Flask

# ==============================================================================
# 1. KHỞI TẠO FLASK KEEP-ALIVE SERVER (DÙNG CHO RENDER/RAILWAY 24/7)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit H1 LONG Sniper Bot is Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. ĐỌC CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG (ENVIRONMENT VARIABLES)
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_SCORE = int(os.environ.get("MIN_SCORE", "85"))
MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "5000000"))
MIN_H1_VOLUME_RATIO = float(os.environ.get("MIN_H1_VOLUME_RATIO", "1.8"))
MIN_TP1_MOVE_PCT = float(os.environ.get("MIN_TP1_MOVE_PCT", "5.0"))
MIN_RR_TP1 = float(os.environ.get("MIN_RR_TP1", "2.0"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
MAX_SYMBOLS_TO_SCAN = int(os.environ.get("MAX_SYMBOLS_TO_SCAN", "60"))
DB_PATH = "signals_history.db"

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. QUẢN LÝ CƠ SỞ DỮ LIỆU SQLITE (CHỐNG GỬI TRÙNG TÍN HIỆU)
# ==============================================================================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sent_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                h1_timestamp INTEGER NOT NULL,
                setup_name TEXT,
                score INTEGER,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, h1_timestamp)
            )
        """)
        conn.commit()

def is_signal_sent(symbol: str, h1_timestamp: int) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sent_signals WHERE symbol = ? AND h1_timestamp = ?",
            (symbol, h1_timestamp)
        )
        return cursor.fetchone() is not None

def record_signal(symbol: str, h1_timestamp: int, setup_name: str, score: int):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO sent_signals (symbol, h1_timestamp, setup_name, score) VALUES (?, ?, ?, ?)",
            (symbol, h1_timestamp, setup_name, score)
        )
        conn.commit()

# ==============================================================================
# 4. BYBIT API V5 HELPERS & CÁC CHỈ BÁO KỸ THUẬT (CLOSED CANDLES ONLY)
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_usdt_perpetual_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto = ['XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD',
                          'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                          'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', '3L', '3S']
            valid = []
            for t in tickers:
                sym = t.get("symbol", "")
                turnover = safe_float(t.get("turnover24h", 0))
                last_price = safe_float(t.get("lastPrice", 0))
                funding_rate = safe_float(t.get("fundingRate", 0)) * 100

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI")):
                    if turnover >= MIN_TURNOVER_24H and not any(k in sym for k in non_crypto):
                        valid.append({
                            "symbol": sym,
                            "turnover24h": turnover,
                            "lastPrice": last_price,
                            "fundingRate": funding_rate
                        })
            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Tickers từ Bybit: {e}", flush=True)
    return []

def get_closed_klines(symbol: str, interval: str, limit: int = 40):
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 2: return []
            # Loại bỏ index 0 (nến đang chạy) và đảo ngược mảng
            closed_raw = raw_list[1:]
            parsed = []
            for c in reversed(closed_raw):
                parsed.append({
                    "timestamp": int(c[0]),
                    "open": safe_float(c[1]),
                    "high": safe_float(c[2]),
                    "low": safe_float(c[3]),
                    "close": safe_float(c[4]),
                    "volume": safe_float(c[5])
                })
            return parsed
    except Exception:
        pass
    return []

def get_open_interest_change(symbol: str):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=1h&limit=3"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = safe_float(oi_list[0].get("openInterest", 0))
                prev_oi = safe_float(oi_list[1].get("openInterest", 0))
                if prev_oi > 0:
                    return round(((curr_oi - prev_oi) / prev_oi) * 100, 2), "1h", "Valid"
    except Exception:
        pass
    return 0.0, "N/A", "Data Unavailable"

def get_taker_buy_ratio_5m(symbol: str):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=5m&limit=2"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return round(safe_float(res["result"]["list"][0].get("buyRatio", 0.5)) * 100, 1)
    except Exception:
        pass
    return 50.0

def calculate_ema(candles: list, period: int):
    if len(candles) < period: return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema

def calculate_rsi(candles: list, period: int = 14):
    if len(candles) < period + 1: return []
    closes = [c['close'] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    rsi = []
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rs = avg_g / avg_l if avg_l != 0 else 100
    rsi.append(100 - (100 / (1 + rs)))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l != 0 else 100
        rsi.append(100 - (100 / (1 + rs)))
    return rsi

def calculate_atr(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i]['high'], candles[i]['low'], candles[i-1]['close']
        tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return float(np.mean(tr_list[-period:])) if tr_list else 0.0

def detect_h1_patterns(h1_candles: list) -> list:
    if len(h1_candles) < 3: return []
    patterns = []
    c1, c2, c3 = h1_candles[-1], h1_candles[-2], h1_candles[-3]
    o1, h1, l1, cl1 = c1['open'], c1['high'], c1['low'], c1['close']
    o2, h2, l2, cl2 = c2['open'], c2['high'], c2['low'], c2['close']
    o3, h3, l3, cl3 = c3['open'], c3['high'], c3['low'], c3['close']

    body1 = abs(cl1 - o1)
    range1 = (h1 - l1) if (h1 - l1) > 0 else 1e-8
    lower_wick1 = min(o1, cl1) - l1
    upper_wick1 = h1 - max(o1, cl1)

    if lower_wick1 >= 1.5 * body1 and cl1 >= (l1 + 0.45 * range1):
        patterns.append("Hammer / Bullish Pin Bar")
    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        patterns.append("Bullish Engulfing")
    if cl3 < o3 and abs(cl2 - o2) <= 0.4 * abs(cl3 - o3) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        patterns.append("Morning Star")
    
    prev_high = max([c['high'] for c in h1_candles[-10:-1]])
    if cl1 > prev_high: patterns.append("Resistance Breakout")
    
    prev_low = min([c['low'] for c in h1_candles[-10:-1]])
    if l1 < prev_low and cl1 >= prev_low: patterns.append("Liquidity Sweep Reclaim")

    return patterns

# ==============================================================================
# 5. BỘ EVALUATOR LỌC TÍN HIỆU LONG VÀ TÍNH ENTRY/SL/TP
# ==============================================================================
def evaluate_long_candidate(ticker_info, config_turnover):
    try:
        sym = ticker_info['symbol']
        curr_price = ticker_info['lastPrice']
        funding = ticker_info['fundingRate']

        h1_candles = get_closed_klines(sym, "60", limit=40)
        h4_candles = get_closed_klines(sym, "240", limit=40)

        if len(h1_candles) < 30 or len(h4_candles) < 30: return None

        last_h1 = h1_candles[-1]
        cl1 = last_h1['close']
        h1_time = last_h1['timestamp']

        ema20_h1 = calculate_ema(h1_candles, 20)
        ema50_h1 = calculate_ema(h1_candles, 50)
        ema20_h4 = calculate_ema(h4_candles, 20)
        ema50_h4 = calculate_ema(h4_candles, 50)
        rsi_h1 = calculate_rsi(h1_candles, 14)
        atr_h1 = calculate_atr(h1_candles, 14)

        if not (ema20_h1 and ema50_h1 and ema20_h4 and ema50_h4 and rsi_h1): return None

        # Khối lượng H1
        vols = [c['volume'] for c in h1_candles[:-1]]
        avg_vol = np.mean(vols[-20:]) if vols else 1.0
        vol_ratio = float(last_h1['volume'] / avg_vol) if avg_vol > 0 else 1.0

        if vol_ratio < MIN_H1_VOLUME_RATIO: return None  # Hard Gate Volume

        oi_change, oi_interval, oi_status = get_open_interest_change(sym)
        taker_buy_pct = get_taker_buy_ratio_5m(sym)

        # Hard Gate OI & Taker Buy
        if oi_change < 1.0 or taker_buy_pct < 55.0: return None

        patterns = detect_h1_patterns(h1_candles)
        if not patterns: return None

        # CHẤM ĐIỂM
        score = 0
        reasons = []

        # Trend (20đ)
        if ema20_h1[-1] > ema50_h1[-1] and cl1 > ema20_h1[-1]: score += 10
        if ema20_h4[-1] > ema50_h4[-1]: score += 10
        reasons.append("Cấu trúc xu hướng H1/H4 đồng thuận")

        # Pattern (30đ)
        p_score = min(30, len(patterns) * 15)
        score += p_score
        reasons.append(f"Mô hình: {', '.join(patterns)}")

        # Vol (15đ)
        v_score = 15 if vol_ratio >= 2.2 else 10
        score += v_score
        reasons.append(f"Volume H1 bùng nổ {vol_ratio:.1f}x")

        # OI (15đ)
        if oi_change >= 2.0: score += 15
        else: score += 8
        reasons.append(f"OI 1h tăng {oi_change:+}%")

        # RSI (5đ) & Strength (10đ) & Quality (5đ)
        if 40 <= rsi_h1[-1] <= 70: score += 5
        score += 15

        if score < MIN_SCORE: return None

        # Tính toán Entry, SL, TP
        entry = cl1
        if abs(curr_price - entry) / entry * 100 > 1.0: return None # Chống đuổi giá

        swing_low = min([c['low'] for c in h1_candles[-10:]])
        sl = swing_low - (0.3 * atr_h1)
        risk = entry - sl

        if risk <= 0 or (risk / entry * 100) > 6.0 or (risk / entry * 100) < 0.5: return None

        tp1 = max(entry + (risk * MIN_RR_TP1), entry * (1 + MIN_TP1_MOVE_PCT / 100))
        tp1_pct = ((tp1 - entry) / entry) * 100
        rr_tp1 = (tp1 - entry) / risk

        tp2 = entry + (risk * 3.5)
        tp3 = entry + (risk * 5.0)

        return {
            "symbol": sym,
            "h1_timestamp": h1_time,
            "score": score,
            "setup_name": " + ".join(patterns),
            "entry": entry,
            "sl": sl,
            "sl_pct": round((risk / entry) * 100, 2),
            "tp1": tp1,
            "tp1_pct": round(tp1_pct, 2),
            "rr_tp1": round(rr_tp1, 2),
            "tp2": tp2,
            "tp2_pct": round(((tp2 - entry) / entry) * 100, 2),
            "rr_tp2": 3.5,
            "tp3": tp3,
            "tp3_pct": round(((tp3 - entry) / entry) * 100, 2),
            "rr_tp3": 5.0,
            "vol_ratio": round(vol_ratio, 2),
            "oi_info": f"{oi_change:+}%/1h",
            "taker_buy": taker_buy_pct,
            "funding": funding,
            "reasons": reasons,
            "confirm_time": time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(h1_time / 1000))
        }
    except Exception:
        return None

# ==============================================================================
# 6. GỬI TIN NHẮN TELEGRAM
# ==============================================================================
def send_telegram_alert(sig):
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[WARN] Chưa cấu hình Telegram Token!", flush=True)
        return False

    msg = f"""🚀 <b>LONG — {sig['symbol']}</b>
<b>Setup:</b> {sig['setup_name']}

💰 <b>Entry:</b> {sig['entry']:.4f}
🛡 <b>SL:</b> {sig['sl']:.4f} (-{sig['sl_pct']}%)
🎯 <b>TP1:</b> {sig['tp1']:.4f} (+{sig['tp1_pct']}%, R:R = {sig['rr_tp1']})
🎯 <b>TP2:</b> {sig['tp2']:.4f} (+{sig['tp2_pct']}%, R:R = {sig['rr_tp2']})
🎯 <b>TP3:</b> {sig['tp3']:.4f} (+{sig['tp3_pct']}%, R:R = {sig['rr_tp3']})

⭐ <b>Score:</b> {sig['score']}/100
📊 <b>H1 Volume:</b> {sig['vol_ratio']}x
💰 <b>Taker Buy Pressure:</b> {sig['taker_buy']}%
📈 <b>OI Change:</b> {sig['oi_info']}
💸 <b>Funding Rate:</b> {sig['funding']:.3f}%

💡 <b>Lý do vào lệnh:</b>
• {chr(10).join(['• ' + r for r in sig['reasons']])}

🚫 <b>Vô hiệu khi:</b> Giá đóng nến H1 dưới {sig['sl']:.4f}
🕒 <b>Nến xác nhận:</b> {sig['confirm_time']}

<i>Lưu ý: Đây là cảnh báo phân tích kỹ thuật, không phải cam kết lợi nhuận.</i>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        print(f"[ERROR] Lỗi gửi Telegram: {e}", flush=True)
        return False

# ==============================================================================
# 7. AUTOMATIC SCANNER LOOP
# ==============================================================================
def run_scanner_loop():
    init_db()
    print("⚡️ Bắt đầu Bybit H1 LONG Scanner Engine Active...", flush=True)
    while True:
        try:
            tickers = get_usdt_perpetual_tickers()
            target_tickers = tickers[:MAX_SYMBOLS_TO_SCAN]
            print(f"\n🔍 [SCANNER] Đang rà soát tín hiệu LONG cho {len(target_tickers)} Altcoins...", flush=True)

            scanned_signals = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(evaluate_long_candidate, t, MIN_TURNOVER_24H) for t in target_tickers]
                for f in futures:
                    res = f.result()
                    if res:
                        # Kiểm tra chống gửi trùng bằng timestamp nến
                        if not is_signal_sent(res['symbol'], res['h1_timestamp']):
                            scanned_signals.append(res)

            scanned_signals.sort(key=lambda x: x['score'], reverse=True)

            if scanned_signals:
                for sig in scanned_signals:
                    if send_telegram_alert(sig):
                        record_signal(sig['symbol'], sig['h1_timestamp'], sig['setup_name'], sig['score'])
                        print(f"✅ [SIGNAL SENT] LONG #{sig['symbol']} Score: {sig['score']}", flush=True)
            else:
                print("🔎 Không có tín hiệu LONG đạt chuẩn trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_scanner_loop()
