import os
import time
import json
import requests
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG & ĐIỀU KIỆN LỌC (CONFIGURATIONS)
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # 1. Lọc Altcoin thanh khoản >= $8M USD
TOP_CANDIDATES_LIMIT = 50              # Lọc Top candidates theo khối lượng giao dịch

# Telegram Bot Token & Chat ID
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

# Signal Filters & Cooldown
MIN_SETUP_SCORE = 75                   # 18 & 19. Score tối thiểu gửi signal
MIN_TP1_STRENGTH_SCORE = 75            # 12 & 19. Độ mạnh TP1 tối thiểu
MIN_TP1_DISTANCE_PCT = 5.0             # 11 & 19. TP1 tối thiểu 5.0%
MIN_RR_TP1 = 1.5                       # 16 & 19. Risk/Reward TP1 tối thiểu
SIGNAL_COOLDOWN_SECONDS = 3600         # 22. Cooldown 60 phút chống spam cùng 1 coin/setup

# Cache & Local DB
processed_signals = {}
BACKTEST_DB_FILE = "signals_history.json"

# Web Server cho Keep-Alive 24/7 trên Cloud (Render/Replit)
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Futures High-Probability Scanner Bot Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. TELEGRAM API & BACKTEST LOGGER HELPERS
# ==============================================================================
def send_telegram_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("[WARN] Chưa cấu hình BOT_TOKEN hoặc CHAT_ID!", flush=True)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=8)
    except Exception as e:
        print(f"[ERROR] Lỗi gửi Telegram: {e}", flush=True)

def log_signal_for_backtest(signal_data):
    """23. Lưu dữ liệu signals xuống file JSON phục vụ Backtest live"""
    try:
        history = []
        if os.path.exists(BACKTEST_DB_FILE):
            with open(BACKTEST_DB_FILE, 'r', encoding='utf-8') as f:
                try:
                    history = json.load(f)
                except Exception:
                    history = []
        history.append(signal_data)
        with open(BACKTEST_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Lỗi lưu log Backtest: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. BYBIT API V5 DATA FETCHERS
# ==============================================================================
def get_all_bybit_futures_tickers():
    """1. Lọc coin Perpetual USDT có 24h Turnover >= 8M USD"""
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto = [
                'XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD',
                'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX', 'NVDA',
                'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', '3L', '3S'
            ]
            valid_list = []
            for t in tickers:
                sym = t['symbol']
                turnover = float(t.get('turnover24h', 0))
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and turnover >= MIN_FUTURES_TURNOVER_24H:
                    if not any(k in sym for k in non_crypto):
                        valid_list.append({
                            'symbol': sym,
                            'turnover24h': turnover,
                            'lastPrice': float(t.get('lastPrice', 0)),
                            'price24hPcnt': float(t.get('price24hPcnt', 0)) * 100
                        })
            return valid_list
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Tickers từ Bybit: {e}", flush=True)
    return []

def get_klines(symbol, interval, category="linear", limit=50):
    """2. Lấy dữ liệu OHLCV"""
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw = res["result"]["list"]
            parsed = []
            for c in reversed(raw):
                parsed.append({
                    'timestamp': int(c[0]),
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                })
            return parsed
    except Exception:
        pass
    return []

def get_open_interest_pct_30m(symbol):
    """8. Tính % thay đổi Open Interest"""
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=30m&limit=3"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

def get_buy_sell_ratios(symbol):
    """7. Tính Buy/Sell Pressure Ratio"""
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=5m&limit=3"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            l = res["result"]["list"]
            if len(l) >= 1:
                buy_ratio = float(l[0].get("buyRatio", 0.5))
                sell_ratio = float(l[0].get("sellRatio", 0.5))
                return buy_ratio, sell_ratio
    except Exception:
        pass
    return 0.50, 0.50

# ==============================================================================
# 4. KỸ THUẬT & INDICATORS (INDICATORS & TECHNICAL CALCULATIONS)
# ==============================================================================
def calculate_atr_h1(candles, period=14):
    """10 & 17. Tính ATR H1"""
    if len(candles) < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        h = candles[i]['high']
        l = candles[i]['low']
        prev_c = candles[i-1]['close']
        tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0

def calculate_ema(candles, period):
    if len(candles) < period:
        return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema = [sum(closes[:period]) / period]
    for price in closes[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema

def detect_h1_candle_pattern(h1_candles):
    """3. Nhận diện các mô hình nến đảo chiều chính H1"""
    if len(h1_candles) < 3:
        return "None", None

    c1 = h1_candles[-1] # Nến hiện tại
    c2 = h1_candles[-2] # Nến trước
    c3 = h1_candles[-3]

    o1, h1, l1, cl1 = c1['open'], c1['high'], c1['low'], c1['close']
    o2, h2, l2, cl2 = c2['open'], c2['high'], c2['low'], c2['close']
    o3, h3, l3, cl3 = c3['open'], c3['high'], c3['low'], c3['close']

    body1 = abs(cl1 - o1)
    range1 = (h1 - l1) if (h1 - l1) > 0 else 1.0
    lower_wick1 = min(o1, cl1) - l1
    upper_wick1 = h1 - max(o1, cl1)

    # Bullish Patterns
    if lower_wick1 >= 1.8 * body1 and cl1 >= l1 + (0.55 * range1):
        return "Hammer / Bullish Pin Bar", "LONG"
    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        return "Bullish Engulfing", "LONG"
    if cl3 < o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.4) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        return "Morning Star", "LONG"

    # Bearish Patterns
    if upper_wick1 >= 1.8 * body1 and cl1 <= l1 + (0.45 * range1):
        return "Shooting Star / Bearish Pin Bar", "SHORT"
    if cl2 > o2 and cl1 < o1 and cl1 <= o2 and o1 >= cl2:
        return "Bearish Engulfing", "SHORT"
    if cl3 > o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.4) and cl1 < o1 and cl1 <= (o3 + cl3) / 2:
        return "Evening Star", "SHORT"

    return "None", None

# ==============================================================================
# 5. BỘ LỌC SETUP TÍNH SCORE & CHÉM SIGNAL (CORE SCANNING ENGINE)
# ==============================================================================
def analyze_bybit_symbol(ticker_info):
    sym = ticker_info['symbol']
    turnover_24h = ticker_info['turnover24h']

    # 1 & 2. Lấy dữ liệu Nến H1, H4, D1
    h1_candles = get_klines(sym, "60", limit=45)
    h4_candles = get_klines(sym, "240", limit=30)
    d1_candles = get_klines(sym, "D", limit=15)

    if not h1_candles or len(h1_candles) < 30 or not h4_candles or len(h4_candles) < 10:
        return None

    last_h1 = h1_candles[-1]
    close_now = float(last_h1['close'])
    high_h1 = float(last_h1['high'])
    low_h1 = float(last_h1['low'])
    open_h1 = float(last_h1['open'])

    atr_h1 = calculate_atr_h1(h1_candles, 14)
    if atr_h1 <= 0:
        return None

    # Orderflow Data
    oi_30m_pct = get_open_interest_pct_30m(sym)
    buy_ratio, sell_ratio = get_buy_sell_ratios(sym)

    # Volume Expansion H1
    vols_20 = [c['volume'] for c in h1_candles[-21:-1]]
    avg_vol_20 = np.mean(vols_20) if vols_20 else 1.0
    vol_ratio = float(last_h1['volume']) / avg_vol_20 if avg_vol_20 > 0 else 1.0

    # CVD & Divergence Estimation (Estimated via Vol & Price change delta)
    p_change_1h = ((close_now - open_h1) / open_h1) * 100
    cvd_dir = "↑" if (p_change_1h > 0 and buy_ratio >= 0.52) else ("↓" if (p_change_1h < 0 and sell_ratio >= 0.52) else "→")

    # Swings H1 & H4 Support/Resistance
    prev_swing_low_h1 = min([c['low'] for c in h1_candles[-10:-1]])
    prev_swing_high_h1 = max([c['high'] for c in h1_candles[-10:-1]])
    
    major_h4_resistance = max([c['high'] for c in h4_candles[-15:-1]])
    major_h4_support = min([c['low'] for c in h4_candles[-15:-1]])

    # 3. Liquidity Sweep & Reclaim
    is_sweep_low = (low_h1 < prev_swing_low_h1) and (close_now >= prev_swing_low_h1)
    is_sweep_high = (high_h1 > prev_swing_high_h1) and (close_now <= prev_swing_high_h1)

    pattern_name, pattern_dir = detect_h1_reversal_candle(h1_candles)

    # Phân định hướng Setup (LONG / SHORT)
    direction = None
    setup_type = "REVERSAL"

    if (is_sweep_low or (pattern_dir == "LONG" and close_now >= prev_swing_low_h1)):
        direction = "LONG"
    elif (is_sweep_high or (pattern_dir == "SHORT" and close_now <= prev_swing_high_h1)):
        direction = "SHORT"
    elif close_now > prev_swing_high_h1 and vol_ratio >= 1.8:
        direction = "LONG"
        setup_type = "BREAKOUT"
    elif close_now < prev_swing_low_h1 and vol_ratio >= 1.8:
        direction = "SHORT"
        setup_type = "BREAKOUT"

    if not direction:
        return None

    # 21. KIỂM TRA PHÂN LOẠI WATCH HAY CONFIRMED
    # Nếu đang hình thành nhưng chưa đủ điểm kích hoạt xác nhận trigger nến/volume -> WATCH
    is_confirmed_trigger = (pattern_dir == direction) or (vol_ratio >= 1.5 and is_sweep_low if direction == "LONG" else is_sweep_high)
    
    if not is_confirmed_trigger:
        # Báo dạng WATCH nếu có tín hiệu manh nha
        if (direction == "LONG" and buy_ratio >= 0.58) or (direction == "SHORT" and sell_ratio >= 0.58):
            return {
                'mode': 'WATCH',
                'symbol': sym,
                'direction': direction,
                'buy_ratio': buy_ratio,
                'sell_ratio': sell_ratio,
                'cvd_dir': cvd_dir
            }
        return None

    # 9. TÍNH ENTRY ZONE
    if direction == "LONG":
        entry_low = min(close_now, prev_swing_low_h1 * 1.001)
        entry_high = max(close_now, open_h1)
        # 10. STOP LOSS THEO KỸ THUẬT & ATR BUFFER
        sl_price = min(low_h1, prev_swing_low_h1) - (0.3 * atr_h1)
        risk = entry_high - sl_price
        
        # 11, 13, 14. TP1, TP2, TP3 TECHNICAL TARGETS
        tp1_raw = max(prev_swing_high_h1, entry_high + (entry_high * 0.051))
        # Bắt buộc TP1 >= 5%
        if ((tp1_raw - entry_high) / entry_high * 100) < MIN_TP1_DISTANCE_PCT:
            tp1_raw = entry_high * (1 + (MIN_TP1_DISTANCE_PCT / 100))
        
        tp1_price = tp1_raw
        tp2_price = max(major_h4_resistance, tp1_price * 1.04)
        
        d1_high = max([c['high'] for c in d1_candles[-10:]]) if d1_candles else tp2_price * 1.06
        tp3_price = max(d1_high, tp2_price * 1.05)
    else:
        entry_low = min(close_now, open_h1)
        entry_high = max(close_now, prev_swing_high_h1 * 0.999)
        sl_price = max(high_h1, prev_swing_high_h1) + (0.3 * atr_h1)
        risk = sl_price - entry_low

        tp1_raw = min(prev_swing_low_h1, entry_low * (1 - 0.051))
        if ((entry_low - tp1_raw) / entry_low * 100) < MIN_TP1_DISTANCE_PCT:
            tp1_raw = entry_low * (1 - (MIN_TP1_DISTANCE_PCT / 100))

        tp1_price = tp1_raw
        tp2_price = min(major_h4_support, tp1_price * 0.96)
        
        d1_low = min([c['low'] for c in d1_candles[-10:]]) if d1_candles else tp2_price * 0.94
        tp3_price = min(d1_low, tp2_price * 0.95)

    if risk <= 0:
        return None

    # 11 & 16. CALCULATE RR & TP DISTANCE %
    avg_entry = (entry_low + entry_high) / 2
    tp1_dist_pct = abs(tp1_price - avg_entry) / avg_entry * 100
    rr_tp1 = abs(tp1_price - avg_entry) / risk
    rr_tp2 = abs(tp2_price - avg_entry) / risk
    rr_tp3 = abs(tp3_price - avg_entry) / risk

    # 11 & 16, 19. CHECK HARD GATE CONDITIONAL FILTERS
    if tp1_dist_pct < MIN_TP1_DISTANCE_PCT or rr_tp1 < MIN_RR_TP1:
        return None

    # 18. TÍNH SETUP SCORE (100 PTS SYSTEM)
    score = 0
    if (direction == "LONG" and is_sweep_low) or (direction == "SHORT" and is_sweep_high): score += 15 # Liquidity Sweep
    if pattern_dir == direction: score += 15 # Structure & Candle
    if (direction == "LONG" and cvd_dir == "↑") or (direction == "SHORT" and cvd_dir == "↓"): score += 15 # CVD Alignment
    if (direction == "LONG" and buy_ratio >= 0.62) or (direction == "SHORT" and sell_ratio >= 0.62): score += 15 # Buy/Sell Pressure
    elif (direction == "LONG" and buy_ratio >= 0.58) or (direction == "SHORT" and sell_ratio >= 0.58): score += 8
    if oi_30m_pct >= 1.5: score += 15 # OI Confirmation
    elif oi_30m_pct >= 0.5: score += 8
    if vol_ratio >= 1.8: score += 15 # Volume
    elif vol_ratio >= 1.2: score += 8
    if rr_tp1 >= 2.0: score += 10 # R:R Excellent

    setup_score = min(100, score)

    # 12. TÍNH TP1 STRENGTH SCORE (MAX 100)
    tp1_strength = 50
    if vol_ratio >= 1.5: tp1_strength += 15
    if (direction == "LONG" and buy_ratio >= 0.60) or (direction == "SHORT" and sell_ratio >= 0.60): tp1_strength += 15
    if oi_30m_pct >= 1.0: tp1_strength += 20
    tp1_strength = min(100, tp1_strength)

    # 19. ĐIỀU KIỆN CUỐI CÙNG
    if setup_score < MIN_SETUP_SCORE or tp1_strength < MIN_TP1_STRENGTH_SCORE:
        return None

    return {
        'mode': 'CONFIRMED',
        'symbol': sym,
        'direction': direction,
        'setup_type': setup_type,
        'pattern_name': pattern_name,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'sl_price': sl_price,
        'tp1_price': tp1_price,
        'tp1_dist_pct': tp1_dist_pct,
        'rr_tp1': rr_tp1,
        'tp2_price': tp2_price,
        'tp2_dist_pct': abs(tp2_price - avg_entry) / avg_entry * 100,
        'rr_tp2': rr_tp2,
        'tp3_price': tp3_price,
        'tp3_dist_pct': abs(tp3_price - avg_entry) / avg_entry * 100,
        'rr_tp3': rr_tp3,
        'buy_ratio': buy_ratio,
        'sell_ratio': sell_ratio,
        'cvd_dir': cvd_dir,
        'oi_30m_pct': oi_30m_pct,
        'vol_ratio': vol_ratio,
        'setup_score': setup_score,
        'tp1_strength': tp1_strength,
        'turnover_24h': turnover_24h,
        'atr_h1': atr_h1,
        'is_sweep': (is_sweep_low if direction == "LONG" else is_sweep_high),
        'timestamp': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    }

# ==============================================================================
# 6. TELEGRAM FORMATTER & AUTOMATIC SCANNER LOOP
# ==============================================================================
def format_and_send_telegram(sig):
    sym = sig['symbol']
    now_time = time.time()

    # 22. COOLDOWN PREVENT SPAM
    if sym in processed_signals:
        last_time = processed_signals[sym]
        if now_time - last_time < SIGNAL_COOLDOWN_SECONDS:
            return
    processed_signals[sym] = now_time

    # 21. MODE WATCH MESSAGE
    if sig['mode'] == 'WATCH':
        pressure_val = int(sig['buy_ratio'] * 100) if sig['direction'] == "LONG" else int(sig['sell_ratio'] * 100)
        msg = f"""👀 <b>WATCH — {sig['symbol']}</b>

H1 Structure: <b>{sig['direction']}</b>
Liquidity: Gần vùng Sweep / Rejection
CVD Flow: <b>{sig['cvd_dir']}</b>
Pressure: <b>{pressure_val}%</b>

<i>Chờ: Liquidity Sweep + Reversal Candle Confirmation!</i>"""
        send_telegram_message(msg)
        return

    # 20. MODE CONFIRMED TELEGRAM SIGNAL FORMAT (EXACT TEMPLATE REQUIREMENT)
    if sig['direction'] == "LONG":
        header = f"🚀 <b>LONG — {sig['symbol']}</b>"
        pressure_str = f"Buy Pressure: {int(sig['buy_ratio'] * 100)}%"
        tp1_sign = "+"
        tp2_sign = "+"
        tp3_sign = "+"
    else:
        header = f"🔻 <b>SHORT — {sig['symbol']}</b>"
        pressure_str = f"Sell Pressure: {int(sig['sell_ratio'] * 100)}%"
        tp1_sign = "-"
        tp2_sign = "-"
        tp3_sign = "-"

    sweep_check = "✓" if sig['is_sweep'] else "x"

    msg = f"""{header}

📌 <b>Mô hình:</b>
H1 {sig['setup_type']} ({sig['pattern_name']})

💰 <b>Entry:</b>
{sig['entry_low']:.4f} – {sig['entry_high']:.4f}

🛡 <b>SL:</b>
{sig['sl_price']:.4f}

🎯 <b>TP1:</b>
{sig['tp1_price']:.4f} ({tp1_sign}{sig['tp1_dist_pct']:.1f}%)
R:R = {sig['rr_tp1']:.1f}

🎯 <b>TP2:</b>
{sig['tp2_price']:.4f} ({tp2_sign}{sig['tp2_dist_pct']:.1f}%)
R:R = {sig['rr_tp2']:.1f}

🎯 <b>TP3:</b>
{sig['tp3_price']:.4f} ({tp3_sign}{sig['tp3_dist_pct']:.1f}%)
R:R = {sig['rr_tp3']:.1f}

📊 <b>XÁC NHẬN</b>
H1 Structure: {sig['direction']} ✓
Liquidity Sweep: {sweep_check}
Candle Confirmation: ✓
CVD: {sig['cvd_dir']}
{pressure_str}
OI: {sig['oi_30m_pct']:+.1f}%
Volume: ↑ ({sig['vol_ratio']:.1f}x)

📈 <b>TP1 Strength:</b> {sig['tp1_strength']}/100
⭐ <b>Setup Score:</b> {sig['setup_score']}/100

⚙️ <b>Quản lý:</b>
TP1 → chốt 30%, SL về Entry
TP2 → chốt 30%, SL lên TP1
TP3 → giữ phần còn lại"""

    send_telegram_message(msg)
    log_signal_for_backtest(sig) # 23. Log backtest data
    print(f"✅ [SIGNAL SENT] #{sig['symbol']} {sig['direction']} Score: {sig['setup_score']}", flush=True)

def run_bybit_scanner_loop():
    print("⚡️ Bắt đầu Bybit Futures Scanner Engine Active...", flush=True)
    while True:
        try:
            tickers = get_all_bybit_futures_tickers()
            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins đạt chuẩn Turnover >= 8M USD...", flush=True)

            scanned_signals = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(analyze_bybit_symbol, t) for t in tickers]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_signals.append(res)

            # Lọc tín hiệu CONFIRMED tốt nhất
            confirmed_signals = [s for s in scanned_signals if s['mode'] == 'CONFIRMED']
            watch_signals = [s for s in scanned_signals if s['mode'] == 'WATCH']

            confirmed_signals.sort(key=lambda x: x['setup_score'], reverse=True)

            # Gửi tối đa Top Signals chất lượng nhất
            if confirmed_signals:
                for sig in confirmed_signals[:3]:
                    format_and_send_telegram(sig)
            elif watch_signals:
                # Nếu không có Confirmed, gửi tối đa 1 Watch
                format_and_send_telegram(watch_signals[0])
            else:
                print("🔎 Không có tín hiệu thỏa mãn điều kiện nghiêm ngặt trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

# ==============================================================================
# 7. MAIN ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    # Chạy Web server giữ alive
    Thread(target=run_flask, daemon=True).start()
    # Chạy scanner chính
    run_bybit_scanner_loop()
