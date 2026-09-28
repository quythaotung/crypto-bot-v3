import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết lý do Reject/Pass ra Render
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Turnover 24h tối thiểu $5M USD

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit 3-Engine Long Bot (Health Score Filter) Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def get_current_price(symbol):
    try:
        url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0]["lastPrice"])
    except Exception:
        pass
    return None

def process_telegram_commands():
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10"
            res = requests.get(url, timeout=12).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip().lower()
                    
                    if text in ["/pnl", "/summary", "pnl"]:
                        send_pnl_summary()
        except Exception:
            pass
        time.sleep(2)

def send_pnl_summary():
    if not signals_history:
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\nChưa có lệnh nào được phát ra từ khi khởi động."
    else:
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH (3-ENGINE SETUP):</b>\n-------------------------------------\n"
        total_pnl_pct = 0.0
        
        for sig in signals_history:
            curr_price = get_current_price(sig['symbol'])
            if not curr_price:
                curr_price = sig['entry']
                
            pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
            
            status = "⏳ Đang chạy"
            if curr_price >= sig['tp2']:
                status = "🚀 Hit TP2"
                pnl_pct = ((sig['tp2'] - sig['entry']) / sig['entry']) * 100
            elif curr_price >= sig['tp1']:
                status = "🎯 Hit TP1"
            elif curr_price <= sig['sl']:
                status = "🛑 Hit SL"
                pnl_pct = ((sig['sl'] - sig['entry']) / sig['entry']) * 100
                
            total_pnl_pct += pnl_pct
            icon = "🟢" if pnl_pct >= 0 else "🔴"
            
            msg += f"<b>#{sig['id']} [{sig['setup_code']}] {sig['symbol']}</b> | Entry: {sig['entry']:.4f}\n"
            msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
            msg += f"• Health: {sig['health_score']} | SetupScore: {sig['setup_score']}\n"
            msg += f"• Trạng thái: <b>{status}</b>\n-------------------------------------\n"
            
        total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
        msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi PnL: {e}", flush=True)

# ==============================================================================
# 2. BYBIT REST API & INDICATORS HELPERS
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            symbols = [
                t['symbol'] for t in tickers 
                if t['symbol'].endswith('USDT') 
                and not t['symbol'].startswith(('USDC', 'BUSD', 'DAI'))
                and not any(x in t['symbol'] for x in ['3L', '3S', '2L', '2S'])
            ]
            return symbols
    except Exception:
        pass
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

def get_closed_klines(symbol, interval, limit=50):
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            closed_candles = raw_list[1:]
            parsed = []
            for c in reversed(closed_candles):
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

def calculate_rsi(candles, period=14):
    if len(candles) < period + 1: return []
    closes = [c['close'] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i-1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    
    rsi_values = []
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    rsi_values.append(100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss))))
        
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rsi_values.append(100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss))))
    return rsi_values

def calculate_ema(candles, period):
    closes = [c['close'] for c in candles]
    if len(closes) < period: return 0.0
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price * k) + (ema * (1 - k))
    return ema

def calculate_m15_atr(symbol, period=14):
    m15_candles = get_closed_klines(symbol, "15", limit=period+5)
    if len(m15_candles) < period: return 0.0
    tr_list = []
    for i in range(1, len(m15_candles)):
        high = m15_candles[i]['high']
        low = m15_candles[i]['low']
        prev_close = m15_candles[i-1]['close']
        tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0

def get_open_interest_data(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                pct = ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0
                return curr_oi, prev_oi, pct
    except Exception:
        pass
    return None, None, 0.0

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def get_taker_buy_data(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) >= 2:
                curr_buy = float(data[0].get("buyRatio", 0))
                prev_buy = float(data[1].get("buyRatio", 0))
                return curr_buy, prev_buy
    except Exception:
        pass
    return None, None

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 3. BỘ LỌC COIN KHỎE (HEALTH SCORE - 100 ĐIỂM)
# ==============================================================================
def calculate_health_score(symbol, h1_candles, h4_candles, btc_h1_candles):
    score = 0
    
    if not h1_candles or not h4_candles or not btc_h1_candles:
        return 0

    # 1. Relative Strength so với BTC (25đ)
    symbol_chg = (h1_candles[-1]['close'] - h1_candles[-6]['close']) / h1_candles[-6]['close']
    btc_chg = (btc_h1_candles[-1]['close'] - btc_h1_candles[-6]['close']) / btc_h1_candles[-6]['close']
    if symbol_chg > btc_chg + 0.01: score += 25
    elif symbol_chg > btc_chg: score += 18
    elif symbol_chg >= btc_chg - 0.005: score += 10

    # 2. H1 Trend + EMA34/EMA89 (20đ)
    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)
    if h1_candles[-1]['close'] > ema34_h1 > ema89_h1: score += 20
    elif ema34_h1 > ema89_h1: score += 12
    elif h1_candles[-1]['close'] > ema34_h1: score += 8

    # 3. H4 Trend (15đ)
    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if h4_candles[-1]['close'] > ema34_h4 > ema89_h4: score += 15
    elif h4_candles[-1]['close'] > ema34_h4: score += 9

    # 4. Giữ giá tốt khi BTC điều chỉnh (15đ)
    if btc_chg < -0.005 and symbol_chg > -0.002: score += 15
    elif symbol_chg >= 0: score += 10

    # 5. Volume/Liquidity 24h (10đ)
    turnover = get_24h_turnover(symbol)
    if turnover >= 50_000_000: score += 10
    elif turnover >= 20_000_000: score += 8
    elif turnover >= 5_000_000: score += 5

    # 6. OI Structure (10đ)
    _, _, oi_pct = get_open_interest_data(symbol, "1h")
    if oi_pct >= 2.0: score += 10
    elif oi_pct >= 0.5: score += 6

    # 7. Khoảng cách an toàn tới EMA34 (5đ)
    if abs(h1_candles[-1]['close'] - ema34_h1) / ema34_h1 <= 0.02: score += 5

    return min(score, 100)

# ==============================================================================
# 4. ENGINE SETUP DISPATCHER (LONG A, LONG B, LONG C)
# ==============================================================================

# --- SETUP 1: LONG A - H1 LIQUIDITY SWEEP ---
def check_long_a_sweep(symbol, health_score, h1_candles):
    if health_score < 70: return None
    
    last = h1_candles[-1]
    prev_candles = h1_candles[-16:-1]
    swing_low = min(c['low'] for c in prev_candles)
    swing_high = max(c['high'] for c in prev_candles)

    # Sweep Condition
    if not (last['low'] < swing_low and last['close'] > swing_low):
        return None

    h1_range = last['high'] - last['low']
    if h1_range == 0: return None
    
    lower_wick = min(last['open'], last['close']) - last['low']
    wick_ratio = lower_wick / h1_range
    close_loc = (last['close'] - last['low']) / h1_range

    if wick_ratio < 0.35 or close_loc < 0.60: return None

    # RSI Bullish Divergence
    rsi_vals = calculate_rsi(h1_candles, 14)
    has_rsi_div = False
    if len(rsi_vals) >= 15:
        past_rsi_min = min(rsi_vals[-12:-3])
        if last['low'] < swing_low and rsi_vals[-1] > past_rsi_min and past_rsi_min <= 45:
            has_rsi_div = True

    # Volume & CVD
    avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol if avg_vol > 0 else 0
    if vol_ratio < 1.3: return None

    curr_buy, prev_buy = get_taker_buy_data(symbol)
    taker_pct = (curr_buy * 100) if curr_buy is not None else 55.0
    cvd_ok = (curr_buy is not None and prev_buy is not None and curr_buy >= prev_buy) or taker_pct >= 55.0
    if not cvd_ok: return None

    # Anti-Pump Check
    if (last['close'] - swing_low) / swing_low > 0.025: return None

    # Calculate Score A
    setup_score = 40
    if has_rsi_div: setup_score += 25
    if vol_ratio >= 1.5: setup_score += 15
    if taker_pct >= 60: setup_score += 10
    if health_score >= 85: setup_score += 10
    setup_score = min(setup_score, 100)

    atr15 = calculate_m15_atr(symbol)
    entry = last['close']
    sl = last['low'] - (atr15 * 0.3) if atr15 > 0 else last['low'] * 0.997
    risk = entry - sl
    if risk <= 0: return None

    return {
        'setup_code': 'LONG A',
        'title': 'H1 LIQUIDITY SWEEP',
        'health_score': health_score,
        'setup_score': setup_score,
        'entry': entry,
        'sl': sl,
        'tp1': entry + (risk * 1.5),
        'tp2': max(entry + (risk * 2.8), swing_high),
        'reasons': [
            f"Sweep đáy Swing Low ({swing_low:.4f}) & Reclaim đẹp",
            f"Râu dưới {wick_ratio*100:.1f}%, Đóng cửa {close_loc*100:.1f}% đỉnh",
            f"RSI Bullish Divergence" if has_rsi_div else "RSI hỗ trợ phục hồi",
            f"Volume +{vol_ratio:.2f}x, CVD Taker Buy {taker_pct:.1f}%"
        ],
        'signal_id': f"LONGA_{symbol}_{last['timestamp']}"
    }

# --- SETUP 2: LONG B - H1 SUPPORT / EMA BOUNCE ---
def check_long_b_support(symbol, health_score, h1_candles):
    if health_score < 75: return None

    last = h1_candles[-1]
    prev_candles = h1_candles[-15:-1]
    ema34 = calculate_ema(h1_candles, 34)
    ema89 = calculate_ema(h1_candles, 89)
    
    # Touch or Pullback near Support/EMA
    near_ema = (abs(last['low'] - ema34) / ema34 <= 0.01) or (abs(last['low'] - ema89) / ema89 <= 0.01)
    higher_low = (last['low'] >= min(c['low'] for c in prev_candles[-3:]))
    
    if not (near_ema or higher_low): return None

    h1_range = last['high'] - last['low']
    if h1_range == 0: return None
    close_loc = (last['close'] - last['low']) / h1_range
    if close_loc < 0.55: return None

    rsi_vals = calculate_rsi(h1_candles, 14)
    if not rsi_vals or rsi_vals[-1] > 55: return None

    avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol if avg_vol > 0 else 0

    curr_buy, _ = get_taker_buy_data(symbol)
    taker_pct = (curr_buy * 100) if curr_buy is not None else 52.0
    if taker_pct < 50.0: return None

    setup_score = 50
    if ema34 > ema89: setup_score += 20
    if near_ema: setup_score += 15
    if rsi_vals[-1] <= 45: setup_score += 15
    setup_score = min(setup_score, 100)

    atr15 = calculate_m15_atr(symbol)
    entry = last['close']
    sl = min(last['low'], ema34 if near_ema else last['low']) - (atr15 * 0.3)
    risk = entry - sl
    if risk <= 0: return None

    return {
        'setup_code': 'LONG B',
        'title': 'H1 SUPPORT / EMA BOUNCE',
        'health_score': health_score,
        'setup_score': setup_score,
        'entry': entry,
        'sl': sl,
        'tp1': entry + (risk * 1.5),
        'tp2': entry + (risk * 2.5),
        'reasons': [
            f"Coin khỏe (Health: {health_score}) Pullback về Support/EMA",
            f"Tạo Higher Low/Rejection tại cản động EMA34 ({ema34:.4f})",
            f"RSI Recovery ({rsi_vals[-1]:.1f} <= 55), CVD ổn định ({taker_pct:.1f}%)"
        ],
        'signal_id': f"LONGB_{symbol}_{last['timestamp']}"
    }

# --- SETUP 3: LONG C - H1 BREAKOUT + RETEST ---
def check_long_c_breakout(symbol, health_score, h1_candles):
    if health_score < 80: return None

    last = h1_candles[-1]
    prev_candles = h1_candles[-20:-2]
    swing_high = max(c['high'] for c in prev_candles)

    # Breakout or Retest Check
    is_breakout = (last['close'] > swing_high)
    is_retest = (h1_candles[-2]['close'] > swing_high and last['low'] <= swing_high and last['close'] >= swing_high)

    if not (is_breakout or is_retest): return None

    avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol if avg_vol > 0 else 0
    if vol_ratio < 1.4: return None

    _, _, oi_pct = get_open_interest_data(symbol, "1h")
    if oi_pct < 1.5: return None

    curr_buy, _ = get_taker_buy_data(symbol)
    taker_pct = (curr_buy * 100) if curr_buy is not None else 58.0
    if taker_pct < 55.0: return None

    rsi_vals = calculate_rsi(h1_candles, 14)
    if not rsi_vals or rsi_vals[-1] > 70: return None # Tránh mua khi RSI quá nóng (>70)

    setup_score = 60
    if is_retest: setup_score += 20
    if vol_ratio >= 1.8: setup_score += 10
    if oi_pct >= 3.0: setup_score += 10
    setup_score = min(setup_score, 100)

    atr15 = calculate_m15_atr(symbol)
    entry = last['close']
    sl = swing_high - (atr15 * 0.4) if atr15 > 0 else swing_high * 0.995
    risk = entry - sl
    if risk <= 0: return None

    return {
        'setup_code': 'LONG C',
        'title': 'H1 BREAKOUT + RETEST',
        'health_score': health_score,
        'setup_score': setup_score,
        'entry': entry,
        'sl': sl,
        'tp1': entry + (risk * 1.5),
        'tp2': entry + (risk * 2.8),
        'reasons': [
            f"Phá vỡ / Retest giữ vững cản Swing High ({swing_high:.4f})",
            f"Volume Breakout +{vol_ratio:.2f}x, OI tăng +{oi_pct:.2f}%",
            f"CVD Taker Buy {taker_pct:.1f}% xác nhận xu hướng, RSI chưa quá nóng ({rsi_vals[-1]:.1f})"
        ],
        'signal_id': f"LONGC_{symbol}_{last['timestamp']}"
    }

# ==============================================================================
# 5. PIPELINE PHÂN TÍCH VÀ GỬI TIN NHẮN
# ==============================================================================
def analyze_symbol_engine(symbol, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: return None

    h1_candles = get_closed_klines(symbol, "60", limit=50)
    h4_candles = get_closed_klines(symbol, "240", limit=50)
    
    if len(h1_candles) < 35 or len(h4_candles) < 35: return None

    # TÍNH HEALTH SCORE
    health_score = calculate_health_score(symbol, h1_candles, h4_candles, btc_h1_candles)

    # ROUTING 3 SETUP (ƯU TIÊN: LONG A -> LONG B -> LONG C)
    sig_a = check_long_a_sweep(symbol, health_score, h1_candles)
    if sig_a: return sig_a

    sig_b = check_long_b_support(symbol, health_score, h1_candles)
    if sig_b: return sig_b

    sig_c = check_long_c_breakout(symbol, health_score, h1_candles)
    if sig_c: return sig_c

    return None

def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    risk_pct = ((sig['entry'] - sig['sl']) / sig['entry']) * 100

    reasons_fmt = "\n".join([f"• {r}" for r in sig['reasons']])

    msg = f"""🚀 <b>[{sig['setup_code']} — {sig['title']}] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
🛡 <b>Health Score: {sig['health_score']}/100</b>
⭐️ <b>Setup Score: {sig['setup_score']}/100</b>

🔥 <b>LÝ DO BẮT TÍN HIỆU:</b>
{reasons_fmt}
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH:</b>

📍 <b>Entry:</b> {sig['entry']:.4f}
🛑 <b>Stop Loss:</b> {sig['sl']:.4f} (-{risk_pct:.2f}%)
🎯 <b>Take Profit 1:</b> {sig['tp1']:.4f} (1.5R)
🚀 <b>Take Profit 2:</b> {sig['tp2']:.4f} (2.5R+)
-------------------------------------
👉 <b>CHECK CHART KHUNG H1 NGAY!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['setup_code']}] cho {sig['symbol']}", flush=True)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# 6. MAIN LOOP
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Engine 3-Setup (LONG A / B / C)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=50)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang quét Health Score & 3-Setup cho {len(all_symbols)} coin...", flush=True)
        
        for symbol in all_symbols:
            try:
                sig = analyze_symbol_engine(symbol, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
            except Exception:
                pass
            time.sleep(0.15)
            
        print(f"✅ Hoàn thành chu kỳ quét. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
