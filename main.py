import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết ra Render
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Turnover 24h tối thiểu $5M USD
MAX_ALERTS_PER_CYCLE = 4           # Tối đa gửi Top 4 coin điểm cao nhất mỗi vòng quét

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
    return "Bybit Long H1 Pre-Pump Base Engine Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH (PRE-PUMP BASE):</b>\n-------------------------------------\n"
        total_pnl_pct = 0.0
        
        for sig in signals_history:
            curr_price = get_current_price(sig['symbol'])
            if not curr_price:
                curr_price = sig['entry_mid']
                
            pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
            
            status = "⏳ Đang chạy"
            if curr_price >= sig['tp2']:
                status = "🚀 Hit TP2"
                pnl_pct = ((sig['tp2'] - sig['entry_mid']) / sig['entry_mid']) * 100
            elif curr_price >= sig['tp1']:
                status = "🎯 Hit TP1"
            elif curr_price <= sig['sl']:
                status = "🛑 Hit SL"
                pnl_pct = ((sig['sl'] - sig['entry_mid']) / sig['entry_mid']) * 100
                
            total_pnl_pct += pnl_pct
            icon = "🟢" if pnl_pct >= 0 else "🔴"
            
            msg += f"<b>#{sig['id']} [{sig['label']}] {sig['symbol']}</b> | Entry: {sig['entry_mid']:.4f}\n"
            msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
            msg += f"• Total Score: {sig['total_score']}/100 | Status: <b>{status}</b>\n-------------------------------------\n"
            
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

def get_open_interest_series(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=5"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 3:
                return [float(x["openInterest"]) for x in reversed(oi_list)]
    except Exception:
        pass
    return []

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def get_taker_buy_series(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit=4"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) >= 3:
                return [float(x.get("buyRatio", 0.5)) * 100 for x in reversed(data)]
    except Exception:
        pass
    return []

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 3. CORE STRATEGY: LONG H1 PRE-PUMP BASE (SCORE 100)
# ==============================================================================

def find_support_zone(h1_candles):
    if len(h1_candles) < 25: return None, None, 0
    recent_lows = [c['low'] for c in h1_candles[-20:]]
    recent_lows.sort()
    
    supp_low = recent_lows[0]
    supp_high = recent_lows[3]
    
    # Đếm số lần giá phản ứng quanh vùng Hỗ trợ
    touch_count = sum(1 for c in h1_candles[-25:] if supp_low * 0.997 <= c['low'] <= supp_high * 1.008)
    return supp_low, supp_high, touch_count

def evaluate_prepump_base(symbol, h1_candles, h4_candles, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: return None
    if len(h1_candles) < 35 or len(h4_candles) < 35: return None

    supp_low, supp_high, touch_count = find_support_zone(h1_candles)
    if not supp_low or touch_count < 1: return None

    last = h1_candles[-1]
    curr_price = last['close']

    # 1. BẮT BUỘC: GIÁ ĐANG Ở GẦN VÙNG NỀN HỖ TRỢ (Distance <= 2.0%)
    dist_to_supp = (curr_price - supp_high) / supp_high if curr_price >= supp_high else 0.0
    if dist_to_supp > 0.020 or curr_price < supp_low * 0.993:
        return None # Đã pump quá xa khỏi Support Zone hoặc sập gãy hỗ trợ

    # --- CHẤM ĐIỂM THANG 100 ĐIỂM ---

    # 1. Support Structure (Max 20đ)
    score_support = 20 if touch_count >= 2 else 12

    # 2. Accumulation / Compression (Max 15đ)
    # Biên độ dao động nén thu hẹp + Không liên tục tạo Lower Low
    range_last = last['high'] - last['low']
    range_avg = sum(c['high'] - c['low'] for c in h1_candles[-10:-1]) / 9
    is_compressed = range_last <= range_avg * 1.1
    higher_low = last['low'] >= min(c['low'] for c in h1_candles[-4:-1])
    
    score_accum = 0
    if is_compressed: score_accum += 8
    if higher_low: score_accum += 7

    # 3. Price Action / Reclaim (Max 15đ)
    h1_range = last['high'] - last['low']
    wick_ratio = (min(last['open'], last['close']) - last['low']) / h1_range if h1_range > 0 else 0
    is_sweep = last['low'] < supp_low and last['close'] > supp_low

    score_pa = 0
    if is_sweep: score_pa = 15
    elif wick_ratio >= 0.35: score_pa = 12
    elif curr_price >= supp_high: score_pa = 10
    else: score_pa = 6

    # 4. OI Structure (Max 15đ) - Phân tích theo tiến trình Giai Đoạn
    oi_series = get_open_interest_series(symbol, "1h")
    score_oi = 0
    if len(oi_series) >= 3:
        oi_prev2, oi_prev1, oi_curr = oi_series[-3], oi_series[-2], oi_series[-1]
        # Giai đoạn 3: OI bắt đầu tăng lại từ nền tích lũy
        if oi_curr > oi_prev1 and oi_prev1 >= oi_prev2 * 0.99:
            score_oi = 15
        elif oi_curr >= oi_prev1: # Giai đoạn 2: OI ổn định/ngừng giảm
            score_oi = 10
        else:
            score_oi = 5
    else:
        score_oi = 8

    # 5. CVD Structure (Max 10đ)
    taker_series = get_taker_buy_series(symbol)
    score_cvd = 0
    if len(taker_series) >= 2:
        cvd_prev, cvd_curr = taker_series[-2], taker_series[-1]
        if cvd_curr > cvd_prev and cvd_curr >= 50.0: # CVD tạo Higher Low / tăng
            score_cvd = 10
        elif cvd_curr >= 50.0:
            score_cvd = 7
        else:
            score_cvd = 4
    else:
        score_cvd = 6

    # 6. Volume (Max 10đ)
    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    # Volume cạn kiệt trong nền vẫn được điểm, volume nhích tăng khi Reclaim điểm cao nhất
    if 1.2 <= vol_ratio <= 2.2: score_vol = 10 # Volume vừa chớm bùng nổ đẹp
    elif vol_ratio < 1.0: score_vol = 7        # Volume cạn tích lũy nén
    else: score_vol = 5                        # Volume đã quá cao (có thể đã pump xa)

    # 7. RSI (Max 5đ) - Ưu tiên 35 đến 55
    rsi_vals = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_vals[-1] if rsi_vals else 50.0
    score_rsi = 5 if 35.0 <= curr_rsi <= 55.0 else (3 if curr_rsi < 35 else 2)

    # 8. Relative Strength vs BTC (Max 5đ)
    sym_chg_6h = (h1_candles[-1]['close'] - h1_candles[-6]['close']) / h1_candles[-6]['close']
    btc_chg_6h = (btc_h1_candles[-1]['close'] - btc_h1_candles[-6]['close']) / btc_h1_candles[-6]['close'] if btc_h1_candles else 0.0
    score_rs = 5 if sym_chg_6h >= btc_chg_6h else 2

    # 9. BTC Regime (Max 5đ)
    score_btc = 5 if btc_chg_6h >= -0.008 else 2

    total_score = score_support + score_accum + score_pa + score_oi + score_cvd + score_vol + score_rsi + score_rs + score_btc
    total_score = min(total_score, 100)

    if total_score < 68: # REJECT
        return None

    # TÍNH VÙNG ENTRY ZONE & SL/TP CẤU TRÚC
    atr15 = calculate_m15_atr(symbol)
    entry_low = supp_low
    entry_high = min(curr_price, supp_high * 1.008)
    entry_mid = (entry_low + entry_high) / 2

    # SL dưới đáy Support Zone / Sweep Low
    sl = supp_low - (atr15 * 0.3) if atr15 > 0 else supp_low * 0.995
    risk = entry_mid - sl
    if risk <= 0: return None

    # TP Multi-Tier
    prev_highs = [c['high'] for c in h1_candles[-25:]]
    swing_high = max(prev_highs)
    
    tp1 = entry_mid + (risk * 1.5)
    tp2 = max(entry_mid + (risk * 2.5), swing_high)
    tp3 = entry_mid + (risk * 3.8)

    if total_score >= 85:
        status_code = "PRE_PUMP_BASE"
        status_label = "🚀 LONG H1 PRE-PUMP BASE"
    elif total_score >= 75:
        status_code = "LONG_SETUP"
        status_label = "🟢 LONG SETUP"
    else:
        status_code = "WATCH"
        status_label = "👀 WATCH"

    return {
        'status_code': status_code,
        'label': status_label,
        'symbol': symbol,
        'total_score': total_score,
        'vol_ratio': vol_ratio,
        'oi_pct': ((oi_series[-1] - oi_series[-2])/oi_series[-2]*100) if len(oi_series)>=2 else 0.0,
        'taker_pct': taker_series[-1] if taker_series else 50.0,
        'supp_low': supp_low,
        'supp_high': supp_high,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'entry_mid': entry_mid,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'tp3': tp3,
        'risk_pct': (risk / entry_mid) * 100,
        'signal_id': f"{status_code}_{symbol}_{last['timestamp']}"
    }

# ==============================================================================
# 4. FORMATTER TELEGRAM chuẩn FORMAT PRE-PUMP BASE
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""{sig['label']} #{sig['id']} - <b>{sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['total_score']}/100</b>
🧱 Support Zone: <b>{sig['supp_low']:.4f} – {sig['supp_high']:.4f}</b>

🔥 <b>DẤU HIỆU DÒNG TIỀN & SHORT SQUEEZE:</b>
• Volume H1: <b>+{sig['vol_ratio']:.2f}x (SMA20)</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}% (Bắt đầu tăng/ngừng giảm)</b>
• Lực Mua CVD: <b>{sig['taker_pct']:.1f}% (CVD tạo đáy & hồi phục)</b>
• Trạng Thái: <b>Giữ vững nền Hỗ Trợ trước Breakout</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>

📍 <b>Entry Zone:</b>
{sig['entry_low']:.4f} – {sig['entry_high']:.4f}

🛑 <b>Stop Loss:</b>
{sig['sl']:.4f} (Dưới đáy Support Zone: -{sig['risk_pct']:.2f}%)

🎯 <b>Take Profit 1:</b>
{sig['tp1']:.4f} (Kháng cự gần / 1.5R)

🚀 <b>Take Profit 2:</b>
{sig['tp2']:.4f} (Swing High / 2.5R+)

🔥 <b>Take Profit 3:</b>
{sig['tp3']:.4f} (Expansion Momentum / 3.8R)
-------------------------------------
👉 <b>CHECK CHART H1 & BẮT SÓNG NGAY!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['label']}] cho {sig['symbol']} (Score: {sig['total_score']})", flush=True)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# 5. MAIN LOOP RUNNER CHỐNG SPAM (TOP 3-5 COIN/VÒNG QUÉT)
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Long H1 Pre-Pump Base (Chống Spam Top 4 Coin)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=50)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang quét thang điểm Pre-Pump Base 100 cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                h1_candles = get_closed_klines(symbol, "60", limit=50)
                h4_candles = get_closed_klines(symbol, "240", limit=50)
                
                sig = evaluate_prepump_base(symbol, h1_candles, h4_candles, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.12)

        # LỌC TOP 4 COIN ĐIỂM CAO NHẤT ĐỂ BẮN CẢNH BÁO
        if candidates:
            candidates.sort(key=lambda x: (x['total_score'], x['oi_pct'], x['vol_ratio']), reverse=True)
            top_candidates = candidates[:MAX_ALERTS_PER_CYCLE]

            for c in top_candidates:
                processed_signal_ids.add(c['signal_id'])
                send_telegram_signal(c)
            
        print(f"✅ Hoàn thành 1 vòng quét. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
