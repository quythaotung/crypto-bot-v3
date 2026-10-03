import time
import requests
import os
import numpy as np
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - STRATEGY: h1-spot-squeeze-precision-v2
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 15_000_000  # $15M USDT

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                
processed_fingerprints = set()
symbol_cooldowns = {}              
daily_call_count = 0
last_call_reset_time = time.time()
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot H1 Spot-Squeeze Precision Active 24/7!"

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

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# DATA FETCHING & CVD DIVERGENCE CALCULATOR
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            excluded = ['3L', '3S', '2L', '2S', 'QQQ', 'SPCX', 'SPY']
            parsed_list = []
            for t in tickers:
                sym = t['symbol']
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD')) and not any(x in sym for x in excluded):
                    parsed_list.append({
                        'symbol': sym,
                        'turnover24h': float(t.get('turnover24h', 0)),
                        'fundingRate': float(t.get('fundingRate', 0)) * 100,
                        'lastPrice': float(t.get('lastPrice', 0))
                    })
            return parsed_list
    except Exception:
        pass
    return []

def get_closed_klines(symbol, interval, category="linear", limit=100):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
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
                    'volume': float(c[5]),
                    'turnover': float(c[6])
                })
            return parsed
    except Exception:
        pass
    return []

def get_taker_ratios(symbol, category="linear", limit=6):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period=1h&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def calculate_atr(candles, period=14):
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low = candles[i]['low']
        prev_close = candles[i-1]['close']
        tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0

def get_open_interest_pct(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

# ==============================================================================
# CORE EVALUATOR: SPOT ACCUMULATION & SHORT SQUEEZE (MÔ HÌNH QNT / ZRO)
# ==============================================================================
def evaluate_spot_squeeze_setup(ticker_data):
    symbol = ticker_data['symbol']
    fut_turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    if fut_turnover < MIN_FUTURES_TURNOVER_24H: return None
    if funding > 0.05 or funding < -0.05: return None

    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 21600: return None

    h1_candles = get_closed_klines(symbol, "60", category="linear", limit=60)
    spot_h1_candles = get_closed_klines(symbol, "60", category="spot", limit=60)

    if not h1_candles or len(h1_candles) < 30: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_atr = calculate_atr(h1_candles, 14)

    # 1. KIỂM TRA PHÂN KỲ SPOT CVD vs PERP CVD (ZRO STYLE)
    spot_taker = get_taker_ratios(symbol, category="spot", limit=5)
    perp_taker = get_taker_ratios(symbol, category="linear", limit=5)

    spot_accumulating = sum(1 for r in spot_taker[:3] if r >= 53.0) >= 2
    oi_h1_pct = get_open_interest_pct(symbol, "1h")

    # 2. XÁC ĐỊNH NỀN TÍCH LŨY BASE H1 (4-15 NẾN)
    recent_lows = [c['low'] for c in h1_candles[-12:-1]]
    recent_highs = [c['high'] for c in h1_candles[-12:-1]]
    
    base_low = min(recent_lows)
    base_high = max(recent_highs)
    base_range_pct = (base_high - base_low) / base_low * 100

    # Nếu nền đi ngang quá rộng (> 5.5%), không phải tích lũy nén giá
    if base_range_pct > 5.5: return None

    # 3. ĐIỀU KIỆN KÍCH HOẠT TRIGGER (BREAKOUT HOẶC HOLDING SUPPORT RECLAIM)
    is_breakout = last_h1['close'] > base_high
    is_holding = (curr_price >= base_low + (base_high - base_low)*0.4) and last_h1['close'] > last_h1['open']

    if not (is_breakout or is_holding): return None

    # 4. TÍNH STOP LOSS VÀ TAKE PROFIT THEO CẤU TRÚC
    sl_price = base_low - (0.2 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 3.0: return None # Loại bỏ các lệnh rủi ro SL > 3%

    tp1_price = curr_price + (h1_atr * 2.5)
    tp2_price = curr_price + (h1_atr * 4.0)

    tp1_pct = (tp1_price - curr_price) / curr_price * 100
    if tp1_pct / risk_pct < 1.5: return None # Tỷ lệ R:R tối thiểu 1.5R

    # Chấm điểm uy tín Setup (0 - 100)
    score = 70
    if spot_accumulating: score += 15
    if oi_h1_pct >= 1.5: score += 10
    if is_breakout: score += 5

    if score < 80: return None

    fingerprint = f"{symbol}_{last_h1['timestamp']}_SPOT_SQUEEZE"
    if fingerprint in processed_fingerprints: return None

    processed_fingerprints.add(fingerprint)
    symbol_cooldowns[symbol] = now

    return {
        'symbol': symbol,
        'entry': curr_price,
        'entry_low': base_high if is_breakout else base_low,
        'entry_high': curr_price,
        'sl': sl_price,
        'risk_pct': risk_pct,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'score': score,
        'spot_taker': spot_taker[0],
        'perp_taker': perp_taker[0],
        'oi_pct': oi_h1_pct,
        'funding': funding,
        'base_range': base_range_pct
    }

def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""🔥 <b>[SPOT ACCUMULATION & SHORT SQUEEZE] #{sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
⭐️ <b>Điểm Uy Tín:</b> <code>{sig['score']}/100</code>

📊 <b>DÒNG TIỀN NÉN GIÁ (ZRO/QNT STYLE):</b>
• Spot Taker Buy: <code>{sig['spot_taker']:.1f}%</code> (Cá mập mua gom)
• Perp Taker Buy: <code>{sig['perp_taker']:.1f}%</code>
• OI H1 Expansion: <code>{sig['oi_pct']:+.2f}%</code>
• Biên độ nền nén H1: <code>{sig['base_range']:.2f}%</code>
• Funding Rate: <code>{sig['funding']:+.3f}%</code>

🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>
📍 Entry Zone: <code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>
🛑 Stop Loss: <code>{sig['sl']:.4f}</code> (-{sig['risk_pct']:.2f}%)
🎯 Take Profit 1: <code>{sig['tp1']:.4f}</code> (+{(sig['tp1']-sig['entry'])/sig['entry']*100:.1f}%)
🚀 Take Profit 2: <code>{sig['tp2']:.4f}</code> (+{(sig['tp2']-sig['entry'])/sig['entry']*100:.1f}%)

⚠️ <i>Vào lệnh sát vùng Base. Không Mua Đuổi nếu giá đã vọt cao.</i>"""

    send_telegram_direct_message(msg)

def run_radar_loop():
    print("⚡️ Bắt đầu Radar Spot Squeeze Precision Scanner...", flush=True)
    while True:
        all_tickers = get_all_bybit_usdt_symbols()
        for ticker in all_tickers:
            try:
                sig = evaluate_spot_squeeze_setup(ticker)
                if sig:
                    send_telegram_signal(sig)
            except Exception:
                pass
            time.sleep(0.05)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_radar_loop()
