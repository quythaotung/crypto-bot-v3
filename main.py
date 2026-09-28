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
MAX_ALERTS_PER_CYCLE = 4           # Tối đa gửi Top 4 coin đẹp nhất mỗi vòng quét

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
    return "Bybit Multi-Setup Squeeze & Reclaim Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH (SCORE SYSTEM):</b>\n-------------------------------------\n"
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
            
            msg += f"<b>#{sig['id']} [{sig['setup_name']}] {sig['symbol']}</b> | Entry: {sig['entry_mid']:.4f}\n"
            msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
            msg += f"• Score: {sig['total_score']}/100 (Health:{sig['health_score']}) | Status: <b>{status}</b>\n-------------------------------------\n"
            
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
# 3. THANG ĐIỂM TỔNG HỢP (HEALTH SCORE 40đ + SETUP SCORE 60đ = 100đ)
# ==============================================================================

def evaluate_health_score(symbol, h1_candles, h4_candles, btc_h1_candles):
    """
    HEALTH SCORE (MAX 40 ĐIỂM)
    1. Relative Strength vs BTC: 10đ
    2. H1 Trend: 10đ
    3. H4 Trend: 10đ
    4. Giữ giá khi BTC yếu: 10đ
    """
    health_score = 0
    if not h1_candles or not h4_candles or not btc_h1_candles: return 0

    # Relative Strength
    sym_chg_6h = (h1_candles[-1]['close'] - h1_candles[-6]['close']) / h1_candles[-6]['close']
    btc_chg_6h = (btc_h1_candles[-1]['close'] - btc_h1_candles[-6]['close']) / btc_h1_candles[-6]['close']
    
    if sym_chg_6h > btc_chg_6h + 0.01: health_score += 10
    elif sym_chg_6h > btc_chg_6h: health_score += 7
    elif sym_chg_6h >= btc_chg_6h - 0.005: health_score += 4

    # H1 Trend (EMA34/89)
    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)
    if h1_candles[-1]['close'] >= ema34_h1 >= ema89_h1: health_score += 10
    elif ema34_h1 >= ema89_h1: health_score += 7
    elif h1_candles[-1]['close'] >= ema34_h1: health_score += 5

    # H4 Trend (EMA34/89)
    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if h4_candles[-1]['close'] >= ema34_h4 >= ema89_h4: health_score += 10
    elif h4_candles[-1]['close'] >= ema34_h4: health_score += 6

    # Hold price when BTC weak
    if btc_chg_6h < -0.005 and sym_chg_6h > -0.002: health_score += 10
    elif sym_chg_6h >= 0: health_score += 6

    return min(health_score, 40)

def evaluate_symbol_setups(symbol, h1_candles, h4_candles, btc_h1_candles):
    """
    ĐÁNH GIÁ 3 DẠNG SETUP VÀ TÍNH SETUP SCORE (MAX 60 ĐIỂM)
    - Volume: 10đ
    - OI: 10đ
    - CVD / Taker Buy: 10đ
    - Structure / Breakout / Reclaim: 10đ
    - RSI: 10đ
    - Liquidity / Squeeze: 10đ
    """
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: return None
    if len(h1_candles) < 35 or len(h4_candles) < 35: return None

    health_score = evaluate_health_score(symbol, h1_candles, h4_candles, btc_h1_candles)
    if health_score < 20: # Lọc coin quá yếu (< 20/40)
        return None

    last = h1_candles[-1]
    prev_candles = h1_candles[-20:-1]
    swing_low = min(c['low'] for c in prev_candles)
    swing_high = max(c['high'] for c in prev_candles)
    h1_range = last['high'] - last['low']
    if h1_range == 0: return None

    # Lấy chỉ số kĩ thuật
    avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol if avg_vol > 0 else 0
    
    _, _, oi_pct = get_open_interest_data(symbol, "1h")
    curr_buy, prev_buy = get_taker_buy_data(symbol)
    taker_pct = (curr_buy * 100) if curr_buy is not None else 50.0

    rsi_vals = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_vals[-1] if rsi_vals else 50.0

    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)

    # Đánh giá các điểm Setup
    vol_score = 10 if vol_ratio >= 2.0 else (8 if vol_ratio >= 1.5 else (5 if vol_ratio >= 1.2 else 2))
    oi_score = 10 if oi_pct >= 5.0 else (8 if oi_pct >= 2.0 else (5 if oi_pct >= 0.5 else 0))
    
    if taker_pct >= 80: cvd_score = 10
    elif taker_pct >= 65: cvd_score = 8
    elif taker_pct >= 55: cvd_score = 5
    else: cvd_score = 2

    # Nhận dạng 3 dạng Setup & Cấu trúc
    setup_name = ""
    setup_type_tag = ""
    struct_score = 0
    liq_score = 0
    rsi_score = 0

    lower_wick = min(last['open'], last['close']) - last['low']
    wick_ratio = lower_wick / h1_range

    # 1. SETUP LONG C: SQUEEZE EXPLOSION (MẪU QNT)
    if vol_ratio >= 1.5 and oi_pct >= 2.0 and taker_pct >= 65.0 and (last['close'] >= swing_high * 0.998):
        setup_name = "LONG H1 SQUEEZE EXPLOSION"
        setup_type_tag = "LONG C"
        struct_score = 10
        liq_score = 10
        rsi_score = 10 if 50 <= curr_rsi <= 68 else 6

    # 2. SETUP LONG A: LIQUIDITY SWEEP / REVERSAL
    elif (last['low'] < swing_low and last['close'] > swing_low) or (wick_ratio >= 0.35 and last['close'] > swing_low):
        setup_name = "LONG H1 LIQUIDITY SWEEP"
        setup_type_tag = "LONG A"
        struct_score = 10
        liq_score = 10
        past_rsi_min = min(rsi_vals[-12:-3]) if len(rsi_vals) >= 12 else 40
        rsi_score = 10 if (curr_rsi > past_rsi_min and past_rsi_min <= 45) else 7

    # 3. SETUP LONG B: SUPPORT RECLAIM / EMA BOUNCE
    elif (abs(last['low'] - ema34_h1)/ema34_h1 <= 0.012) or (abs(last['low'] - ema89_h1)/ema89_h1 <= 0.012) or (last['low'] >= min(c['low'] for c in prev_candles[-4:])):
        setup_name = "LONG H1 SUPPORT RECLAIM"
        setup_type_tag = "LONG B"
        struct_score = 8
        liq_score = 6
        rsi_score = 10 if curr_rsi <= 55 else 5

    else:
        return None # Không trùng setup nào

    setup_score = vol_score + oi_score + cvd_score + struct_score + rsi_score + liq_score
    setup_score = min(setup_score, 60)

    total_score = health_score + setup_score

    # Lọc điểm tối thiểu (>= 70 mới cân nhắc)
    if total_score < 70:
        return None

    # CHỐNG EXTENDED / PUMP CHASE: Bỏ qua nếu giá đã chạy quá +2% từ Breakout/Support
    if setup_type_tag == "LONG C" and (last['close'] - swing_high) / swing_high > 0.020:
        return None
    if setup_type_tag == "LONG A" and (last['close'] - swing_low) / swing_low > 0.025:
        return None

    # TÍNH ENTRY ZONE, SL VÀ TP CHUẨN CẤU TRÚC
    atr15 = calculate_m15_atr(symbol)
    
    if setup_type_tag == "LONG A":
        sl = last['low'] - (atr15 * 0.3) if atr15 > 0 else last['low'] * 0.997
        entry_low = swing_low
        entry_high = last['close']
    elif setup_type_tag == "LONG B":
        sl = min(last['low'], ema34_h1) - (atr15 * 0.3) if atr15 > 0 else last['low'] * 0.996
        entry_low = min(ema34_h1, last['low'])
        entry_high = last['close']
    else: # LONG C
        sl = swing_high - (atr15 * 0.4) if atr15 > 0 else swing_high * 0.995
        entry_low = swing_high
        entry_high = last['close']

    entry_mid = (entry_low + entry_high) / 2
    risk = entry_mid - sl
    if risk <= 0: return None

    tp1 = entry_mid + (risk * 1.5)
    tp2 = max(entry_mid + (risk * 2.5), swing_high * 1.01)

    signal_status = "LONG" if total_score >= 80 else "WATCH"

    return {
        'signal_status': signal_status,
        'setup_type_tag': setup_type_tag,
        'setup_name': setup_name,
        'symbol': symbol,
        'total_score': total_score,
        'health_score': health_score,
        'setup_score': setup_score,
        'vol_ratio': vol_ratio,
        'oi_pct': oi_pct,
        'taker_pct': taker_pct,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'entry_mid': entry_mid,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'risk_pct': (risk / entry_mid) * 100,
        'signal_id': f"{setup_type_tag}_{symbol}_{last['timestamp']}"
    }

# ==============================================================================
# 4. FORMATTER TIN NHẮN TELEGRAM chuẩn MẪU QNT
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    icon_head = "🚀" if sig['signal_status'] == "LONG" else "👀"
    status_label = "LONG" if sig['signal_status'] == "LONG" else "CẢNH BÁO WATCH"

    msg = f"""{icon_head} <b>[{sig['setup_name']}] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['total_score']}/100</b> ({status_label})
🛡 Health Score: {sig['health_score']}/40 | Setup Score: {sig['setup_score']}/60

🔥 <b>DẤU HIỆU DÒNG TIỀN & SHORT SQUEEZE:</b>
• Volume H1: <b>+{sig['vol_ratio']:.2f}x (SMA20)</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}% (Tiền bơm mạnh)</b>
• Lực Mua CVD: <b>{sig['taker_pct']:.1f}% (Taker Buy ăn giá)</b>
• Trạng Thái: <b>Cấu trúc H1 xác nhận đẹp</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH ({sig['signal_status']}):</b>

📍 <b>Entry Zone:</b>
{sig['entry_low']:.4f} – {sig['entry_high']:.4f}

🛑 <b>Stop Loss:</b>
{sig['sl']:.4f} (Dưới vùng cấu trúc: -{sig['risk_pct']:.2f}%)

🎯 <b>Take Profit 1:</b>
{sig['tp1']:.4f} (Kháng cự gần / 1.5R)

🚀 <b>Take Profit 2:</b>
{sig['tp2']:.4f} (Vùng Expansion / 2.5R+)
-------------------------------------
👉 <b>CHECK CHART H1 & BẮT SÓNG NGAY!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['setup_type_tag']}] cho {sig['symbol']} (Score: {sig['total_score']})", flush=True)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# 5. MAIN LOOP RUNNER VỚI LỌC CHỐNG SPAM (TOP 3-5 COIN)
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Squeeze Explosion & Reclaim (Chống Spam Top 4 Coin)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=50)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang quét thang điểm 100 cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                h1_candles = get_closed_klines(symbol, "60", limit=50)
                h4_candles = get_closed_klines(symbol, "240", limit=50)
                
                sig = evaluate_symbol_setups(symbol, h1_candles, h4_candles, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.12)

        # SẮP XẾP VÀ CHỌN TOP 4 COIN ĐIỂM CAO NHẤT ĐỂ BẮN (CHỐNG SPAM)
        if candidates:
            # Ưu tiên: Total Score cao -> Health Score cao -> Volume/OI cao
            candidates.sort(key=lambda x: (x['total_score'], x['health_score'], x['vol_ratio']), reverse=True)
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
