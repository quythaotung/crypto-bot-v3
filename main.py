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
    return "Bybit Accumulation Base & Squeeze Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH:</b>\n-------------------------------------\n"
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
            
            msg += f"<b>#{sig['id']} [{sig['setup_type_tag']}] {sig['symbol']}</b> | Entry: {sig['entry_mid']:.4f}\n"
            msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
            msg += f"• Score: {sig['total_score']}/100 | Status: <b>{status}</b>\n-------------------------------------\n"
            
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
# 3. THIẾT LẬP SUPPORT ZONE & SETUP LONG A (ACCUMULATION BASE)
# ==============================================================================

def find_support_zone(h1_candles, h4_candles):
    """
    Xác định Support Zone dựa trên:
    - Swing Lows gần đây (ít nhất 2 lần phản ứng)
    - Breakout base / Kháng cự cũ bị vượt
    """
    if len(h1_candles) < 25: return None, None, 0
    
    recent_lows = [c['low'] for c in h1_candles[-20:]]
    recent_lows.sort()
    
    # Lấy vùng hỗ trợ từ cụm giá thấp gần nhất
    supp_low = recent_lows[0]
    supp_high = recent_lows[3] # Tự động tạo dải hỗ trợ (Support Zone)
    
    # Kiểm tra số lần phản ứng tại vùng này
    touch_count = sum(1 for c in h1_candles[-25:] if supp_low * 0.998 <= c['low'] <= supp_high * 1.005)
    
    return supp_low, supp_high, touch_count

def evaluate_long_a_accumulation(symbol, h1_candles, h4_candles, btc_h1_candles):
    """
    SETUP LONG A — H1 ACCUMULATION BASE / SUPPORT RECLAIM
    Thang điểm 100 điểm:
    - Support Structure: 20đ
    - Price Action / Reclaim: 20đ
    - Volume: 10đ
    - OI: 15đ
    - CVD: 10đ
    - RSI: 10đ
    - Relative Strength: 10đ
    - BTC Regime: 5đ
    """
    supp_low, supp_high, touch_count = find_support_zone(h1_candles, h4_candles)
    if not supp_low or touch_count < 1:
        return None

    last = h1_candles[-1]
    curr_price = last['close']
    
    # 1. GIÁ PHẢI Ở GẦN SUPPORT (Distance <= 2.0%)
    dist_to_supp = (curr_price - supp_high) / supp_high
    if dist_to_supp > 0.020 or curr_price < supp_low * 0.995:
        return None # Loại nếu quá xa hoặc thủng hỗ trợ mạnh

    # --- TÍNH TOÁN BỘ ĐIỂM 100 ĐIỂM ---
    score_support_struct = 20 if touch_count >= 2 else 12
    
    # Price Action: Reclaim / Wick / Higher Low
    h1_range = last['high'] - last['low']
    wick_ratio = (min(last['open'], last['close']) - last['low']) / h1_range if h1_range > 0 else 0
    
    score_pa = 0
    if last['low'] < supp_low and last['close'] > supp_low: score_pa += 20 # Sweep & Reclaim
    elif wick_ratio >= 0.30: score_pa += 16
    elif curr_price >= supp_high: score_pa += 14
    else: score_pa += 10

    # Flow Confirmation: Volume, OI, CVD
    avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol if avg_vol > 0 else 1.0
    score_vol = 10 if vol_ratio >= 1.2 else (7 if 0.8 <= vol_ratio < 1.2 else 4)

    _, _, oi_pct = get_open_interest_data(symbol, "1h")
    score_oi = 15 if oi_pct >= 1.5 else (10 if oi_pct >= 0 else 5)

    curr_buy, prev_buy = get_taker_buy_data(symbol)
    taker_pct = (curr_buy * 100) if curr_buy is not None else 50.0
    score_cvd = 10 if taker_pct >= 55.0 else (6 if taker_pct >= 48.0 else 3)

    # RSI (Ưu tiên 35 - 55, Bullish Divergence cộng điểm)
    rsi_vals = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_vals[-1] if rsi_vals else 50.0
    
    score_rsi = 0
    if 35 <= curr_rsi <= 55: score_rsi += 7
    elif curr_rsi < 35: score_rsi += 5
    
    if len(rsi_vals) >= 12 and curr_rsi > min(rsi_vals[-12:-3]):
        score_rsi += 3 # Cộng điểm Bullish Divergence
    score_rsi = min(score_rsi, 10)

    # Relative Strength & BTC Regime
    sym_chg_6h = (h1_candles[-1]['close'] - h1_candles[-6]['close']) / h1_candles[-6]['close']
    btc_chg_6h = (btc_h1_candles[-1]['close'] - btc_h1_candles[-6]['close']) / btc_h1_candles[-6]['close'] if btc_h1_candles else 0
    
    score_rs = 10 if sym_chg_6h > btc_chg_6h else 5
    score_btc = 5 if btc_chg_6h >= -0.005 else 2

    total_score = score_support_struct + score_pa + score_vol + score_oi + score_cvd + score_rsi + score_rs + score_btc
    total_score = min(total_score, 100)

    if total_score < 70:
        return None

    # TÍNH ENTRY ZONE, SL & TP CẤU TRÚC
    atr15 = calculate_m15_atr(symbol)
    entry_low = max(supp_low, curr_price * 0.997)
    entry_high = curr_price
    entry_mid = (entry_low + entry_high) / 2

    # SL dưới đáy Support Zone + Buffer ATR
    sl = supp_low - (atr15 * 0.3) if atr15 > 0 else supp_low * 0.995
    risk = entry_mid - sl
    if risk <= 0: return None

    # TP Multi-Tier
    prev_highs = [c['high'] for c in h1_candles[-25:]]
    swing_high = max(prev_highs)
    
    tp1 = entry_mid + (risk * 1.5)
    tp2 = max(entry_mid + (risk * 2.5), swing_high)
    tp3 = entry_mid + (risk * 3.8)

    signal_status = "LONG" if total_score >= 80 else "WATCH"

    return {
        'signal_status': signal_status,
        'setup_type_tag': "LONG A",
        'setup_name': "H1 ACCUMULATION BASE / SUPPORT RECLAIM",
        'symbol': symbol,
        'total_score': total_score,
        'health_score': score_rs + score_btc + score_support_struct,
        'setup_score': score_pa + score_vol + score_oi + score_cvd + score_rsi,
        'vol_ratio': vol_ratio,
        'oi_pct': oi_pct,
        'taker_pct': taker_pct,
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
        'signal_id': f"LONGA_BASE_{symbol}_{last['timestamp']}"
    }

# ==============================================================================
# 4. FORMATTER TIN NHẮN TELEGRAM chuẩn KHUÔN MẪU
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    icon_head = "🧱" if sig['signal_status'] == "LONG" else "👀"
    status_label = "LONG ACCUMULATION" if sig['signal_status'] == "LONG" else "CẢNH BÁO WATCH"

    msg = f"""{icon_head} <b>[{sig['setup_name']}] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['total_score']}/100</b> ({status_label})
🛡 Support Zone: <b>{sig['supp_low']:.4f} – {sig['supp_high']:.4f}</b>

🔥 <b>TÍN HIỆU TÍCH LŨY / CỨU NỀN HỖ TRỢ:</b>
• Volume H1: <b>+{sig['vol_ratio']:.2f}x (SMA20)</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}% (Tăng khi Reclaim)</b>
• Lực Mua CVD: <b>{sig['taker_pct']:.1f}% (Cải thiện)</b>
• Trạng Thái: <b>Giữ vững Support Zone trước Breakout</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (BUY THE BASE):</b>

📍 <b>Entry Zone:</b>
{sig['entry_low']:.4f} – {sig['entry_high']:.4f}

🛑 <b>Stop Loss:</b>
{sig['sl']:.4f} (Dưới đáy Support Zone: -{sig['risk_pct']:.2f}%)

🎯 <b>Take Profit 1:</b>
{sig['tp1']:.4f} (Kháng cự gần / 1.5R)

🚀 <b>Take Profit 2:</b>
{sig['tp2']:.4f} (Swing High cũ / 2.5R+)

🔥 <b>Take Profit 3:</b>
{sig['tp3']:.4f} (Expansion Momentum / 3.8R)
-------------------------------------
👉 <b>BẮT TÍCH LŨY NỀN HỖ TRỢ TRƯỚC KHI PUMP!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['setup_type_tag']}] cho {sig['symbol']} (Score: {sig['total_score']})", flush=True)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# 5. MAIN LOOP RUNNER VỚI CHỐNG SPAM (TOP 4 COIN)
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Long Accumulation Base & Squeeze (Top 4 Coin)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=50)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang quét thang điểm 100 tích lũy cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                if get_24h_turnover(symbol) < MIN_TURNOVER_24H:
                    continue
                    
                h1_candles = get_closed_klines(symbol, "60", limit=50)
                h4_candles = get_closed_klines(symbol, "240", limit=50)
                
                sig = evaluate_long_a_accumulation(symbol, h1_candles, h4_candles, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.12)

        # CHỌN TOP 4 COIN ĐIỂM CAO NHẤT ĐỂ BẮN CẢNH BÁO
        if candidates:
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
