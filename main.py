import time
import requests
import os
import math
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION (CÁC BIẾN CẤU HÌNH CÓ THỂ ĐIỀU CHỈNH CHÍNH)
# ==============================================================================
DEBUG_MODE = True                  # In chi tiết lý do REJECT ra Logs
REQUIRE_TAKER_BUY = True           # Bắt buộc kiểm tra Taker Buy >= 55%
COOLDOWN_SECONDS = 900             # Giãn cách quét giữa các chu kỳ (15 phút)

# Mặc định lọc Turnover < 5M USD
MIN_TURNOVER_24H = 5_000_000       

# Danh sách các cặp Coin theo dõi (Có thể thêm/bớt tùy thích)
SYMBOLS_TO_WATCH = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "GRASSUSDT", "QNTUSDT", 
    "XRPUSDT", "DOGEUSDT", "SUIUSDT", "BNBUSDT", "LINKUSDT",
    "AVAXUSDT", "NEARUSDT", "APTUSDT", "FETUSDT", "TAOUSDT"
]

# Telegram Credentials lấy từ Render Environment Variables
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# ==============================================================================
# 1. FLASK HEALTH-CHECK SERVER (DÙNG CHO RENDER KEEP-ALIVE)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Long Liquidity Sweep Bot is Running 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. BYBIT REST API HELPERS (DỮ LIỆU THẬT 100%)
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_closed_klines(symbol, interval, limit=25):
    """
    Lấy danh sách nến ĐÃ ĐÓNG từ Bybit V5 API.
    Bybit trả về dữ liệu mới nhất ở index 0, nên index 1 là nến vừa ĐÓNG hoàn chỉnh.
    """
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            # Bỏ nến index 0 (nến đang chạy), lấy các nến đã đóng hoàn toàn
            closed_candles = raw_list[1:]
            
            parsed = []
            for c in reversed(closed_candles): # Đảo lại thứ tự thời gian tăng dần
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
    except Exception as e:
        if DEBUG_MODE: print(f"[{symbol}] Lỗi lấy Kline {interval}: {e}")
    return []

def get_open_interest_h1(symbol):
    """
    Lấy Open Interest 2 nến H1 gần nhất từ Bybit.
    Trả về (current_oi, prev_oi, oi_change_pct)
    """
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=1h&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                # Index 0 là mới nhất, index 1 là trước đó
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                if prev_oi > 0:
                    pct = ((curr_oi - prev_oi) / prev_oi) * 100
                    return curr_oi, prev_oi, pct
    except Exception as e:
        if DEBUG_MODE: print(f"[{symbol}] Lỗi lấy Open Interest: {e}")
    return None, None, None

def get_24h_turnover(symbol):
    """Lấy Turnover (Khối lượng giao dịch USD) 24h"""
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            ticker = res["result"]["list"][0]
            return float(ticker.get("turnover24h", 0))
    except Exception as e:
        if DEBUG_MODE: print(f"[{symbol}] Lỗi lấy Ticker 24h: {e}")
    return 0

def get_taker_buy_ratio(symbol):
    """
    Lấy Taker Buy Volume từ Bybit API công khai (nếu endpoint hỗ trợ).
    Nếu API không cung cấp, trả về None (Không tự bịa dữ liệu).
    """
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit=2"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data_list = res["result"]["list"]
            if len(data_list) > 0:
                buy_ratio = float(data_list[0].get("buyRatio", 0))
                if buy_ratio > 0:
                    return buy_ratio # Trả về giá trị dạng 0.58 (58%)
    except Exception:
        pass
    return None

# ==============================================================================
# 3. THUẬT TOÁN TÍNH TOÁN VÀ KIỂM TRA ĐIỀU KIỆN TÍN HIỆU
# ==============================================================================

def calculate_sma_volume(candles, period=20):
    if len(candles) < period:
        return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

def is_sweep_and_reclaim(candles, lookback=10):
    """Check xem nến đóng cửa mới nhất có Liquidity Sweep phía dưới Swing Low hay không."""
    if len(candles) < lookback + 1:
        return False, 0
    
    current_candle = candles[-1]
    previous_candles = candles[-(lookback+1):-1]
    
    swing_low = min(c['low'] for c in previous_candles)
    
    # Low hiện tại phải đâm thủng Swing Low cũ, nhưng CLOSE phải đóng ngược lại phía trên
    if current_candle['low'] < swing_low and current_candle['close'] > swing_low:
        return True, swing_low
    return False, swing_low

def analyze_symbol(symbol):
    logs = [f"\n[{symbol}] REJECT ANALYSIS:"]
    
    # 1. KIỂM TRA TURNOVER 24H & ADAPTIVE FILTER
    turnover = get_24h_turnover(symbol)
    if turnover < MIN_TURNOVER_24H:
        logs.append(f"- Turnover: FAIL ({turnover/1e6:.2f}M < 5M)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    
    if 5_000_000 <= turnover < 10_000_000:
        required_score = 90
    elif 10_000_000 <= turnover < 20_000_000:
        required_score = 86
    elif 20_000_000 <= turnover < 50_000_000:
        required_score = 83
    else:
        required_score = 82

    # 2. KIỂM TRA NẾN H1
    h1_candles = get_closed_klines(symbol, "60", limit=25)
    if len(h1_candles) < 21:
        logs.append("- H1 Data: FAIL (Không đủ nến)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    
    h1_last = h1_candles[-1]
    h1_range = h1_last['high'] - h1_last['low']
    if h1_range == 0: return None
    
    h1_lower_wick = min(h1_last['open'], h1_last['close']) - h1_last['low']
    h1_wick_ratio = h1_lower_wick / h1_range
    h1_close_loc = (h1_last['close'] - h1_last['low']) / h1_range
    
    # Check H1 Sweep & Reclaim
    h1_sweep, h1_swing_low = is_sweep_and_reclaim(h1_candles, lookback=12)
    
    if not h1_sweep:
        logs.append("- H1 Sweep: FAIL (Không quét Swing Low)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append("- H1 Sweep: PASS")

    if h1_wick_ratio < 0.35 or h1_close_loc < 0.65:
        logs.append(f"- H1 Wick/Close: FAIL (Wick: {h1_wick_ratio:.2f} < 0.35 hoặc CloseLoc: {h1_close_loc:.2f} < 0.65)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append(f"- H1 Wick/Close: PASS (Wick: {h1_wick_ratio:.2f}, CloseLoc: {h1_close_loc:.2f})")

    # 3. H1 VOLUME EXPLOSION
    h1_avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    h1_vol_ratio = h1_last['volume'] / h1_avg_vol if h1_avg_vol > 0 else 0
    
    if h1_vol_ratio < 1.8:
        logs.append(f"- H1 Volume: FAIL ({h1_vol_ratio:.2f}x < 1.80x)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append(f"- H1 Volume: PASS ({h1_vol_ratio:.2f}x)")

    # 4. H1 OPEN INTEREST
    _, _, oi_change_pct = get_open_interest_h1(symbol)
    if oi_change_pct is None or oi_change_pct < 2.0:
        logs.append(f"- H1 OI: FAIL ({oi_change_pct if oi_change_pct else 0:.2f}% < +2.0%)")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append(f"- H1 OI: PASS (+{oi_change_pct:.2f}%)")

    # 5. H1 TAKER BUY / CVD
    taker_buy_ratio = get_taker_buy_ratio(symbol)
    if taker_buy_ratio is None:
        logs.append("- Taker Buy/CVD: Unavailable on API")
        if REQUIRE_TAKER_BUY:
            if DEBUG_MODE: print("\n".join(logs))
            return None
        taker_buy_ratio = 0.55 # Fallback nếu không bắt buộc
    else:
        if taker_buy_ratio < 0.55:
            logs.append(f"- Taker Buy: FAIL ({taker_buy_ratio*100:.1f}% < 55%)")
            if DEBUG_MODE: print("\n".join(logs))
            return None
        logs.append(f"- Taker Buy: PASS ({taker_buy_ratio*100:.1f}%)")

    # 6. H4 CONFIRMATION
    h4_candles = get_closed_klines(symbol, "240", limit=25)
    if len(h4_candles) < 21: return None
    h4_last = h4_candles[-1]
    h4_range = h4_last['high'] - h4_last['low']
    
    h4_pass = False
    h4_vol_bonus = False
    if h4_range > 0:
        h4_lower_wick = min(h4_last['open'], h4_last['close']) - h4_last['low']
        h4_wick_ratio = h4_lower_wick / h4_range
        h4_close_loc = (h4_last['close'] - h4_last['low']) / h4_range
        h4_sweep, _ = is_sweep_and_reclaim(h4_candles, lookback=8)
        
        if h4_sweep and h4_wick_ratio >= 0.30 and h4_close_loc >= 0.60:
            h4_pass = True
        
        h4_avg_vol = calculate_sma_volume(h4_candles[:-1], 20)
        if h4_avg_vol > 0 and (h4_last['volume'] / h4_avg_vol) >= 1.5:
            h4_vol_bonus = True

    if not h4_pass:
        logs.append("- H4 Confirmation: FAIL")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append("- H4 Confirmation: PASS")

    # 7. M15 CONFIRMATION
    m15_candles = get_closed_klines(symbol, "15", limit=25)
    if len(m15_candles) < 21: return None
    m15_last = m15_candles[-1]
    m15_prev = m15_candles[-2]
    
    m15_pass = False
    # A. Bullish Engulfing
    if m15_last['close'] > m15_prev['open'] and m15_prev['close'] < m15_prev['open']:
        m15_pass = True
    # B. Break High M15 trước
    elif m15_last['close'] > m15_prev['high']:
        m15_pass = True
    # C. Sweep nhỏ
    elif is_sweep_and_reclaim(m15_candles, lookback=5)[0]:
        m15_pass = True

    m15_avg_vol = calculate_sma_volume(m15_candles[:-1], 20)
    m15_vol_bonus = (m15_avg_vol > 0 and (m15_last['volume'] / m15_avg_vol) >= 1.2)

    if not m15_pass:
        logs.append("- M15 Confirmation: FAIL")
        if DEBUG_MODE: print("\n".join(logs))
        return None
    else:
        logs.append("- M15 Confirmation: PASS")

    # 8. TÍNH ĐIỂM SCORE (TỐI ĐA 100)
    score = 0
    if h1_sweep: score += 20
    if h1_wick_ratio >= 0.35: score += 10
    if h1_close_loc >= 0.65: score += 5
    if h1_vol_ratio >= 1.8: score += 15
    if oi_change_pct >= 2.0: score += 15
    if taker_buy_ratio >= 0.55: score += 15
    if h4_pass: score += 5
    if h4_vol_bonus: score += 3
    if m15_pass: score += 7
    if m15_vol_bonus: score += 5
    
    score = min(score, 100)
    logs.append(f"- Calculated Score: {score} (Yêu cầu adaptive: >= {required_score})")

    if score < required_score:
        logs.append("- Result: NO SIGNAL (Không đạt Score)")
        if DEBUG_MODE: print("\n".join(logs))
        return None

    # 9. TÍNH ENTRY / SL / TP (R:R >= 1.5R)
    entry_price = h1_last['close']
    sl_price = h1_last['low'] * 0.998 # Buffer 0.2% dưới chân râu Sweep
    risk = entry_price - sl_price
    
    if risk <= 0: return None

    tp1 = entry_price + (risk * 1.5)
    tp2 = entry_price + (risk * 2.5)
    rr_ratio = (tp1 - entry_price) / risk

    signal_id = f"{symbol}_{h1_last['timestamp']}"

    return {
        'symbol': symbol,
        'score': score,
        'h1_vol_ratio': h1_vol_ratio,
        'oi_change_pct': oi_change_pct,
        'taker_buy_pct': taker_buy_ratio * 100,
        'entry': entry_price,
        'sl': sl_price,
        'tp1': tp1,
        'tp2': tp2,
        'rr': rr_ratio,
        'signal_id': signal_id,
        'h4_vol_bonus': h4_vol_bonus,
        'm15_vol_bonus': m15_vol_bonus
    }

# ==============================================================================
# 4. TẠO VÀ GỬI THÔNG BÁO TELEGRAM
# ==============================================================================
processed_signals = set()

def send_telegram_signal(data):
    msg = f"""🚀 <b>LONG H1 LIQUIDITY SWEEP</b>

<b>Symbol:</b> #{data['symbol']}
<b>Score:</b> {data['score']}/100

<b>H4:</b>
✅ Sweep
✅ Reclaim
<b>Volume:</b> {'✅ >1.5x' if data['h4_vol_bonus'] else '🟡 Normal'}

<b>H1:</b>
✅ Liquidity Sweep
✅ Lower Wick
✅ <b>Volume:</b> {data['h1_vol_ratio']:.2f}x
✅ <b>OI:</b> +{data['oi_change_pct']:.2f}%
✅ <b>Taker Buy:</b> {data['taker_buy_pct']:.1f}%

<b>M15:</b>
✅ Confirmation
<b>Volume:</b> {'✅ >1.2x' if data['m15_vol_bonus'] else '🟡 Normal'}

📍 <b>Entry:</b> {data['entry']:.4f}
🛑 <b>SL:</b> {data['sl']:.4f}
🎯 <b>TP1:</b> {data['tp1']:.4f}
🚀 <b>TP2:</b> {data['tp2']:.4f}

⚖️ <b>R:R:</b> 1:{data['rr']:.1f}
"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code == 200:
            print(f"✅ Gửi Signal Telegram thành công cho {data['symbol']}!")
        else:
            print(f"❌ Lỗi Telegram: {res.text}")
    except Exception as e:
        print(f"❌ Exception Telegram: {e}")

# ==============================================================================
# 5. CHU KỲ QUÉT TỰ ĐỘNG
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar quét tín hiệu Long Liquidity Sweep 24/7...")
    while True:
        for symbol in SYMBOLS_TO_WATCH:
            try:
                sig = analyze_symbol(symbol)
                if sig:
                    if sig['signal_id'] not in processed_signals:
                        processed_signals.add(sig['signal_id'])
                        send_telegram_signal(sig)
                    else:
                        if DEBUG_MODE: print(f"[{symbol}] Signal ID trùng lập, bỏ qua.")
            except Exception as e:
                print(f"Lỗi khi xử lý {symbol}: {e}")
            time.sleep(1) # Tránh bị Rate Limit Bybit
            
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    # Chạy Web Server ngầm cho Render
    server_thread = Thread(target=run_flask)
    server_thread.daemon = True
    server_thread.start()
    
    # Chạy Loop quét Tín Hiệu
    run_radar_loop()
