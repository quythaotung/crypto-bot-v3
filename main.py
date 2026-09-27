import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION (CÁC BIẾN CẤU HÌNH)
# ==============================================================================
DEBUG_MODE = True                  # In chi tiết lý do REJECT ra Logs
REQUIRE_TAKER_BUY = True           # Bắt buộc Taker Buy >= 55%
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Turnover tối thiểu $5M USD (Lọc coin rác)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Bộ nhớ tạm lưu lịch sử lệnh & chống trùng lặp
signals_history = []
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Squeeze Explosion Bot (All Coins Bybit) Active 24/7!"

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
    """Lắng nghe lệnh /pnl từ Telegram"""
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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH SQUEEZE:</b>\n-------------------------------------\n"
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
            
            msg += f"<b>#{sig['id']} {sig['symbol']}</b> | Entry: {sig['entry']:.4f}\n"
            msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
            msg += f"• Trạng thái: <b>{status}</b>\n-------------------------------------\n"
            
        total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
        msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi PnL: {e}")

# ==============================================================================
# 2. BYBIT REST API HELPERS (LẤY TOÀN BỘ DANH SÁCH COIN BYBIT)
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    """Tự động lấy TẤT CẢ các cặp giao dịch .USDT Futures đang niêm yết trên Bybit"""
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            # Lấy tất cả các cặp kết thúc bằng USDT, loại bỏ token đòn bẩy hoặc Stablecoin
            symbols = [
                t['symbol'] for t in tickers 
                if t['symbol'].endswith('USDT') 
                and not t['symbol'].startswith(('USDC', 'BUSD', 'DAI'))
                and not any(x in t['symbol'] for x in ['3L', '3S', '2L', '2S'])
            ]
            print(f"🌐 [BYBIT API] Đã lấy thành công danh sách TOÀN BỘ {len(symbols)} coin USDT!")
            return symbols
    except Exception as e:
        print(f"Lỗi lấy danh sách toàn bộ coin: {e}")
    
    # Fallback danh sách mặc định nếu nghẽn mạng
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"]

def get_closed_klines(symbol, interval, limit=25):
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            closed_candles = raw_list[1:] # Bỏ nến index 0 đang chạy
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

def get_open_interest_h1(symbol):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=1h&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                if prev_oi > 0:
                    return ((curr_oi - prev_oi) / prev_oi) * 100
    except Exception:
        pass
    return None

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def get_taker_buy_ratio(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit=2"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) > 0:
                buy_ratio = float(data[0].get("buyRatio", 0))
                if buy_ratio > 0:
                    return buy_ratio
    except Exception:
        pass
    return None

# ==============================================================================
# 3. THUẬT TOÁN ĐIỀU KIỆN FILTER LONG LIQUIDITY SWEEP
# ==============================================================================
def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

def is_sweep_and_reclaim(candles, lookback=10):
    if len(candles) < lookback + 1: return False, 0
    current_candle = candles[-1]
    previous_candles = candles[-(lookback+1):-1]
    swing_low = min(c['low'] for c in previous_candles)
    if current_candle['low'] < swing_low and current_candle['close'] > swing_low:
        return True, swing_low
    return False, swing_low

def analyze_symbol(symbol):
    # 1. Turnover & Adaptive Filter
    turnover = get_24h_turnover(symbol)
    if turnover < MIN_TURNOVER_24H: return None
    
    if 5_000_000 <= turnover < 10_000_000: required_score = 90
    elif 10_000_000 <= turnover < 20_000_000: required_score = 86
    elif 20_000_000 <= turnover < 50_000_000: required_score = 83
    else: required_score = 82

    # 2. Kiểm tra H1
    h1_candles = get_closed_klines(symbol, "60", limit=25)
    if len(h1_candles) < 21: return None
    
    h1_last = h1_candles[-1]
    h1_range = h1_last['high'] - h1_last['low']
    if h1_range == 0: return None
    
    h1_lower_wick = min(h1_last['open'], h1_last['close']) - h1_last['low']
    h1_wick_ratio = h1_lower_wick / h1_range
    h1_close_loc = (h1_last['close'] - h1_last['low']) / h1_range
    
    h1_sweep, _ = is_sweep_and_reclaim(h1_candles, lookback=12)
    if not h1_sweep or h1_wick_ratio < 0.35 or h1_close_loc < 0.65: return None

    # 3. H1 Volume >= 1.8x
    h1_avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    h1_vol_ratio = h1_last['volume'] / h1_avg_vol if h1_avg_vol > 0 else 0
    if h1_vol_ratio < 1.8: return None

    # 4. H1 Open Interest >= 2%
    oi_change_pct = get_open_interest_h1(symbol)
    if oi_change_pct is None or oi_change_pct < 2.0: return None

    # 5. H1 Taker Buy >= 55%
    taker_buy_ratio = get_taker_buy_ratio(symbol)
    if taker_buy_ratio is None:
        if REQUIRE_TAKER_BUY: return None
        taker_buy_ratio = 0.55
    elif taker_buy_ratio < 0.55: return None

    # 6. H4 Confirmation
    h4_candles = get_closed_klines(symbol, "240", limit=25)
    if len(h4_candles) < 21: return None
    h4_last = h4_candles[-1]
    h4_range = h4_last['high'] - h4_last['low']
    h4_pass = False
    h4_vol_bonus = False
    if h4_range > 0:
        h4_lower_wick = min(h4_last['open'], h4_last['close']) - h4_last['low']
        h4_sweep, _ = is_sweep_and_reclaim(h4_candles, lookback=8)
        if h4_sweep and (h4_lower_wick / h4_range) >= 0.30 and ((h4_last['close'] - h4_last['low']) / h4_range) >= 0.60:
            h4_pass = True
        h4_avg_vol = calculate_sma_volume(h4_candles[:-1], 20)
        if h4_avg_vol > 0 and (h4_last['volume'] / h4_avg_vol) >= 1.5: h4_vol_bonus = True

    if not h4_pass: return None

    # 7. M15 Confirmation
    m15_candles = get_closed_klines(symbol, "15", limit=25)
    if len(m15_candles) < 21: return None
    m15_last = m15_candles[-1]
    m15_prev = m15_candles[-2]
    
    m15_pass = False
    if (m15_last['close'] > m15_prev['open'] and m15_prev['close'] < m15_prev['open']) or \
       (m15_last['close'] > m15_prev['high']) or \
       is_sweep_and_reclaim(m15_candles, lookback=5)[0]:
        m15_pass = True

    m15_avg_vol = calculate_sma_volume(m15_candles[:-1], 20)
    m15_vol_bonus = (m15_avg_vol > 0 and (m15_last['volume'] / m15_avg_vol) >= 1.2)

    if not m15_pass: return None

    # 8. Score
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

    if score < required_score: return None

    # Entry, SL, TP (R:R >= 1.5R)
    entry_price = h1_last['close']
    sl_price = h1_last['low'] * 0.998
    risk = entry_price - sl_price
    if risk <= 0: return None

    tp1 = entry_price + (risk * 1.5)
    tp2 = entry_price + (risk * 2.5)

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
        'signal_id': signal_id
    }

# ==============================================================================
# 4. GỬI TELEGRAM & CẬP NHẬT STT
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""🚀 <b>[LONG H1 SQUEEZE EXPLOSION] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['score']}/100</b>

🔥 <b>DẤU HIỆU CÁ MẬP GOM SPOT & SHORT SQUEEZE:</b>
• Volume H1: <b>+{sig['h1_vol_ratio']:.1f}x (Dốc đứng)</b>
• Open Interest (OI): <b>+{sig['oi_change_pct']:.1f}% (Tiền bơm mạnh)</b>
• Lực Mua CVD: <b>{sig['taker_buy_pct']:.1f}% (Taker Buy ép đỉnh)</b>
• Trạng Thái: <b>Breakout Nền Tích Lũy</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>

📍 <b>Entry:</b> {sig['entry']:.4f}
🛑 <b>Stop Loss:</b> {sig['sl']:.4f}
🎯 <b>Take Profit 1:</b> {sig['tp1']:.4f}
🚀 <b>Take Profit 2:</b> {sig['tp2']:.4f}
-------------------------------------
👉 <b>CHECK CHART H1 NGAY!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu Squeeze #{sig['id']} cho {sig['symbol']}")
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

# ==============================================================================
# 5. CHU KỲ RUNNER SCAN TOÀN BỘ SÀN
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Squeeze Explosion 24/7 (Quét TOÀN BỘ sàn Bybit)...")
    while True:
        # Lấy danh sách toàn bộ các coin niêm yết trên Bybit mỗi chu kỳ
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang tiến hành quét {len(all_symbols)} coin...")
        
        for symbol in all_symbols:
            try:
                sig = analyze_symbol(symbol)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
            except Exception as e:
                pass
            time.sleep(0.2) # Nghỉ nhẹ 0.2s để tránh quá tải API
            
        print(f"✅ Hoàn thành 1 chu kỳ quét toàn sàn. Nghỉ {COOLDOWN_SECONDS}s...")
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
