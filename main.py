import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True
REQUIRE_TAKER_BUY = True
COOLDOWN_SECONDS = 300             # Quét 5 phút / lần
MIN_TURNOVER_24H = 5_000_000       # Turnover >= $5M USD

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit H1 & H4 Signal Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH (H1 & H4):</b>\n-------------------------------------\n"
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
            
            msg += f"<b>#{sig['id']} [{sig['type']}] {sig['symbol']}</b> | Entry: {sig['entry']:.4f}\n"
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
# 2. BYBIT API DATA HELPERS
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

def get_closed_klines(symbol, interval, limit=25):
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

def get_open_interest_change(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
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
                if buy_ratio > 0: return buy_ratio
    except Exception:
        pass
    return None

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

# ==============================================================================
# 3. ANALYSIS LOGIC: H1 SQUEEZE & H4 BULLISH REJECTION
# ==============================================================================

# --- PHÂN TÍCH H4 BULLISH REJECTION (THEO ẢNH QNTUSDT) ---
def analyze_h4_rejection(symbol):
    h4_candles = get_closed_klines(symbol, "240", limit=25)
    if len(h4_candles) < 21: return None
    
    h4_last = h4_candles[-1]
    h4_range = h4_last['high'] - h4_last['low']
    if h4_range == 0: return None
    
    # 1. Điều kiện Râu dưới H4 rút chân mạnh >= 40% range
    h4_lower_wick = min(h4_last['open'], h4_last['close']) - h4_last['low']
    h4_wick_ratio = h4_lower_wick / h4_range
    if h4_wick_ratio < 0.40: return None
    
    # 2. Điều kiện Vị trí đóng cửa nằm trên 65% thân nến
    h4_close_loc = (h4_last['close'] - h4_last['low']) / h4_range
    if h4_close_loc < 0.65: return None
    
    # 3. Sweep Liquidity H4
    h4_sweep, _ = is_sweep_and_reclaim(h4_candles, lookback=10)
    if not h4_sweep: return None
    
    # 4. Volume H4 >= 1.5x
    h4_avg_vol = calculate_sma_volume(h4_candles[:-1], 20)
    h4_vol_ratio = h4_last['volume'] / h4_avg_vol if h4_avg_vol > 0 else 0
    if h4_vol_ratio < 1.5: return None
    
    # 5. Open Interest H4 >= 3%
    oi_h4_pct = get_open_interest_change(symbol, interval_str="4h")
    if oi_h4_pct is None or oi_h4_pct < 3.0: return None
    
    # Tính toán Entry, SL, TP cho H4
    entry = h4_last['close']
    sl = h4_last['low'] * 0.995 # SL dưới chân râu nến H4
    risk = entry - sl
    if risk <= 0: return None
    
    tp1 = entry + (risk * 1.5)
    tp2 = entry + (risk * 3.0)
    
    score = 92
    signal_id = f"H4_{symbol}_{h4_last['timestamp']}"
    
    return {
        'type': 'H4 REJECTION',
        'symbol': symbol,
        'score': score,
        'wick_ratio': h4_wick_ratio * 100,
        'vol_ratio': h4_vol_ratio,
        'oi_pct': oi_h4_pct,
        'entry': entry,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'signal_id': signal_id
    }

# --- PHÂN TÍCH H1 SQUEEZE EXPLOSION ---
def analyze_h1_squeeze(symbol):
    turnover = get_24h_turnover(symbol)
    if turnover < MIN_TURNOVER_24H: return None
    
    if 5_000_000 <= turnover < 10_000_000: required_score = 90
    elif 10_000_000 <= turnover < 20_000_000: required_score = 86
    elif 20_000_000 <= turnover < 50_000_000: required_score = 83
    else: required_score = 82

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

    h1_avg_vol = calculate_sma_volume(h1_candles[:-1], 20)
    h1_vol_ratio = h1_last['volume'] / h1_avg_vol if h1_avg_vol > 0 else 0
    if h1_vol_ratio < 1.8: return None

    oi_change_pct = get_open_interest_change(symbol, interval_str="1h")
    if oi_change_pct is None or oi_change_pct < 2.0: return None

    taker_buy_ratio = get_taker_buy_ratio(symbol)
    if taker_buy_ratio is None:
        if REQUIRE_TAKER_BUY: return None
        taker_buy_ratio = 0.55
    elif taker_buy_ratio < 0.55: return None

    entry_price = h1_last['close']
    sl_price = h1_last['low'] * 0.998
    risk = entry_price - sl_price
    if risk <= 0: return None

    tp1 = entry_price + (risk * 1.5)
    tp2 = entry_price + (risk * 2.5)

    signal_id = f"H1_{symbol}_{h1_last['timestamp']}"

    return {
        'type': 'H1 SQUEEZE',
        'symbol': symbol,
        'score': 85,
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
# 4. GỬI TELEGRAM
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    if sig['type'] == 'H4 REJECTION':
        msg = f"""🐋 <b>[H4 BULLISH REJECTION] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['score']}/100</b>

🔥 <b>DẤU HIỆU CÁ MẬP GOM HÀNG KHUNG H4:</b>
• Râu nến dưới H4: <b>{sig['wick_ratio']:.1f}% (Rút chân cực mạnh)</b>
• Volume H4: <b>+{sig['vol_ratio']:.1f}x (Dốc đứng)</b>
• Open Interest (OI H4): <b>+{sig['oi_pct']:.1f}% (Dòng tiền vào mạnh)</b>
• Trạng thái: <b>Sweep Liquidity & Reclaim Cản H4</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG H4):</b>

📍 <b>Entry:</b> {sig['entry']:.4f}
🛑 <b>Stop Loss:</b> {sig['sl']:.4f} (Dưới râu nến H4)
🎯 <b>Take Profit 1:</b> {sig['tp1']:.4f} (1.5R)
🚀 <b>Take Profit 2:</b> {sig['tp2']:.4f} (3.0R)
-------------------------------------
👉 <b>CHUẨN BỊ CHO SÓNG TĂNG MẠNH KHUNG H4!</b>"""
    else:
        msg = f"""🚀 <b>[LONG H1 SQUEEZE EXPLOSION] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['score']}/100</b>

🔥 <b>DẤU HIỆU CÁ MẬP GOM SPOT & SHORT SQUEEZE:</b>
• Volume H1: <b>+{sig['h1_vol_ratio']:.1f}x (Dốc đứng)</b>
• Open Interest (OI): <b>+{sig['oi_change_pct']:.1f}% (Tiền bơm mạnh)</b>
• Lực Mua CVD: <b>{sig['taker_buy_pct']:.1f}% (Taker Buy ép đỉnh)</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG H1):</b>

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
        print(f"✅ Đã bắn tín hiệu #{sig['id']} ({sig['type']}) cho {sig['symbol']}")
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

# ==============================================================================
# 5. MAIN LOOP
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét H1 & H4 trên toàn bộ sàn Bybit...")
    while True:
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang tiến hành quét {len(all_symbols)} coin...")
        
        for symbol in all_symbols:
            try:
                # 1. Quét tín hiệu H4 Rejection trước
                sig_h4 = analyze_h4_rejection(symbol)
                if sig_h4 and sig_h4['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig_h4['signal_id'])
                    send_telegram_signal(sig_h4)

                # 2. Quét tín hiệu H1 Squeeze
                sig_h1 = analyze_h1_squeeze(symbol)
                if sig_h1 and sig_h1['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig_h1['signal_id'])
                    send_telegram_signal(sig_h1)
            except Exception:
                pass
            time.sleep(0.2)
            
        print(f"✅ Hoàn thành 1 vòng quét. Nghỉ {COOLDOWN_SECONDS}s...")
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
