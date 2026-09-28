import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In chi tiết lý do REJECT ra Logs
REQUIRE_HARD_CONDITIONS = True     # Bắt buộc Sweep + RSI Divergence
MIN_SCORE_REQUIRED = 80            # Ngưỡng điểm tối thiểu để bắn tín hiệu (Max 100)
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Lọc coin có Turnover 24h >= $5M USD

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
    return "Bybit H1 Liquidity Sweep & RSI Divergence Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH (SWEEP + RSI DIVERGENCE):</b>\n-------------------------------------\n"
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
            msg += f"• Score: {sig['score']}/100 | Trạng thái: <b>{status}</b>\n-------------------------------------\n"
            
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

def calculate_rsi(candles, period=14):
    if len(candles) < period + 1:
        return []
    closes = [c['close'] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i-1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    
    rsi_values = []
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        rsi_values.append(100)
    else:
        rs = avg_gain / avg_loss
        rsi_values.append(100 - (100 / (1 + rs)))
        
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_values.append(100)
        else:
            rs = avg_gain / avg_loss
            rsi_values.append(100 - (100 / (1 + rs)))
    return rsi_values

def calculate_ema(candles, period):
    closes = [c['close'] for c in candles]
    if len(closes) < period:
        return 0.0
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price * k) + (ema * (1 - k))
    return ema

def calculate_m15_atr(symbol, period=14):
    m15_candles = get_closed_klines(symbol, "15", limit=period+5)
    if len(m15_candles) < period:
        return 0.0
    tr_list = []
    for i in range(1, len(m15_candles)):
        high = m15_candles[i]['high']
        low = m15_candles[i]['low']
        prev_close = m15_candles[i-1]['close']
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
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
# 3. ADVANCED STRATEGY LOGIC: LIQUIDITY SWEEP + RSI BULLISH DIVERGENCE
# ==============================================================================

def check_liquidity_sweep(candles, lookback=15):
    """
    KTL: Low của nến H1 hiện tại đâm thủng Swing Low của các nến trước đó,
    nhưng CLOSE đóng ngược trở lại phía trên vùng Swing Low đó.
    """
    if len(candles) < lookback + 1:
        return False, 0.0, 0.0
    
    last_candle = candles[-1]
    prev_candles = candles[-(lookback+1):-1]
    
    swing_low = min(c['low'] for c in prev_candles)
    swing_high = max(c['high'] for c in prev_candles)
    
    if last_candle['low'] < swing_low and last_candle['close'] > swing_low:
        return True, swing_low, swing_high
    return False, swing_low, swing_high

def check_rsi_bullish_divergence(candles, rsi_values):
    """
    KTL: Giá tạo Lower Low (Đáy giá sau thấp hơn đáy giá trước) 
    nhưng RSI tạo Higher Low (Đáy RSI sau cao hơn đáy RSI trước). Ưu tiên đáy RSI <= 45.
    """
    if len(rsi_values) < 15 or len(candles) < 15:
        return False, 0.0, 0.0
    
    # Lấy chỉ số nến và RSI tương ứng
    curr_low = candles[-1]['low']
    curr_rsi = rsi_values[-1]
    
    # Tìm đáy cục bộ trong khoảng 3 đến 12 nến trước đó
    past_candles = candles[-12:-3]
    past_rsi = rsi_values[-12:-3]
    
    if not past_candles or not past_rsi:
        return False, 0.0, 0.0
        
    min_past_low = min(c['low'] for c in past_candles)
    min_past_rsi = min(past_rsi)
    
    # Điều kiện Phân Kỳ Dương: Giá Đáy Sau < Đáy Trước & RSI Đáy Sau > RSI Đáy Trước
    if curr_low < min_past_low and curr_rsi > min_past_rsi and min_past_rsi <= 45:
        return True, curr_rsi, min_past_rsi
    return False, curr_rsi, min_past_rsi

def analyze_h1_liquidity_divergence(symbol):
    # 1. BỘ LỌC TURNOVER 24H
    turnover = get_24h_turnover(symbol)
    if turnover < MIN_TURNOVER_24H:
        return None

    # 2. LẤY NẾN H1 ĐÃ ĐÓNG
    h1_candles = get_closed_klines(symbol, "60", limit=50)
    if len(h1_candles) < 35:
        return None

    last = h1_candles[-1]
    h1_range = last['high'] - last['low']
    if h1_range == 0: return None

    # --- ĐIỀU KIỆN CỨNG 1: LIQUIDITY SWEEP H1 (20 ĐIỂM) ---
    is_sweep, swing_low, swing_high = check_liquidity_sweep(h1_candles, lookback=15)
    if not is_sweep:
        if DEBUG_MODE: print(f"[{symbol}] REJECT: Không có Liquidity Sweep H1", flush=True)
        return None

    # --- ĐIỀU KIỆN CỨNG 2: RSI BULLISH DIVERGENCE (20 ĐIỂM) ---
    rsi_values = calculate_rsi(h1_candles, period=14)
    is_rsi_div, curr_rsi, past_rsi = check_rsi_bullish_divergence(h1_candles, rsi_values)
    if not is_rsi_div:
        if DEBUG_MODE: print(f"[{symbol}] REJECT: Không có RSI Bullish Divergence", flush=True)
        return None

    # --- CHỐNG MUA ĐUỔI PUMP & CHỐNG ĐỤNG CẢN DẦU ---
    # Giá đóng H1 không được cao hơn quá 2.5% so with Swing Low vừa sweep
    if (last['close'] - swing_low) / swing_low > 0.025:
        if DEBUG_MODE: print(f"[{symbol}] REJECT: Giá đã pump quá xa vùng Sweep", flush=True)
        return None

    # Không nằm quá sát đỉnh kháng cự gần nhất (Khoảng cách phải >= 1.5R)
    dist_to_resistance = swing_high - last['close']

    # --- ĐIỀU KIỆN 3: H1 REJECTION (15 ĐIỂM) ---
    lower_wick = min(last['open'], last['close']) - last['low']
    wick_ratio = lower_wick / h1_range
    close_loc = (last['close'] - last['low']) / h1_range
    
    is_rejection = (wick_ratio >= 0.35) and (close_loc >= 0.60)

    # --- ĐIỀU KIỆN 4: VOLUME H1 (10 ĐIỂM) ---
    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol_20 if avg_vol_20 > 0 else 0.0

    # --- ĐIỀU KIỆN 5 & 6: CVD & SPOT CVD RECOVERY (15 ĐIỂM + 10 ĐIỂM) ---
    curr_buy, prev_buy = get_taker_buy_data(symbol)
    taker_buy_pct = (curr_buy * 100) if curr_buy is not None else 55.0
    cvd_recovering = (curr_buy is not None and prev_buy is not None and curr_buy >= prev_buy) or (taker_buy_pct >= 55.0)

    # --- ĐIỀU KIỆN 7: OPEN INTEREST XÁC NHẬN (5 ĐIỂM) ---
    curr_oi, prev_oi, oi_pct = get_open_interest_data(symbol, interval_str="1h")
    # Giá tăng/reclaim + OI tăng hoặc giữ ổn định = Tiền thực sự hỗ trợ
    oi_valid = (oi_pct >= 0.5)

    # --- ĐIỀU KIỆN 8: EMA34 / EMA89 PULLBACK (5 ĐIỂM) ---
    ema34 = calculate_ema(h1_candles, 34)
    ema89 = calculate_ema(h1_candles, 89)
    ema_bullish = (ema34 > ema89)
    price_near_ema = (abs(last['close'] - ema34) / ema34 <= 0.015) or (abs(last['close'] - ema89) / ema89 <= 0.015)

    # ==============================================================================
    # TÍNH TOÁN TỔNG SCORE (MAX 100 ĐIỂM)
    # ==============================================================================
    score = 0
    if is_sweep: score += 20             # Bắt buộc
    if is_rsi_div: score += 20           # Bắt buộc
    if is_rejection: score += 15
    if vol_ratio >= 1.5: score += 10
    elif vol_ratio >= 1.3: score += 7
    if cvd_recovering: score += 15
    if taker_buy_pct >= 60.0: score += 10 # Spot/Futures CVD nổ mạnh
    if oi_valid: score += 5
    if ema_bullish or price_near_ema: score += 5

    score = min(score, 100)

    if score < MIN_SCORE_REQUIRED:
        if DEBUG_MODE: print(f"[{symbol}] REJECT: Score {score} < {MIN_SCORE_REQUIRED}", flush=True)
        return None

    # ==============================================================================
    # TÍNH ENTRY / SL / TP (R:R > 1.5R)
    # ==============================================================================
    atr_15 = calculate_m15_atr(symbol)
    entry_price = last['close']
    
    # SL dưới đáy râu Sweep + Buffer ATR M15
    sl_price = last['low'] - (atr_15 * 0.3) if atr_15 > 0 else last['low'] * 0.997
    risk = entry_price - sl_price

    if risk <= 0: return None

    # Kiểm tra khoảng cách tới cản Swing High xem có đạt tối thiểu 1.5R không
    if dist_to_resistance < (risk * 1.5):
        if DEBUG_MODE: print(f"[{symbol}] REJECT: Tỷ lệ R:R tới cản không đạt 1.5R", flush=True)
        return None

    tp1 = entry_price + (risk * 1.5)
    tp2 = max(entry_price + (risk * 2.8), swing_high)

    signal_id = f"SWEEP_RSI_{symbol}_{last['timestamp']}"

    return {
        'type': 'LONG H1 LIQUIDITY SWEEP & RSI DIVERGENCE',
        'symbol': symbol,
        'score': score,
        'rsi_curr': curr_rsi,
        'rsi_past': past_rsi,
        'vol_ratio': vol_ratio,
        'oi_pct': oi_pct,
        'taker_buy_pct': taker_buy_pct,
        'wick_ratio': wick_ratio * 100,
        'close_loc': close_loc * 100,
        'entry': entry_price,
        'sl': sl_price,
        'tp1': tp1,
        'tp2': tp2,
        'risk_pct': (risk / entry_price) * 100,
        'signal_id': signal_id
    }

# ==============================================================================
# 4. TELEGRAM ALERT FORMATTER
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""🐋 <b>[LONG H1 SWEEP & RSI DIVERGENCE] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['score']}/100</b>

🔥 <b>HỘI TỤ ĐIỀU KIỆN CÁ MẬP GOM HÀNG:</b>
✅ <b>Liquidity Sweep:</b> Đã quét đáy Swing Low H1 & Reclaim
✅ <b>RSI Divergence:</b> Phân kỳ dương H1 (RSI {sig['rsi_curr']:.1f} > {sig['rsi_past']:.1f})
• Râu rút chân H4/H1: <b>{sig['wick_ratio']:.1f}%</b> (Đóng {sig['close_loc']:.1f}% đỉnh nến)
• Volume H1: <b>+{sig['vol_ratio']:.2f}x (SMA20)</b>
• Open Interest (OI): <b>+{sig['oi_pct']:.2f}% (Tiền tươi)</b>
• Lực Mua CVD: <b>{sig['taker_buy_pct']:.1f}% (Taker Buy hồi phục)</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG ENTRY):</b>

📍 <b>Entry (Close H1):</b> {sig['entry']:.4f}
🛑 <b>Stop Loss:</b> {sig['sl']:.4f} (Dưới đáy Sweep: -{sig['risk_pct']:.2f}%)
🎯 <b>Take Profit 1:</b> {sig['tp1']:.4f} (1.5R)
🚀 <b>Take Profit 2:</b> {sig['tp2']:.4f} (Cản đỉnh cũ / 2.8R)
-------------------------------------
👉 <b>ĐÃ QUÉT THANH KHOẢN & PHÂN KỲ DƯƠNG - VÀO LỆNH AN TOÀN!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
        print(f"✅ Đã bắn tín hiệu #{sig['id']} cho {sig['symbol']}", flush=True)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

# ==============================================================================
# 5. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét H1 Sweep & RSI Divergence trên Bybit...", flush=True)
    while True:
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang tiến hành quét {len(all_symbols)} coin...", flush=True)
        
        for symbol in all_symbols:
            try:
                sig = analyze_h1_liquidity_divergence(symbol)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
            except Exception:
                pass
            time.sleep(0.2)
            
        print(f"✅ Hoàn thành 1 vòng quét. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
