import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - HIGH WINRATE TIMEFRAME ISOLATED BOT
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 10              
MIN_TURNOVER_24H = 8_000_000       # Chỉ lọc coin thanh khoản tốt (>= $8M)
MIN_WINRATE_SCORE = 80             # Nâng tiêu chuẩn điểm lọc setup lên >= 80

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                
processed_signal_ids = set()
symbol_cooldowns = {}              # Giãn cách gọi lệnh theo symbol (tránh trùng)
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Precise SL/TP Timeframe Isolated Bot Active 24/7!"

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
    if not BOT_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram Direct: {e}", flush=True)

def send_pnl_summary():
    if not signals_history:
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang vận hành 24/7!\n⏳ Chưa có tín hiệu chuẩn Kỹ Thuật nào phát ra từ lần khởi động gần nhất."
        send_telegram_direct_message(msg)
        return

    msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH REAL-TIME:</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    total_pnl_pct = 0.0
    
    for sig in signals_history:
        curr_price = get_current_price(sig['symbol'])
        if not curr_price:
            curr_price = sig['entry']
            
        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
        status = sig.get('status_text', '⏳ Đang theo dõi')
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        
        msg += f"<b>#{sig['id']} [{sig['signal_timeframe']} LONG] {sig['symbol']}</b> (Score: {sig['score']})\n"
        msg += f"• Entry: <code>{sig['entry']:.4f}</code> | Hiện tại: <code>{curr_price:.4f}</code> ({icon} <b>{pnl_pct:+.2f}%</b>)\n"
        msg += f"• Trạng thái: <b>{status}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        total_pnl_pct += pnl_pct
        
    total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
    msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"
    send_telegram_direct_message(msg)

def process_telegram_commands():
    last_update_id = 0
    print("🤖 Đã bật luồng lắng nghe lệnh Telegram (/pnl, /ping)...", flush=True)
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
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Precise SL/TP đang hoạt động 24/7!")
        except Exception as e:
            print(f"Lỗi luồng Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# 2. REAL-TIME MONITORING (TP / SL / CHASE WARNING / INVALID)
# ==============================================================================
def process_active_signals_tracker():
    while True:
        try:
            current_active = list(active_signals)
            for sig in current_active:
                try:
                    curr_price = get_current_price(sig['symbol'])
                    if not curr_price:
                        continue

                    tf = sig['signal_timeframe']

                    # 0. CANCEL NẾU GIÁ CHẠY QÚA XA ENTRY ZONE
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.018:
                            sig['status_text'] = f"⚠️ CANCEL (Giá đã rời quá xa {tf} Entry)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE PUMP] #{sig['id']} - {sig['symbol']}</b>\nGiá (<code>{curr_price:.4f}</code>) đã chạy vọt quá xa {tf} Entry Zone. Tự động hủy lệnh!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1 (Tự động HỦY cảnh báo SL)
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = f"🎯 Hit {tf} TP1 (Đã bảo toàn vốn)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM {tf} TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 {tf} TP1 Target: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG! (HIGH WIN-RATE)</b>
🛡 <i>Tự động HỦY cảnh báo SL đối với lệnh này. Dời SL về Breakeven để gồng TP2!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = f"🚀 Hit {tf} TP2"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CHẠM {tf} TP2 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 {tf} TP2 Target: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>CHỐT LỜI THÀNH CÔNG TẠI KHÁNG CỰ {tf}!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = f"🔥 Hit {tf} TP3"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CHẠM {tf} TP3 MAJOR SUPPLY] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 {tf} TP3 Target: <code>{sig['tp3']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH MỤC TIÊU CẤU TRÚC THUẦN {tf}!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)

                    # 4. HIT SL
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = f"🛑 Hit {tf} SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL - {tf} INVALID] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc {tf} SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Gãy mốc Hỗ trợ Cấu trúc {tf}. Dừng lệnh an toàn!</i>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)
                except Exception as inner_e:
                    print(f"Lỗi theo dõi coin {sig.get('symbol')}: {inner_e}", flush=True)
        except Exception as e:
            print(f"Lỗi theo dõi Tracker Loop: {e}", flush=True)
        
        time.sleep(10)

# ==============================================================================
# 3. BYBIT API & INDICATORS HELPERS
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            # BỎ MÃ ETF & CHỨNG KHOÁN MỸ
            excluded = ['3L', '3S', '2L', '2S', 'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT']
            symbols = [
                t['symbol'] for t in tickers 
                if t['symbol'].endswith('USDT') 
                and not t['symbol'].startswith(('USDC', 'BUSD', 'DAI'))
                and not any(x in t['symbol'] for x in excluded)
            ]
            return symbols
    except Exception:
        pass
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

def get_closed_klines(symbol, interval, limit=100):
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

def calculate_ema(candles, period):
    if len(candles) < period: return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema_values = []
    initial_sma = sum(closes[:period]) / period
    ema_values.append(initial_sma)
    for price in closes[period:]:
        new_ema = (price - ema_values[-1]) * multiplier + ema_values[-1]
        ema_values.append(new_ema)
    return ema_values

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

def get_taker_buy_pct(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period={interval_str}&limit=2"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) >= 1:
                return float(data[0].get("buyRatio", 0.5)) * 100
    except Exception:
        pass
    return 50.0

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 4. HÂN TÌM CẢN KỸ THUẬT CHUẨN KHUNG (KEY SWING HIGHS & RESISTANCE ZONES)
# ==============================================================================
def find_valid_timeframe_targets(candles, entry_price):
    """
    Tự động lọc các điểm kháng cự có 'Lực' (Key Swing Highs) trên đúng khung thời gian.
    Chỉ chọn điểm có ít nhất 2 nến xung quanh thấp hơn để loại bỏ râu nhiễu.
    """
    swing_highs = []
    for i in range(2, len(candles) - 2):
        c = candles[i]
        # Điều kiện Key Swing High: Đỉnh cao hơn 2 nến trước và 2 nến sau
        if (c['high'] > candles[i-1]['high'] and c['high'] > candles[i-2]['high'] and
            c['high'] > candles[i+1]['high'] and c['high'] > candles[i+2]['high']):
            if c['high'] > entry_price * 1.006: # Phải cao hơn Entry tối thiểu 0.6%
                swing_highs.append(c['high'])
                
    # Loại bỏ các mốc quá sát nhau (cách nhau dưới 0.4%)
    unique_targets = []
    for target in sorted(swing_highs):
        if not unique_targets or (target - unique_targets[-1]) / unique_targets[-1] * 100 >= 0.4:
            unique_targets.append(target)
            
    return unique_targets

# ==============================================================================
# 5. ENGINE: H1 BREAKOUT -> BASE -> RECLAIM (SL/TP THUẦN H1 100%)
# ==============================================================================
def evaluate_h1_breakout_base_reclaim(symbol, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    # LỌC TRÙNG KÈO (Chỉ phát 1 lần trong 4 tiếng cho mỗi symbol)
    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 14400:
        return None

    signal_timeframe = "H1"

    # 1. H4 CONTEXT FILTER ONLY (Chỉ dùng làm bối cảnh)
    h4_candles = get_closed_klines(symbol, "240", limit=60)
    if len(h4_candles) < 30: return None

    ema89_h4 = calculate_ema(h4_candles, 89)
    last_h4 = h4_candles[-1]
    curr_h4_close = last_h4['close']
    
    recent_h4_lows = [c['low'] for c in h4_candles[-12:-1]]
    prev_h4_lows = [c['low'] for c in h4_candles[-24:-12]]
    recent_h4_highs = [c['high'] for c in h4_candles[-12:-1]]
    prev_h4_highs = [c['high'] for c in h4_candles[-24:-12]]

    is_h4_dumping = (min(recent_h4_lows) < min(prev_h4_lows)) and (max(recent_h4_highs) < max(prev_h4_highs))
    
    h4_pass = False
    h4_score = 0
    if ema89_h4 and curr_h4_close > ema89_h4[-1]:
        h4_pass, h4_score = True, 15
    elif min(recent_h4_lows) >= min(prev_h4_lows):
        h4_pass, h4_score = True, 12
    elif not is_h4_dumping:
        h4_pass, h4_score = True, 10

    if not h4_pass or (is_h4_dumping and ema89_h4 and curr_h4_close < ema89_h4[-1]):
        return None

    # 2. PHÁT HIỆN H1 BREAKOUT
    h1_candles = get_closed_klines(symbol, "60", limit=60)
    if len(h1_candles) < 30: return None

    recent_h1_highs = [c['high'] for c in h1_candles[-40:-10]]
    if not recent_h1_highs: return None
    breakout_level = max(recent_h1_highs)

    breakout_idx = -1
    breakout_candle = None

    for idx in range(-10, -2):
        candle = h1_candles[idx]
        if candle['close'] > breakout_level:
            avg_vol = calculate_sma_volume(h1_candles[:idx], 20)
            if candle['volume'] >= avg_vol * 1.1:
                breakout_idx = idx
                breakout_candle = candle
                break

    if not breakout_candle: return None

    breakout_score = 20
    breakout_vol_ratio = breakout_candle['volume'] / calculate_sma_volume(h1_candles[:breakout_idx], 20) if calculate_sma_volume(h1_candles[:breakout_idx], 20) > 0 else 1.0

    # 3. PHÁT HIỆN H1 BASE SAU BREAKOUT
    base_candles = h1_candles[breakout_idx+1:]
    if len(base_candles) < 2 or len(base_candles) > 9: return None

    base_low = min([c['low'] for c in base_candles])
    base_high = max([c['high'] for c in base_candles])
    
    for bc in base_candles:
        if bc['close'] < breakout_level * 0.985: return None

    avg_base_vol = sum([c['volume'] for c in base_candles]) / len(base_candles)
    is_vol_drying = avg_base_vol < breakout_candle['volume']
    base_score = 15 if is_vol_drying else 10

    # 4. H1 RECLAIM ENTRY
    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']

    reclaim_type_a = (last_h1['low'] < breakout_level and curr_price >= breakout_level)
    reclaim_type_b = (last_h1['low'] >= base_low and curr_price > (base_low + base_high)/2 and last_h1['close'] > last_h1['open'])

    if not (reclaim_type_a or reclaim_type_b): return None
    reclaim_score = 20 if reclaim_type_a else 16

    # 5. FLOW CONFIRMATION
    taker_h1 = get_taker_buy_pct(symbol, "1h")
    cvd_score = 10 if taker_h1 >= 60.0 else (5 if taker_h1 >= 50.0 else -10)
    
    oi_pct = get_open_interest_pct(symbol, "1h")
    oi_score = 5 if oi_pct >= 1.5 else (3 if oi_pct >= 0 else -5)

    vol_score = 10 if breakout_vol_ratio >= 1.5 else 5
    if last_h1['volume'] > avg_base_vol: vol_score += 5
    btc_score = 5

    total_score = min(h4_score + breakout_score + base_score + reclaim_score + vol_score + cvd_score + oi_score + btc_score, 100)
    if total_score < MIN_WINRATE_SCORE: return None

    # 6. ANTI-CHASE FILTER H1
    dist_from_breakout = (curr_price - breakout_level) / breakout_level * 100
    if dist_from_breakout > 1.5 or curr_price > base_high * 1.012: return None

    entry_low = min(breakout_level, base_low)
    entry_high = curr_price
    entry_mid = curr_price

    # 7. SL CHUẨN H1 (SL DƯỚI BASE LOW + BUFFER ATR 0.35 TĂNG ĐỘ AN TOÀN)
    h1_atr = calculate_atr(h1_candles, 14)
    sl_price = base_low - (0.35 * h1_atr)
    risk = entry_mid - sl_price
    if risk <= 0: return None

    # 8. TÍNH TP CHUẨN THUẦN H1 (CẢN H1 CÓ LỰC)
    h1_targets = find_valid_timeframe_targets(h1_candles, entry_mid)

    tp1_price = h1_targets[0] if len(h1_targets) >= 1 else entry_mid + (h1_atr * 1.8)
    tp2_price = h1_targets[1] if len(h1_targets) >= 2 else tp1_price + (h1_atr * 2.5)
    tp3_price = h1_targets[2] if len(h1_targets) >= 3 else tp2_price + (h1_atr * 3.2)

    # 9. ROOM FILTER (ĐẢM BẢO WIN-RATE TP1 CAO)
    if (tp1_price - entry_mid) <= (risk * 1.0) or ((tp1_price - entry_mid)/entry_mid * 100) < 0.8: 
        return None

    setup_quality = "STRONG LONG SETUP" if total_score >= 85 else "LONG CANDIDATE"
    symbol_cooldowns[symbol] = now

    return {
        'signal_timeframe': 'H1',
        'engine_tag': 'H1 BREAKOUT BASE RECLAIM',
        'title': '🟢 H1 LONG',
        'symbol': symbol,
        'entry': entry_mid,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'sl': sl_price,
        'sl_reason': 'H1 Retest Base Low + ATR Buffer',
        'tp1': tp1_price,
        'tp1_reason': 'H1 Key Swing High',
        'tp2': tp2_price,
        'tp2_reason': 'H1 Major Resistance Zone',
        'tp3': tp3_price,
        'tp3_reason': 'H1 Supply Zone',
        'score': total_score,
        'setup_quality': setup_quality,
        'risk_pct': (risk / entry_mid) * 100,
        'breakout_level': breakout_level,
        'base_low': base_low,
        'vol_ratio': breakout_vol_ratio,
        'oi_pct': oi_pct,
        'taker_pct': taker_h1,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"H1_RECLAIM_{symbol}_{last_h1['timestamp']}"
    }

# ==============================================================================
# 6. TELEGRAM ALERT FORMATTER
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    sig['hit_tp1'] = False
    sig['hit_tp2'] = False
    sig['status_text'] = '⏳ Đang theo dõi real-time'
    
    signals_history.append(sig)
    active_signals.append(sig)

    tf = sig['signal_timeframe']

    msg = f"""<b>{sig['title']}</b>
#{sig['symbol']}

⭐ <b>Score: {sig['score']}/100</b>
🔥 <b>Setup: {sig['setup_quality']}</b>

📍 <b>Entry Zone:</b>
<code>{sig['entry_low']:.4f}</code> – <code>{sig['entry_high']:.4f}</code>

🛑 <b>{tf} SL:</b> <code>{sig['sl']:.4f}</code>
Reason: {sig['sl_reason']} (-{sig['risk_pct']:.2f}%)

🎯 <b>{tf} TP1:</b> <code>{sig['tp1']:.4f}</code>
Reason: {sig['tp1_reason']}

🎯 <b>{tf} TP2:</b> <code>{sig['tp2']:.4f}</code>
Reason: {sig['tp2_reason']}

🚀 <b>{tf} TP3:</b> <code>{sig['tp3']:.4f}</code>
Reason: {sig['tp3_reason']}

📊 <b>Confirmation:</b>
• {tf} Breakout & Base: ✓
• Volume: +{sig['vol_ratio']:.1f}x SMA
• Spot CVD: {sig['taker_pct']:.1f}%
• OI: +{sig['oi_pct']:.2f}%

📐 <b>Structure:</b>
{tf} Breakout → Base → Reclaim

🌐 <b>Context:</b> H4 Context Bullish

⚠️ <i>Không chase nếu giá rời Entry Zone quá xa.</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM] #{sig['id']} {sig['symbol']} [{tf} LONG] (Score: {sig['score']})", flush=True)

# ==============================================================================
# 7. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét Lọc Cấu Trúc Precise SL/TP (H1/H4)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 [SCANNING PRECISE TIMEFRAME] Đang quét {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                sig_h1 = evaluate_h1_breakout_base_reclaim(symbol, btc_h1)
                if sig_h1 and sig_h1['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig_h1['signal_id'])
                    send_telegram_signal(sig_h1)
                    candidates.append(sig_h1)
            except Exception:
                pass
            time.sleep(0.08)

        print(f"✅ Hoàn tất 1 lượt quét. Chờ {COOLDOWN_SECONDS}s quét tiếp...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
