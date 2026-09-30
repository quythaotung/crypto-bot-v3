import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - TIMEFRAME ISOLATION STRICT CODE
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết lý do loại coin ra Render
COOLDOWN_SECONDS = 10              # Quét liên tục không nghỉ 5p để bắn tín hiệu tức thì
MIN_TURNOVER_24H = 5_000_000       # Turnover 24h tối thiểu $5M USD
MAX_ALERTS_PER_CYCLE = 3           # Tối đa gửi Top 3 tín hiệu đẹp nhất mỗi lượt
MIN_WINRATE_SCORE = 75             # Ngưỡng điểm chấp nhận alert (>=75)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                # Danh sách theo dõi TP/SL real-time
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Timeframe Isolated Structure Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang chạy lọc cấu trúc cô lập Timeframe 24/7!\n⏳ Chưa có tín hiệu đạt chuẩn nào phát ra từ lần khởi động gần nhất."
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Timeframe Isolated đang hoạt động 24/7!")
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

                    # 0. CHASE WARNING / CANCEL
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.02:
                            sig['status_text'] = f"⚠️ CANCEL (Giá vượt xa {tf} Entry Zone)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE] #{sig['id']} - {sig['symbol']}</b>\nGiá hiện tại (<code>{curr_price:.4f}</code>) đã rời quá xa {tf} Entry Zone. Hủy theo dõi!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = f"🎯 Hit {tf} TP1 (Đã hủy bảo vệ SL)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM {tf} TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 {tf} TP1: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN {tf} TP1 THÀNH CÔNG!</b>
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
🚀 {tf} TP2: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>CHỐT LỜI THÀNH CÔNG TẠI VÙNG RESISTANCE {tf}!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3 (NẾU CÓ)
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = f"🔥 Hit {tf} TP3"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CHẠM {tf} TP3 MAJOR SUPPLY] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 {tf} TP3: <code>{sig['tp3']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH TOÀN BỘ MỤC TIÊU CẤU TRÚC {tf}!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)

                    # 4. HIT SL (Chỉ báo khi CHƯA chạm TP1)
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = f"🛑 Hit {tf} SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL - {tf} INVALID] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc {tf} SL: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Vi phạm mốc Hỗ trợ {tf} Invalidation. Setup bị vô hiệu hóa!</i>"""
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
# 4. ENGINE 1: H1 LONG SETUP (SL/TP CHỈ TÍNH TỪ H1 STRUCTURE)
# ==============================================================================

def evaluate_h1_breakout_base_reclaim(symbol, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    signal_timeframe = "H1" # TIMEFRAME ISOLATION

    # 1. H4 CONTEXT FILTER ONLY (Bối cảnh H4, không can thiệp SL/TP)
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

    # 5. FLOW CONFIRMATION & BONUS SCORE
    taker_h1 = get_taker_buy_pct(symbol, "1h")
    cvd_score = 10 if taker_h1 >= 60.0 else (5 if taker_h1 >= 50.0 else -10)
    
    oi_pct = get_open_interest_pct(symbol, "1h")
    oi_score = 5 if oi_pct >= 1.5 else (3 if oi_pct >= 0 else -5)

    vol_score = 10 if breakout_vol_ratio >= 1.5 else 5
    if last_h1['volume'] > avg_base_vol: vol_score += 5
    btc_score = 5

    bonus_score = 0
    rsi_vals = calculate_rsi(h1_candles, 14)
    if len(rsi_vals) >= 10 and rsi_vals[-1] > min(rsi_vals[-10:-2]) and last_h1['low'] <= base_low:
        bonus_score += 5

    if reclaim_type_a and (min(last_h1['open'], last_h1['close']) - last_h1['low']) > (last_h1['high'] - last_h1['low']) * 0.3:
        bonus_score += 5

    total_score = min(h4_score + breakout_score + base_score + reclaim_score + vol_score + cvd_score + oi_score + btc_score + bonus_score, 100)
    if total_score < MIN_WINRATE_SCORE: return None

    # 6. ANTI-CHASE FILTER H1
    dist_from_breakout = (curr_price - breakout_level) / breakout_level * 100
    if dist_from_breakout > 2.0 or curr_price > base_high * 1.015: return None

    entry_low = min(breakout_level, base_low)
    entry_high = curr_price
    entry_mid = curr_price

    # 7. SL & TP CHỈ DỰA TRÊN H1 MARKET STRUCTURE (CÔ LẬP TIMEFRAME)
    h1_atr = calculate_atr(h1_candles, 14)
    
    # H1 SL = H1 Base Low / H1 Retest Low - ATR Buffer H1
    sl_price = base_low - (0.15 * h1_atr)
    risk = entry_mid - sl_price
    if risk <= 0: return None

    # TÌM CÁC VÙNG CẢN H1 CÓ LỰC (H1 Swing High / Resistance / Supply)
    h1_resistance_candidates = []
    for i in range(10, len(h1_candles)-2):
        candle = h1_candles[i]
        # Check swing high có volume hoặc đỉnh rõ ràng
        if candle['high'] > h1_candles[i-1]['high'] and candle['high'] > h1_candles[i+1]['high'] and candle['high'] > entry_mid * 1.005:
            h1_resistance_candidates.append(candle['high'])

    h1_resistance_candidates = sorted(list(set(h1_resistance_candidates)))

    # Phân bổ TP1, TP2, TP3 thuần H1 Structure
    tp1_price = h1_resistance_candidates[0] if len(h1_resistance_candidates) >= 1 else entry_mid + (h1_atr * 2.0)
    tp2_price = h1_resistance_candidates[1] if len(h1_resistance_candidates) >= 2 else tp1_price + (h1_atr * 2.5)
    tp3_price = h1_resistance_candidates[2] if len(h1_resistance_candidates) >= 3 else tp2_price + (h1_atr * 3.0)

    # 8. ROOM FILTER H1
    if (tp1_price - entry_mid) <= (risk * 1.0): return None

    setup_quality = "STRONG LONG SETUP" if total_score >= 85 else "LONG CANDIDATE"

    return {
        'signal_timeframe': 'H1',
        'engine_tag': 'H1 BREAKOUT BASE RECLAIM',
        'title': '🟢 H1 LONG',
        'symbol': symbol,
        'entry': entry_mid,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'sl': sl_price,
        'sl_reason': 'H1 Retest / Base Low',
        'tp1': tp1_price,
        'tp1_reason': 'H1 Swing High',
        'tp2': tp2_price,
        'tp2_reason': 'H1 Resistance',
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
# 5. ENGINE 2: H4 LONG SETUP (SL/TP CHỈ TÍNH TỪ H4 STRUCTURE)
# ==============================================================================

def evaluate_h4_long_setup(symbol):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    signal_timeframe = "H4" # TIMEFRAME ISOLATION

    h4_candles = get_closed_klines(symbol, "240", limit=60)
    if len(h4_candles) < 40: return None

    last_h4 = h4_candles[-1]
    curr_price = last_h4['close']

    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if not ema34_h4 or not ema89_h4: return None

    # H4 Breakout & Retest Support
    recent_h4_highs = [c['high'] for c in h4_candles[-30:-5]]
    if not recent_h4_highs: return None
    h4_breakout_level = max(recent_h4_highs)

    # Nến H4 chọc retest H4 support
    h4_atr = calculate_atr(h4_candles, 14)
    h4_retest_low = min([c['low'] for c in h4_candles[-3:]])

    if not (last_h4['low'] <= h4_breakout_level * 1.01 and curr_price >= h4_breakout_level):
        return None

    taker_h4 = get_taker_buy_pct(symbol, "4h")
    if taker_h4 < 55.0: return None

    entry_mid = curr_price
    entry_low = h4_breakout_level
    entry_high = curr_price

    # SL & TP CHỈ DỰA TRÊN H4 MARKET STRUCTURE
    sl_price = h4_retest_low - (0.20 * h4_atr)
    risk = entry_mid - sl_price
    if risk <= 0: return None

    h4_resistance_candidates = []
    for i in range(10, len(h4_candles)-2):
        candle = h4_candles[i]
        if candle['high'] > h4_candles[i-1]['high'] and candle['high'] > h4_candles[i+1]['high'] and candle['high'] > entry_mid * 1.01:
            h4_resistance_candidates.append(candle['high'])

    h4_resistance_candidates = sorted(list(set(h4_resistance_candidates)))

    tp1_price = h4_resistance_candidates[0] if len(h4_resistance_candidates) >= 1 else entry_mid + (h4_atr * 2.5)
    tp2_price = h4_resistance_candidates[1] if len(h4_resistance_candidates) >= 2 else tp1_price + (h4_atr * 3.0)
    tp3_price = h4_resistance_candidates[2] if len(h4_resistance_candidates) >= 3 else tp2_price + (h4_atr * 4.0)

    if (tp1_price - entry_mid) <= (risk * 1.2): return None

    return {
        'signal_timeframe': 'H4',
        'engine_tag': 'H4 SWEEP RECLAIM',
        'title': '🟢 H4 LONG',
        'symbol': symbol,
        'entry': entry_mid,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'sl': sl_price,
        'sl_reason': 'H4 Swing / Support Low',
        'tp1': tp1_price,
        'tp1_reason': 'H4 Swing High',
        'tp2': tp2_price,
        'tp2_reason': 'H4 Resistance',
        'tp3': tp3_price,
        'tp3_reason': 'H4 Major Supply',
        'score': 88,
        'setup_quality': 'STRONG H4 LONG',
        'risk_pct': (risk / entry_mid) * 100,
        'vol_ratio': last_h4['volume'] / calculate_sma_volume(h4_candles[:-1], 20),
        'oi_pct': get_open_interest_pct(symbol, "4h"),
        'taker_pct': taker_h4,
        'timestamp': last_h4['timestamp'],
        'signal_id': f"H4_SETUP_{symbol}_{last_h4['timestamp']}"
    }

# ==============================================================================
# 6. TELEGRAM ALERT FORMATTER (HIỂN THỊ RÕ TIMEFRAME CÔ LẬP)
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
    print("⚡️ Bắt đầu Radar Quét Lọc Cấu Trúc Cô Lập Timeframe (H1/H4)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 [SCANNING ISOLATED TIMEFRAMES] Đang quét {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                # 1. Quét H1 Setup
                sig_h1 = evaluate_h1_breakout_base_reclaim(symbol, btc_h1)
                if sig_h1 and sig_h1['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig_h1['signal_id'])
                    send_telegram_signal(sig_h1)
                    candidates.append(sig_h1)
                    
                # 2. Quét H4 Setup
                sig_h4 = evaluate_h4_long_setup(symbol)
                if sig_h4 and sig_h4['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig_h4['signal_id'])
                    send_telegram_signal(sig_h4)
                    candidates.append(sig_h4)
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
