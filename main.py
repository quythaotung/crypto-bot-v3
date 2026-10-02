import time
import requests
import os
import numpy as np
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - STRATEGY: long-h1-h4-trend-flow-v1
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_TURNOVER_24H = 10_000_000      # $10M USD minimum 24h quote volume

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                
processed_signal_ids = set()
symbol_cooldowns = {}              
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot long-h1-h4-trend-flow-v1 Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL (PAPER TRADING):</b>\n\n🟢 Bot <code>long-h1-h4-trend-flow-v1</code> đang vận hành 24/7!\n⏳ Chưa có tín hiệu chuẩn Trend-Flow nào phát ra từ lần khởi động gần nhất."
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
        
        msg += f"<b>#{sig['id']} [{sig['setup_name']}] {sig['symbol']}</b>\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot strategy <code>long-h1-h4-trend-flow-v1</code> đang hoạt động 24/7!")
        except Exception as e:
            print(f"Lỗi luồng Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# 2. REAL-TIME MONITORING (PAPER TRADING TRACKER)
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

                    # CANCEL NẾU GIÁ CHẠY QÚA XA ENTRY ZONE BEFORE RETEST
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.018:
                            sig['status_text'] = f"⚠️ CANCEL (Vọt xa Entry Zone)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE PUMP] #{sig['id']} - {sig['symbol']}</b>\nGiá (<code>{curr_price:.4f}</code>) đã chạy vọt quá xa Entry Zone. Tự động hủy lệnh PAPER!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1 (Bảo toàn vốn)
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = f"🎯 Hit TP1 (Đã bảo toàn vốn)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 Target: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL. Dời SL về Breakeven để gồng TP2/TP3!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = f"🚀 Hit TP2"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CHẠM TP2 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 TP2 Target: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>CHỐT LỜI THÀNH CÔNG TẠI KHÁNG CỰ {tf}!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = f"🔥 Hit TP3 Major Target"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CHẠM TP3 MAJOR STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 TP3 Target: <code>{sig['tp3']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH MỤC TIÊU CẤU TRÚC THUẦN {tf}!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)

                    # 4. HIT SL
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = f"🛑 Hit SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL - INVALIDATED] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Vi phạm mốc Hỗ trợ Cấu trúc H1/H4. Dừng lệnh an toàn!</i>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)
                except Exception as inner_e:
                    print(f"Lỗi theo dõi coin {sig.get('symbol')}: {inner_e}", flush=True)
        except Exception as e:
            print(f"Lỗi theo dõi Tracker Loop: {e}", flush=True)
        
        time.sleep(10)

# ==============================================================================
# 3. BYBIT API & TECHNICAL INDICATORS
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            excluded = ['3L', '3S', '2L', '2S', 'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT']
            parsed_list = []
            for t in tickers:
                sym = t['symbol']
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and not any(x in sym for x in excluded):
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

def get_closed_klines(symbol, interval, category="linear", limit=200):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            closed_candles = raw_list[1:] # Loại bỏ nến đang chạy
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
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=4"
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

def get_taker_buy_ratios(symbol, category="linear", interval_str="1h", limit=6):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period={interval_str}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def calculate_cvd_series(candles, taker_buy_ratios):
    """
    Tính toán Delta từng nến:
    delta = 2 * taker_buy_quote - quote_volume
    Trả về danh sách CVD tích lũy.
    """
    cvd_series = []
    current_cvd = 0.0
    for i, c in enumerate(candles[-len(taker_buy_ratios):]):
        ratio = taker_buy_ratios[i] / 100.0 if i < len(taker_buy_ratios) else 0.5
        quote_vol = c['turnover']
        taker_buy_quote = quote_vol * ratio
        delta = (2 * taker_buy_quote) - quote_vol
        current_cvd += delta
        cvd_series.append(current_cvd)
    return cvd_series

def find_valid_structure_targets(candles, entry_price):
    swing_highs = []
    for i in range(2, len(candles) - 2):
        c = candles[i]
        if (c['high'] > candles[i-1]['high'] and c['high'] > candles[i-2]['high'] and
            c['high'] > candles[i+1]['high'] and c['high'] > candles[i+2]['high']):
            if c['high'] > entry_price * 1.008:
                swing_highs.append(c['high'])
                
    unique_targets = []
    for target in sorted(swing_highs):
        if not unique_targets or (target - unique_targets[-1]) / unique_targets[-1] * 100 >= 0.5:
            unique_targets.append(target)
            
    return unique_targets

# ==============================================================================
# 4. HARD GATE & FLOW EVALUATION (STRATEGY: long-h1-h4-trend-flow-v1)
# ==============================================================================

def check_h4_hard_gate(h4_candles, btc_h4_candles):
    """
    Kiem tra Hard Gate H4 cho chieu LONG:
    1. Giá đóng H4 > EMA34 H4 và EMA89 H4
    2. EMA34 H4 > EMA89 H4
    3. EMA34 & EMA89 H4 không dốc xuống (EMA34_curr > EMA34_t-3)
    4. Cấu trúc H4 chưa tạo Lower Low mới (Higher Low / Holding)
    5. BTC không rơi tự do
    """
    if len(h4_candles) < 90: return False, "Thiếu dữ liệu H4"

    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)

    if not ema34_h4 or not ema89_h4: return False, "Lỗi EMA H4"

    last_h4 = h4_candles[-1]
    curr_close = last_h4['close']

    # 1 & 2. Kiem tra vi tri EMA
    if not (curr_close > ema34_h4[-1] and curr_close > ema89_h4[-1] and ema34_h4[-1] > ema89_h4[-1]):
        return False, "Không thỏa Price > EMA34 > EMA89 H4"

    # 3. Kiem tra do doc EMA34
    if len(ema34_h4) >= 4 and ema34_h4[-1] <= ema34_h4[-4]:
        return False, "EMA34 H4 đang dốc xuống"

    # 4. Kiem tra Structure Lower Low H4
    recent_lows = [c['low'] for c in h4_candles[-12:-1]]
    prev_lows = [c['low'] for c in h4_candles[-24:-12]]
    if recent_lows and prev_lows and min(recent_lows) < min(prev_lows):
        return False, "H4 vùa tao Lower Low moi"

    # 5. BTC Context Check
    if btc_h4_candles:
        btc_ema34 = calculate_ema(btc_h4_candles, 34)
        if btc_ema34 and btc_h4_candles[-1]['close'] < btc_ema34[-1] * 0.975:
            return False, "BTC đang o trang thai RISK_OFF"

    return True, "PASSED H4 HARD GATE"

def check_h1_hard_gate(h1_candles):
    """
    Kiem tra Hard Gate H1:
    1. Giá đóng H1 > EMA89 H1
    2. EMA34 H1 >= EMA89 H1
    3. Không LONG nếu H1 vừa đóng dưới EMA89
    """
    if len(h1_candles) < 90: return False

    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)

    if not ema34_h1 or not ema89_h1: return False

    last_h1 = h1_candles[-1]
    if last_h1['close'] <= ema89_h1[-1]: return False
    if ema34_h1[-1] < ema89_h1[-1]: return False

    return True

def evaluate_flow_conditions(symbol, h1_candles, futures_taker, spot_taker, spot_candles):
    """
    Danh gia Bo loc Dong Tien & CVD (Toi thieu 4/6 dieu kien):
    1. Spot taker-buy ratio >= 53% (ít nhat 2/3 nén H1)
    2. Spot CVD H1 slope duong
    3. Futures taker-buy ratio >= 52%
    4. Futures CVD H1 không tao lower-low
    5. Spot volume H1 >= 1.2x median volume 20 nén H1
    6. Pullback CVD giu Higher Low
    """
    score_count = 0

    # 1. Spot Taker Ratio >= 53% (2/3 nén)
    high_spot_count = sum(1 for r in spot_taker[:3] if r >= 53.0)
    if high_spot_count >= 2: score_count += 1

    # 2. Spot CVD Slope Dương
    spot_cvd = calculate_cvd_series(spot_candles, spot_taker) if spot_candles else []
    if len(spot_cvd) >= 3 and spot_cvd[-1] > spot_cvd[-3]: score_count += 1

    # 3. Futures Taker Ratio >= 52%
    if futures_taker and futures_taker[0] >= 52.0: score_count += 1

    # 4. Futures CVD không tạo Lower Low
    futures_cvd = calculate_cvd_series(h1_candles, futures_taker)
    if len(futures_cvd) >= 3 and futures_cvd[-1] >= min(futures_cvd): score_count += 1

    # 5. Spot Volume H1 >= 1.2x Median 20 nến
    if spot_candles and len(spot_candles) >= 20:
        vols = [c['volume'] for c in spot_candles[-20:]]
        median_vol = float(np.median(vols))
        if spot_candles[-1]['volume'] >= 1.2 * median_vol: score_count += 1

    # 6. Pullback CVD Higher Low
    if len(futures_cvd) >= 4 and futures_cvd[-1] > futures_cvd[-4]: score_count += 1

    # Hard Rejects Check
    if len(spot_cvd) >= 2 and len(futures_cvd) >= 2:
        if (spot_cvd[-1] < spot_cvd[-2]) and (futures_cvd[-1] < futures_cvd[-2]):
            return False, "HARD REJECT: Spot & Futures CVD cung giam"

    return (score_count >= 4), f"Flow Score: {score_count}/6"

# ==============================================================================
# 5. EVALUATE SETUPS (SETUP A & SETUP B)
# ==============================================================================

def evaluate_long_h1_h4_strategy(ticker_data, btc_h4_candles):
    symbol = ticker_data['symbol']
    turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    # 0. CHECK TURNOVER & FUNDING RATE HARD GATE (-0.03% den +0.04%)
    if turnover < MIN_TURNOVER_24H: return None
    if not (-0.03 <= funding <= 0.04):
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Funding Rate ({funding:+.3f}%) ngoai khoang cho phep", flush=True)
        return None

    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 14400: return None

    # Fetch closed klines
    h4_candles = get_closed_klines(symbol, "240", category="linear", limit=100)
    h1_candles = get_closed_klines(symbol, "60", category="linear", limit=100)
    spot_h1_candles = get_closed_klines(symbol, "60", category="spot", limit=60)

    if not h4_candles or not h1_candles: return None

    # 1. HARD GATE H4 & H1
    h4_pass, h4_msg = check_h4_hard_gate(h4_candles, btc_h4_candles)
    if not h4_pass:
        if DEBUG_MODE: print(f"[{symbol}] ⏳ WATCH ONLY: {h4_msg}", flush=True)
        return None

    if not check_h1_hard_gate(h1_candles):
        if DEBUG_MODE: print(f"[{symbol}] ⏳ WATCH ONLY: Khong thoa H1 Hard Gate", flush=True)
        return None

    # Order flow data
    futures_taker = get_taker_buy_ratios(symbol, category="linear", interval_str="1h", limit=6)
    spot_taker = get_taker_buy_ratios(symbol, category="spot", interval_str="1h", limit=6)

    flow_pass, flow_msg = evaluate_flow_conditions(symbol, h1_candles, futures_taker, spot_taker, spot_h1_candles)
    if not flow_pass:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT FLOW: {flow_msg}", flush=True)
        return None

    # Technical Indicators
    rsi_h1 = calculate_rsi(h1_candles, 14)
    rsi_h4 = calculate_rsi(h4_candles, 14)
    ema34_h1 = calculate_ema(h1_candles, 34)
    h1_atr = calculate_atr(h1_candles, 14)

    if not rsi_h1 or not rsi_h4 or not ema34_h1 or h1_atr <= 0: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']

    # RSI FILTERS: RESTRICTION CHECK
    if rsi_h1[-1] > 68.0 and (curr_price - ema34_h1[-1])/ema34_h1[-1] * 100 > 2.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT RSI: H1 RSI > 68 qua xa ho tro", flush=True)
        return None
    if rsi_h4[-1] > 72.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT RSI: H4 RSI > 72 (Overbought)", flush=True)
        return None

    setup_type = None
    setup_name = ""

    # --------------------------------------------------------------------------
    # EVALUATE SETUP B — H4 Swing Continuation (Ưu tiên cao hơn)
    # --------------------------------------------------------------------------
    is_setup_b = False
    if len(rsi_h4) >= 4:
        rsi_h4_recent = rsi_h4[-4:-1]
        if any(45.0 <= r <= 50.0 for r in rsi_h4_recent) and rsi_h4[-1] > 50.0:
            if last_h1['close'] > ema34_h1[-1]:
                is_setup_b = True
                setup_type = "SETUP_B"
                setup_name = "H4 SWING CONTINUATION"

    # --------------------------------------------------------------------------
    # EVALUATE SETUP A — H1 Trend Pullback (Nếu không đạt Setup B)
    # --------------------------------------------------------------------------
    is_setup_a = False
    if not is_setup_b and len(rsi_h1) >= 4:
        rsi_h1_recent = rsi_h1[-4:-1]
        if any(45.0 <= r <= 50.0 for r in rsi_h1_recent) and rsi_h1[-1] > 50.0 and rsi_h1[-1] > rsi_h1[-2]:
            # Trigger candle closed above support / EMA34
            if last_h1['close'] >= ema34_h1[-1] * 0.998:
                is_setup_a = True
                setup_type = "SETUP_A"
                setup_name = "H1 TREND PULLBACK"

    if not (is_setup_a or is_setup_b): return None

    # --------------------------------------------------------------------------
    # CALCULATE ENTRY, SL & REAL STRUCTURE TARGETS
    # --------------------------------------------------------------------------
    # Support & Retest Lows
    recent_h1_lows = [c['low'] for c in h1_candles[-6:]]
    support_low = min(recent_h1_lows)
    
    # SL dat duoi diem sai cau truc + ATR buffer (0.25 * ATR)
    sl_price = support_low - (0.25 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    # Strict SL Rules
    if risk_pct > 3.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT SL: Risk qua lon ({risk_pct:.2f}% > 3.0%)", flush=True)
        return None

    # Finder Target
    h1_targets = find_valid_structure_targets(h1_candles, curr_price)
    if not h1_targets:
        fallback_tp1 = curr_price + (h1_atr * 2.2)
        fallback_tp2 = fallback_tp1 + (h1_atr * 2.8)
        tp1_price, tp2_price = fallback_tp1, fallback_tp2
    else:
        tp1_price = h1_targets[0]
        tp2_price = h1_targets[1] if len(h1_targets) >= 2 else tp1_price + (h1_atr * 2.5)

    tp3_price = tp2_price + (h1_atr * 3.0)

    # Risk Reward Verification (Minimum TP1 RR >= 1.5)
    tp1_reward = (tp1_price - curr_price) / curr_price * 100
    if tp1_reward / risk_pct < 1.3:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT RR: Tỷ le R:R TP1 qua thap ({tp1_reward/risk_pct:.2f}R)", flush=True)
        return None

    symbol_cooldowns[symbol] = now

    return {
        'signal_timeframe': 'H1',
        'strategy': 'long-h1-h4-trend-flow-v1',
        'setup_type': setup_type,
        'setup_name': setup_name,
        'symbol': symbol,
        'entry': curr_price,
        'entry_low': max(support_low, curr_price - (0.2 * h1_atr)),
        'entry_high': curr_price,
        'sl': sl_price,
        'sl_reason': 'H1 Structural Support Low + ATR Buffer',
        'tp1': tp1_price,
        'tp1_reason': 'H1 Swing High Resistance',
        'tp2': tp2_price,
        'tp2_reason': 'H1 Major Resistance Target',
        'tp3': tp3_price,
        'tp3_reason': 'H4 Supply Target',
        'risk_pct': risk_pct,
        'funding': funding,
        'rsi_h1': rsi_h1[-1],
        'rsi_h4': rsi_h4[-1],
        'spot_taker': spot_taker[0] if spot_taker else 50.0,
        'futures_taker': futures_taker[0] if futures_taker else 50.0,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"TREND_FLOW_{symbol}_{last_h1['timestamp']}"
    }

# ==============================================================================
# 6. TELEGRAM SIGNAL FORMATTER
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    sig['hit_tp1'] = False
    sig['hit_tp2'] = False
    sig['status_text'] = '⏳ Đang theo dõi real-time (PAPER)'
    
    signals_history.append(sig)
    active_signals.append(sig)

    msg = f"""🔥 <b>[PAPER TRADING] LONG SIGNAL</b>
<b>Strategy:</b> <code>long-h1-h4-trend-flow-v1</code>

#{sig['symbol']}

🟢 <b>Setup: {sig['setup_name']} ({sig['setup_type']})</b>

📍 <b>Entry Zone:</b>
<code>{sig['entry_low']:.4f}</code> – <code>{sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code>
Reason: {sig['sl_reason']} (-{sig['risk_pct']:.2f}%)

🎯 <b>TP1 Target:</b> <code>{sig['tp1']:.4f}</code>
Reason: {sig['tp1_reason']}

🎯 <b>TP2 Target:</b> <code>{sig['tp2']:.4f}</code>
Reason: {sig['tp2_reason']}

🚀 <b>TP3 Target:</b> <code>{sig['tp3']:.4f}</code>
Reason: {sig['tp3_reason']}

📊 <b>Metrics & Order Flow:</b>
• RSI H1: <code>{sig['rsi_h1']:.1f}</code> | RSI H4: <code>{sig['rsi_h4']:.1f}</code>
• Spot Taker Buy: <code>{sig['spot_taker']:.1f}%</code>
• Futures Taker Buy: <code>{sig['futures_taker']:.1f}%</code>
• Funding Rate: <code>{sig['funding']:+.3f}%</code>

📐 <b>Structure Alignment:</b>
H4 Uptrend Hard Gate PASSED → H1 Trigger Retest

⚠️ <i>Lệnh thử nghiệm PAPER TRADING. Không chase nếu rời Entry Zone.</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM] #{sig['id']} {sig['symbol']} [{sig['setup_name']}]", flush=True)

# ==============================================================================
# 7. MAIN RADAR LOOP
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Strategy: long-h1-h4-trend-flow-v1...", flush=True)
    while True:
        btc_h4 = get_closed_klines("BTCUSDT", "240", category="linear", limit=50)
        all_tickers = get_all_bybit_usdt_symbols()
        print(f"🔍 [TREND FLOW SCAN] Đang rà soát Hard Gate & Trigger cho {len(all_tickers)} coin...", flush=True)
        
        candidates = []
        for ticker in all_tickers:
            try:
                sig = evaluate_long_h1_h4_strategy(ticker, btc_h4)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
                    candidates.append(sig)
            except Exception as e:
                pass
            time.sleep(0.08)

        print(f"✅ Hoàn tất 1 lượt quét. Chờ {COOLDOWN_SECONDS}s quét tiếp...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
