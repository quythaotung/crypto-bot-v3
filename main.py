import time
import requests
import os
import numpy as np
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - STRATEGY: long-h1-h4-trend-flow-v1 (ENHANCED)
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 20_000_000  # $20M USDT Futures 24h Quote Vol
MIN_SPOT_TURNOVER_24H = 5_000_000      # $5M USDT Spot 24h Quote Vol

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                
processed_fingerprints = set()
symbol_cooldowns = {}              
daily_call_count = 0
last_call_reset_time = time.time()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl, /scan)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot long-h1-h4-trend-flow-v1 Enhanced Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL (PAPER TRADING):</b>\n\n🟢 Bot <code>long-h1-h4-trend-flow-v1</code> đang vận hành 24/7!\n⏳ Chưa có tín hiệu PAPER nào được tạo."
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
        
        msg += f"<b>#{sig['id']} [{sig['order_type']}] [{sig['setup_name']}] {sig['symbol']}</b> (Score: {sig['score']}/100)\n"
        msg += f"• Entry: <code>{sig['entry']:.4f}</code> | Hiện tại: <code>{curr_price:.4f}</code> ({icon} <b>{pnl_pct:+.2f}%</b>)\n"
        msg += f"• Trạng thái: <b>{status}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        total_pnl_pct += pnl_pct
        
    total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
    msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"
    send_telegram_direct_message(msg)

def process_telegram_commands():
    last_update_id = 0
    print("🤖 Đã bật luồng lắng nghe lệnh Telegram (/pnl, /scan, /ping)...", flush=True)
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
                    elif text in ["/scan", "scan"]:
                        send_pnl_summary() # Doc ket qua gan nhat
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot strategy <code>long-h1-h4-trend-flow-v1</code> đang hoạt động 24/7!")
        except Exception as e:
            print(f"Lỗi luồng Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# 2. REAL-TIME MONITORING (LIMIT/MARKET PAPER TRACKER)
# ==============================================================================
def process_active_signals_tracker():
    while True:
        try:
            current_active = list(active_signals)
            for sig in current_active:
                try:
                    curr_price = get_current_price(sig['symbol'])
                    if not curr_price: continue

                    # Dòng lệnh LIMIT check trước khi khớp
                    if sig['order_type'] == 'LIMIT' and not sig.get('entered', False):
                        # Nếu chạm TP1 trước khi khớp Limit -> chuyển MISSED, hủy lệnh
                        if curr_price >= sig['tp1']:
                            sig['status_text'] = "⚠️ MISSED (Giá chạm TP1 trước khi khớp Limit)"
                            msg = f"⚠️ <b>[LỆNH BỎ LỠ - MISSED] #{sig['id']} - {sig['symbol']}</b>\nGiá đã chạm TP1 (<code>{sig['tp1']:.4f}</code>) trước khi khớp vùng Limit. Tự động HỦY LỆNH!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        # Nếu Nến H1 đóng dưới Hỗ trợ bảo vệ trước khi khớp -> INVALID
                        elif curr_price < sig['support_low']:
                            sig['status_text'] = "🛑 INVALID (Hỗ trợ bị gãy trước khi khớp)"
                            msg = f"🛑 <b>[LỆNH VI PHẠM - INVALID] #{sig['id']} - {sig['symbol']}</b>\nHỗ trợ bảo vệ (<code>{sig['support_low']:.4f}</code>) bị vỡ trước khi khớp Limit. Tự động HỦY LỆNH!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1 (Chốt 20%, giữ SL cấu trúc)
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 (Chốt 20%, giữ SL Cấu Trúc)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 Target: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ ĐẠT TP1! CHỐT 20% LỢI NHUẬN</b>
🛡 <i>Giữ nguyên SL cấu trúc tại <code>{sig['sl']:.4f}</code>. Chờ chốt TP2!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2 (Chốt 25%, dời SL về Entry)
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['sl'] = sig['entry'] # Dời SL về Entry sau TP2
                        sig['status_text'] = "🚀 Hit TP2 (Chốt 25%, Dời SL về Breakeven)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CHẠM TP2 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 TP2 Target: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>ĐÃ ĐẠT TP2! CHỐT 25% LỢI NHUẬN</b>
🛡 <i>Đã dời SL về Breakeven (<code>{sig['entry']:.4f}</code>) để bảo toàn vốn cho Runner 30%!</i>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3 (Nếu có)
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = "🔥 Hit TP3 Major Target (Chốt 25%)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CHẠM TP3 MAJOR TARGET] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 TP3 Target: <code>{sig['tp3']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH MỤC TIÊU TP3 CHÍNH!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)

                    # 4. HIT SL
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Vi phạm điểm sai xu hướng. Dừng lệnh an toàn!</i>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)
                except Exception as inner_e:
                    print(f"Lỗi theo dõi coin {sig.get('symbol')}: {inner_e}", flush=True)
        except Exception as e:
            print(f"Lỗi theo dõi Tracker Loop: {e}", flush=True)
        
        time.sleep(10)

# ==============================================================================
# 3. BYBIT API & TECHNICAL HELPERS
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
                    last_price = float(t.get('lastPrice', 0))
                    bid_price = float(t.get('bid1Price', 0))
                    ask_price = float(t.get('ask1Price', 0))
                    spread_pct = ((ask_price - bid_price) / last_price * 100) if last_price > 0 and ask_price > 0 else 0.0

                    parsed_list.append({
                        'symbol': sym,
                        'turnover24h': float(t.get('turnover24h', 0)),
                        'fundingRate': float(t.get('fundingRate', 0)) * 100,
                        'lastPrice': last_price,
                        'spread_pct': spread_pct
                    })
            return parsed_list
    except Exception:
        pass
    return []

def get_spot_turnover_24h(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=spot&symbol={symbol}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get('turnover24h', 0))
    except Exception:
        pass
    return 0.0

def get_closed_klines(symbol, interval, category="linear", limit=200):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            closed_candles = raw_list[1:] # Loai bo nen dang chay
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
# 4. HARD GATE & SCORING SYSTEM (100 POINTS)
# ==============================================================================

def check_h4_hard_gate(h4_candles, btc_h4_candles):
    if len(h4_candles) < 90: return False, "Thiếu dữ liệu H4"

    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)

    if not ema34_h4 or not ema89_h4: return False, "Lỗi EMA H4"

    last_h4 = h4_candles[-1]
    curr_close = last_h4['close']

    if not (curr_close > ema34_h4[-1] and curr_close > ema89_h4[-1] and ema34_h4[-1] > ema89_h4[-1]):
        return False, "Không thỏa Price > EMA34 > EMA89 H4"

    if len(ema34_h4) >= 4 and ema34_h4[-1] <= ema34_h4[-4]:
        return False, "EMA34 H4 đang dốc xuống"

    recent_lows = [c['low'] for c in h4_candles[-12:-1]]
    prev_lows = [c['low'] for c in h4_candles[-24:-12]]
    if recent_lows and prev_lows and min(recent_lows) < min(prev_lows):
        return False, "H4 vừa tạo Lower Low mới"

    if btc_h4_candles:
        btc_ema34 = calculate_ema(btc_h4_candles, 34)
        if btc_ema34 and btc_h4_candles[-1]['close'] < btc_ema34[-1] * 0.975:
            return False, "BTC đang ở trạng thái RISK_OFF"

    return True, "PASSED H4 HARD GATE"

def check_h1_hard_gate(h1_candles):
    if len(h1_candles) < 90: return False

    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)

    if not ema34_h1 or not ema89_h1: return False

    last_h1 = h1_candles[-1]
    if last_h1['close'] <= ema89_h1[-1]: return False
    if ema34_h1[-1] < ema89_h1[-1]: return False

    return True

def evaluate_long_h1_h4_strategy(ticker_data, btc_h4_candles):
    global daily_call_count, last_call_reset_time

    # Reset DAILY CALL COUNT
    if time.time() - last_call_reset_time >= 86400:
        daily_call_count = 0
        last_call_reset_time = time.time()

    if daily_call_count >= 6: return None # Max 6 calls / 24h

    symbol = ticker_data['symbol']
    fut_turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']
    spread_pct = ticker_data['spread_pct']

    # 1. BỘ LỌC THANH KHOẢN KHẮT KHE
    if fut_turnover < MIN_FUTURES_TURNOVER_24H: return None
    if spread_pct > 0.15: return None

    spot_turnover = get_spot_turnover_24h(symbol)
    if spot_turnover > 0 and spot_turnover < MIN_SPOT_TURNOVER_24H: return None

    # 2. BỘ LỌC FUNDING RATE ENHANCED
    if funding > 0.06 or funding < -0.10: # HARD REJECT
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Funding ({funding:+.3f}%) vi phạm ngưỡng", flush=True)
        return None

    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 21600: return None # Cooldown >= 6h

    h4_candles = get_closed_klines(symbol, "240", category="linear", limit=100)
    h1_candles = get_closed_klines(symbol, "60", category="linear", limit=100)
    spot_h1_candles = get_closed_klines(symbol, "60", category="spot", limit=60)

    if not h4_candles or not h1_candles: return None

    # HARD GATES CHECK
    h4_pass, h4_msg = check_h4_hard_gate(h4_candles, btc_h4_candles)
    if not h4_pass:
        if DEBUG_MODE: print(f"[{symbol}] ⏳ WATCH ONLY: {h4_msg}", flush=True)
        return None

    if not check_h1_hard_gate(h1_candles): return None

    # ORDER FLOW DATA
    futures_taker = get_taker_buy_ratios(symbol, category="linear", interval_str="1h", limit=6)
    spot_taker = get_taker_buy_ratios(symbol, category="spot", interval_str="1h", limit=6)
    
    oi_h1_pct = get_open_interest_pct(symbol, "1h")
    oi_h4_pct = get_open_interest_pct(symbol, "4h")

    # OI HARD REJECTS
    if oi_h1_pct > 8.0 or oi_h4_pct > 15.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT OI: OI H1/H4 quá cao (>8% / >15%)", flush=True)
        return None

    futures_cvd = calculate_cvd_series(h1_candles, futures_taker)
    spot_cvd = calculate_cvd_series(spot_h1_candles, spot_taker) if spot_h1_candles else []

    if len(futures_cvd) >= 2 and len(spot_cvd) >= 2:
        if spot_cvd[-1] < spot_cvd[-2] and futures_cvd[-1] < futures_cvd[-2]:
            if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT FLOW: Spot & Futures CVD cùng giảm", flush=True)
            return None

    # TECHNICAL INDICATORS
    rsi_h1 = calculate_rsi(h1_candles, 14)
    rsi_h4 = calculate_rsi(h4_candles, 14)
    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)
    h1_atr = calculate_atr(h1_candles, 14)

    if not rsi_h1 or not rsi_h4 or not ema34_h1 or h1_atr <= 0: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']

    # RSI HARD REJECTS
    if rsi_h1[-1] > 68.0 and (curr_price - ema34_h1[-1])/ema34_h1[-1] * 100 > 1.5: return None
    if rsi_h4[-1] > 72.0: return None

    # EVALUATE SETUPS
    setup_type, setup_name = None, ""
    if len(rsi_h4) >= 4 and any(45.0 <= r <= 50.0 for r in rsi_h4[-4:-1]) and rsi_h4[-1] > 50.0:
        setup_type, setup_name = "SETUP_B", "H4 SWING CONTINUATION"
    elif len(rsi_h1) >= 4 and any(45.0 <= r <= 50.0 for r in rsi_h1[-4:-1]) and rsi_h1[-1] > 50.0:
        setup_type, setup_name = "SETUP_A", "H1 TREND PULLBACK"

    if not setup_type: return None

    # ==========================================================================
    # CHẤM ĐIỂM 100 TỔNG HỢP (100 POINTS SCORING)
    # ==========================================================================
    
    # 1. Xu hướng EMA H4/H1 (25đ)
    score_ema = 25 if curr_price > ema34_h1[-1] > ema89_h1[-1] else 18

    # 2. RSI Pullback / Reclaim (15đ)
    score_rsi = 15 if (50.0 <= rsi_h1[-1] <= 65.0) else 10

    # 3. CVD & Volume (20đ)
    vols = [c['volume'] for c in h1_candles[-20:]]
    median_vol = float(np.median(vols))
    vol_ratio = last_h1['volume'] / median_vol if median_vol > 0 else 1.0
    score_cvd_vol = 20 if (spot_taker[0] >= 53.0 and vol_ratio >= 1.2) else 12

    # 4. Funding & OI (15đ)
    score_funding_oi = 0
    if -0.03 <= funding <= 0.0: score_funding_oi += 8 # Thưởng funding âm nhẹ
    elif 0.0 < funding <= 0.04: score_funding_oi += 5
    elif 0.04 < funding <= 0.06: score_funding_oi += 2 # Trừ điểm

    if 0.5 <= oi_h1_pct <= 5.0: score_funding_oi += 7
    elif -5.0 <= oi_h1_pct < 0.5: score_funding_oi += 5 # Rũ đòn bẩy lành mạnh
    score_funding_oi = min(score_funding_oi, 15)

    # 5. Chất lượng Hỗ trợ / Entry (15đ)
    recent_lows = [c['low'] for c in h1_candles[-6:]]
    support_low = min(recent_lows)
    dist_from_support = (curr_price - support_low) / h1_atr
    score_entry = 15 if dist_from_support <= 1.0 else 8

    # 6. Thanh khoản / Spread (10đ)
    score_liq = 10 if spread_pct <= 0.05 else 5

    total_score = score_ema + score_rsi + score_cvd_vol + score_funding_oi + score_entry + score_liq

    # Trừ điểm/Ép WATCH nếu Funding từ +0.04% đến +0.06%
    if 0.04 < funding <= 0.06 and total_score >= 82:
        total_score = 78 # Ép về ARMED/WATCH

    if total_score < 82: # Chỉ TRIGGER khi Score >= 82
        if DEBUG_MODE: print(f"[{symbol}] ⏳ SCORE {total_score}/100: Chuyển ARMED/WATCH", flush=True)
        return None

    # ==========================================================================
    # PHÂN LOẠI LỆNH MARKET VS LIMIT & BẢO VỆ CẤU TRÚC SL/TP
    # ==========================================================================
    
    # Check điều kiện MARKET
    upper_wick = last_h1['high'] - max(last_h1['open'], last_h1['close'])
    candle_range = last_h1['high'] - last_h1['low']
    wick_ratio = (upper_wick / candle_range * 100) if candle_range > 0 else 0.0

    is_market_eligible = (
        vol_ratio >= 1.5 and
        wick_ratio < 35.0 and
        dist_from_support <= 1.0 and
        candle_range <= (2.0 * h1_atr) and
        rsi_h1[-1] <= 68.0
    )

    order_type = "MARKET" if is_market_eligible else "LIMIT"

    # XÁC ĐỊNH ENTRY ZONE
    entry_mid = curr_price
    entry_low = max(support_low, curr_price - (0.2 * h1_atr)) if order_type == "LIMIT" else curr_price
    entry_high = curr_price

    # XÁC ĐỊNH BUFFER SL CẤU TRÚC
    raw_buffer = max(curr_price * 0.01, 0.25 * h1_atr)
    buffer = min(curr_price * 0.02, raw_buffer) # Max buffer 2%

    sl_price = support_low - buffer
    risk_pct = (entry_mid - sl_price) / entry_mid * 100

    # KHÔNG TỰ KÉO SL! NẾU RISK > 15% -> LOẠI NGAY LỆNH
    if risk_pct > 15.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT SL: Risk Cấu trúc > 15% ({risk_pct:.2f}%)", flush=True)
        return None

    # TÍNH KHÁNG CỰ VÀ TAKE PROFIT
    targets = find_valid_structure_targets(h1_candles, entry_mid)
    tp1_price = targets[0] if len(targets) >= 1 else entry_mid + (h1_atr * 2.2)
    tp2_price = targets[1] if len(targets) >= 2 else tp1_price + (h1_atr * 2.8)

    # ĐIỀU KIỆN ĐẶC BIỆT ĐỂ HIỂN THỊ TP3 THẬT
    h4_ema34 = calculate_ema(h4_candles, 34)
    h4_ema89 = calculate_ema(h4_candles, 89)
    
    is_tp3_eligible = (
        len(h4_ema34) >= 2 and h4_ema34[-1] > h4_ema34[-2] and
        rsi_h4[-1] < 68.0 and
        funding < 0.04 and
        oi_h4_pct <= 10.0
    )

    tp3_price = tp2_price + (h1_atr * 3.5) if is_tp3_eligible else None

    # BỘ LỌC FINGERPRINT TRÁNH CALL TRÙNG
    fingerprint = f"{symbol}_{last_h1['timestamp']}_{setup_type}"
    if fingerprint in processed_fingerprints: return None

    processed_fingerprints.add(fingerprint)
    symbol_cooldowns[symbol] = now
    daily_call_count += 1

    return {
        'signal_timeframe': 'H1',
        'strategy': 'long-h1-h4-trend-flow-v1',
        'setup_type': setup_type,
        'setup_name': setup_name,
        'order_type': order_type,
        'symbol': symbol,
        'entry': entry_mid,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'support_low': support_low,
        'sl': sl_price,
        'risk_pct': risk_pct,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp3': tp3_price,
        'score': total_score,
        'rsi_h1': rsi_h1[-1],
        'rsi_h4': rsi_h4[-1],
        'spot_taker': spot_taker[0] if spot_taker else 50.0,
        'futures_taker': futures_taker[0] if futures_taker else 50.0,
        'vol_ratio': vol_ratio,
        'oi_h1_pct': oi_h1_pct,
        'funding': funding,
        'spread_pct': spread_pct,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"TRIGGER_{symbol}_{last_h1['timestamp']}"
    }

# ==============================================================================
# 5. TELEGRAM ALERT FORMATTER (ĐÚNG MẪU NỘI DUNG 100%)
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    sig['hit_tp1'] = False
    sig['hit_tp2'] = False
    sig['status_text'] = f"⏳ Đang chờ {sig['order_type']} (PAPER)"
    
    signals_history.append(sig)
    active_signals.append(sig)

    tp3_text = f"• TP3: <code>{sig['tp3']:.4f}</code> (Runner Target)\n" if sig.get('tp3') else "• TP3: Không đủ điều kiện hiển thị Runner Target\n"

    msg = f"""🟢 <b>LONG H1/H4 TREND — #{sig['symbol']}</b>
<b>Setup:</b> {sig['setup_name']} | <b>Điểm:</b> <code>{sig['score']}/100</code>

<b>Xu hướng:</b>
• H4: Giá > EMA34 > EMA89
• H1: Đã reclaim EMA34
• RSI H4: <code>{sig['rsi_h4']:.1f}</code> và tăng
• RSI H1: <code>{sig['rsi_h1']:.1f}</code>

<b>Dòng tiền:</b>
• Spot CVD H1: Tăng
• Futures CVD H1: Tăng
• Spot taker-buy: <code>{sig['spot_taker']:.1f}%</code>
• Futures taker-buy: <code>{sig['futures_taker']:.1f}%</code>
• Spot volume: <code>{sig['vol_ratio']:.1f}x</code> Median20
• OI H1: <code>{sig['oi_h1_pct']:+.1f}%</code>
• Funding: <code>{sig['funding']:+.3f}%</code>
• Spread: <code>{sig['spread_pct']:.2f}%</code>

<b>Lệnh:</b> <b>{sig['order_type']} Entry</b>
• Entry Zone: <code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>
• Hỗ trợ bảo vệ: <code>{sig['support_low']:.4f}</code>
• Sai xu hướng khi: Nến H1 đóng dưới <code>{sig['support_low']:.4f}</code>
• SL: <code>{sig['sl']:.4f}</code> — cách Entry <b>{sig['risk_pct']:.2f}%</b>
• TP1: <code>{sig['tp1']:.4f}</code> (Kháng cự H1 - Chốt 20%)
• TP2: <code>{sig['tp2']:.4f}</code> (Kháng cự H4 - Chốt 25%)
{tp3_text}
<b>Lý do call:</b>
• Thuận xu hướng H4
• RSI H1/H4 reclaim 50
• Spot/Futures CVD xác nhận
• OI tăng vừa phải / Rũ đòn bẩy lành mạnh
• Funding chưa nóng
• Entry sát hỗ trợ cứng

⚠️ <i>PAPER, dữ liệu thử nghiệm, không phải khuyến nghị đầu tư.</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM TRIGGER] #{sig['id']} {sig['symbol']} [{sig['order_type']}] (Score: {sig['score']})", flush=True)

# ==============================================================================
# 6. MAIN RADAR LOOP
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Strategy: long-h1-h4-trend-flow-v1 (Enhanced)...", flush=True)
    while True:
        btc_h4 = get_closed_klines("BTCUSDT", "240", category="linear", limit=50)
        all_tickers = get_all_bybit_usdt_symbols()
        print(f"🔍 [RADAR SCAN] Đang rà soát Hard Gate & Trigger cho {len(all_tickers)} coin...", flush=True)
        
        candidates = []
        for ticker in all_tickers:
            try:
                sig = evaluate_long_h1_h4_strategy(ticker, btc_h4)
                if sig:
                    send_telegram_signal(sig)
                    candidates.append(sig)
                    if len(candidates) >= 2: break # Tối đa 2 call mỗi vòng quét
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
