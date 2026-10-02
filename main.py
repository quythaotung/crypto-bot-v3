import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - H1 SQUEEZE EXPLOSION ENGINE
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết 5 trạng thái minh bạch ra Render
COOLDOWN_SECONDS = 10              # Quét liên tục real-time
MIN_TURNOVER_24H = 10_000_000      # Lọc coin thanh khoản 24h >= $10M USD
MIN_ALERT_SCORE = 80               # Ngưỡng phát lệnh (80-84: CANDIDATE, 85-89: LONG H1 SQUEEZE, >=90: SQUEEZE EXPLOSION)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                # Danh sách theo dõi TP/SL real-time
processed_signal_ids = set()
symbol_cooldowns = {}              # Quản lý giãn cách gọi lệnh theo symbol
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit H1 Squeeze Explosion Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot H1 Squeeze Explosion đang chạy 24/7!\n⏳ Chưa có tín hiệu bùng nổ nào phát ra từ lần khởi động gần nhất."
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
        
        msg += f"<b>#{sig['id']} [SQUEEZE LONG] {sig['symbol']}</b> (Score: {sig['score']}/100)\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H1 Squeeze Explosion đang hoạt động 24/7!")
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

                    # CANCEL NẾU GIÁ CHẠY VỌT QUÁ ENTRY ZONE
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.015:
                            sig['status_text'] = "⚠️ CANCEL (Giá vượt quá Entry Zone)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE PUMP] #{sig['id']} - {sig['symbol']}</b>\nGiá (<code>{curr_price:.4f}</code>) đã rời quá xa Entry Zone. Tự động hủy lệnh!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 (Bảo toàn vốn)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 Target: <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.1f}%</b>)
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL. Dời SL về Breakeven để gồng TP2/TP3!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = "🚀 Hit TP2 Target"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CHẠM TP2 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 TP2 Target: <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.1f}%</b>)
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>BÙNG NỔ THÀNH CÔNG TẠI VÙNG RESISTANCE TIẾP THEO!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. HIT SL
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL - SQUEEZE INVALID] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Gãy mốc Hỗ trợ Nền Tích Lũy. Dừng lệnh an toàn!</i>"""
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
            excluded = ['3L', '3S', '2L', '2S', 'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT']
            parsed_list = []
            for t in tickers:
                sym = t['symbol']
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and not any(x in sym for x in excluded):
                    parsed_list.append({
                        'symbol': sym,
                        'turnover24h': float(t.get('turnover24h', 0)),
                        'price24hPcnt': float(t.get('price24hPcnt', 0)) * 100,
                        'fundingRate': float(t.get('fundingRate', 0)) * 100,
                        'lastPrice': float(t.get('lastPrice', 0))
                    })
            return parsed_list
    except Exception:
        pass
    return []

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

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 4. CORE ENGINE: H1 SQUEEZE EXPLOSION (BASE SEARCH & ORDER FLOW EXPANSION)
# ==============================================================================

def find_accumulation_base(h1_candles):
    """
    Tìm nền tích lũy (Base) trong 20-50 nến H1 gần nhất:
    - Độ dài: 4-20 nến
    - Xây dựng Base High (Breakout Level) và Base Low
    """
    n = len(h1_candles)
    if n < 30: return None

    for base_len in range(4, 21):
        for i in range(n - base_len - 1, max(0, n - 45), -1):
            base_candles = h1_candles[i:i+base_len]
            b_high = max([c['high'] for c in base_candles])
            b_low = min([c['low'] for c in base_candles])
            
            # Kiểm tra biên độ hẹp (Base Range <= 6%)
            range_pct = (b_high - b_low) / b_low * 100
            if range_pct <= 6.5:
                return {
                    'base_high': b_high,
                    'base_low': b_low,
                    'base_len': base_len,
                    'start_idx': i,
                    'end_idx': i + base_len
                }
    return None

def find_up_to_3_resistances(h1_candles, h4_candles, entry_price):
    targets = []
    for i in range(2, len(h1_candles) - 2):
        c = h1_candles[i]
        if (c['high'] > h1_candles[i-1]['high'] and c['high'] > h1_candles[i-2]['high'] and
            c['high'] > h1_candles[i+1]['high'] and c['high'] > h1_candles[i+2]['high']):
            if c['high'] > entry_price * 1.005:
                targets.append(c['high'])

    for i in range(2, len(h4_candles) - 2):
        c = h4_candles[i]
        if (c['high'] > h4_candles[i-1]['high'] and c['high'] > h4_candles[i+1]['high']):
            if c['high'] > entry_price * 1.005:
                targets.append(c['high'])

    unique_targets = sorted(list(set(targets)))
    valid_targets = []
    for t in unique_targets:
        if not valid_targets or (t - valid_targets[-1]) / valid_targets[-1] * 100 >= 0.8:
            valid_targets.append(t)

    return valid_targets[:3]

def evaluate_h1_squeeze_explosion(ticker_data, btc_h1_candles):
    symbol = ticker_data['symbol']
    turnover = ticker_data['turnover24h']

    if turnover < MIN_TURNOVER_24H: return None

    h1_candles = get_closed_klines(symbol, "60", limit=60)
    if len(h1_candles) < 40: return None

    h4_candles = get_closed_klines(symbol, "240", limit=60)
    if len(h4_candles) < 30: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_atr = calculate_atr(h1_candles, 14)
    if h1_atr <= 0: return None

    # --- 1. HARD FILTER: CHECK H4 BEARISH MẠNH ---
    ema34_h4_list = calculate_ema(h4_candles, 34)
    ema89_h4_list = calculate_ema(h4_candles, 89)
    if ema34_h4_list and ema89_h4_list:
        e34_curr = ema34_h4_list[-1]
        e89_curr = ema89_h4_list[-1]
        if h4_candles[-1]['close'] < e34_curr and e34_curr < e89_curr:
            if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: H4 Bearish (Price < EMA34 < EMA89)", flush=True)
            return None

    # --- 2. TÌM NỀN TÍCH LŨY (BASE) ---
    base_info = find_accumulation_base(h1_candles)
    if not base_info:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Không tìm thấy Base tích lũy hợp lệ", flush=True)
        return None

    b_high = base_info['base_high']
    b_low = base_info['base_low']
    b_len = base_info['base_len']
    breakout_level = b_high

    # --- 3. KIỂM TRA ĐIỀU KIỆN BREAKOUT H1 ---
    is_breakout = last_h1['close'] > breakout_level and (last_h1['close'] - breakout_level) >= (0.10 * h1_atr)
    is_retesting = (b_low <= curr_price <= breakout_level + 0.30 * h1_atr) and last_h1['low'] >= b_low * 0.995

    dist_from_breakout = curr_price - breakout_level

    # --- HARD FILTER: CHASE WARNING (> 1.0 ATR hoặc > 4% chưa retest) ---
    if dist_from_breakout > (1.0 * h1_atr):
        if DEBUG_MODE: print(f"[{symbol}] ❌ HARD REJECT: Giá cách Breakout Level > 1.0 ATR ({dist_from_breakout/h1_atr:.2f} ATR)", flush=True)
        return None

    if (curr_price - breakout_level) / breakout_level * 100 > 4.0 and last_h1['low'] > breakout_level + 0.30 * h1_atr:
        if DEBUG_MODE: print(f"[{symbol}] ⏳ WAITING FOR RETEST: Giá đã pump > 4% chưa retest nền", flush=True)
        return None

    # --- 4. EXPANSION ORDER FLOW METRICS ---
    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    rvol = last_h1['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0

    # HARD FILTER: VOLUME H1 < 1.3x SMA20
    if rvol < 1.3:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Volume H1 yếu ({rvol:.1f}x < 1.3x SMA20)", flush=True)
        return None

    taker_h1 = get_taker_buy_pct(symbol, "1h")
    # HARD FILTER: CVD < 55%
    if taker_h1 < 55.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: CVD/Taker Buy thấp ({taker_h1:.1f}% < 55%)", flush=True)
        return None

    oi_pct_h1 = get_open_interest_pct(symbol, "1h")

    # ==========================================================================
    # BẮT ĐẦU CHẤM ĐIỂM BASE SCORE (100 ĐIỂM)
    # ==========================================================================

    # A. Volume Score (20đ)
    if rvol >= 2.5: score_vol = 20
    elif rvol >= 2.0: score_vol = 16
    elif rvol >= 1.5: score_vol = 12
    elif rvol >= 1.2: score_vol = 7
    elif rvol >= 1.0: score_vol = 3
    else: score_vol = 0

    # B. Open Interest Score (20đ)
    if oi_pct_h1 >= 8.0: score_oi = 20
    elif oi_pct_h1 >= 5.0: score_oi = 16
    elif oi_pct_h1 >= 3.0: score_oi = 12
    elif oi_pct_h1 >= 1.0: score_oi = 7
    elif oi_pct_h1 >= 0.0: score_oi = 3
    else: score_oi = 0

    # C. CVD Score (20đ)
    if taker_h1 >= 80.0: score_cvd = 20
    elif taker_h1 >= 70.0: score_cvd = 16
    elif taker_h1 >= 60.0: score_cvd = 12
    elif taker_h1 >= 55.0: score_cvd = 7
    elif taker_h1 >= 50.0: score_cvd = 3
    else: score_cvd = 0

    # D. Price Structure Score (15đ)
    score_struct = 0
    if is_breakout: score_struct += 5
    if is_retesting: score_struct += 5
    if last_h1['close'] > breakout_level: score_struct += 5
    score_struct = min(score_struct, 15)

    # E. Base Quality Score (20đ)
    if b_len >= 16: score_base = 20
    elif b_len >= 11: score_base = 15
    elif b_len >= 7: score_base = 10
    elif b_len >= 4: score_base = 5
    else: score_base = 0

    # F. H4 Context Score (5đ)
    score_h4 = 0
    if ema34_h4_list:
        if h4_candles[-1]['close'] > ema34_h4_list[-1]: score_h4 += 2
        if ema89_h4_list and ema34_h4_list[-1] > ema89_h4_list[-1]: score_h4 += 2
        if len(ema34_h4_list) >= 2 and ema34_h4_list[-1] > ema34_h4_list[-2]: score_h4 += 1
    score_h4 = min(score_h4, 5)

    total_score = score_vol + score_oi + score_cvd + score_struct + score_base + score_h4

    if total_score < MIN_ALERT_SCORE:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Score chỉ đạt {total_score}/100 (<{MIN_ALERT_SCORE})", flush=True)
        return None

    # --- 5. CALCULATE ENTRY, SL, TP & RR FILTER ---
    entry_zone_low = breakout_level
    entry_zone_high = min(curr_price, breakout_level + (0.30 * h1_atr))
    if is_retesting:
        entry_zone_low = breakout_level - (0.20 * h1_atr)
        entry_zone_high = breakout_level + (0.20 * h1_atr)

    entry_mid = curr_price

    # Stop Loss = Base Low / Sweep Low - 0.15 ATR
    sweep_low = min([c['low'] for c in h1_candles[-5:]])
    sl_price = min(b_low, sweep_low) - (0.15 * h1_atr)
    risk_pct = (entry_mid - sl_price) / entry_mid * 100

    # HARD FILTER SL
    if risk_pct > 2.5:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: SL > 2.5% ({risk_pct:.2f}%)", flush=True)
        return None
    if 2.0 < risk_pct <= 2.5 and total_score < 90:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Risk 2.0-2.5% yêu cầu Score >= 90 (Hiện tại: {total_score})", flush=True)
        return None

    # DÒ TÌM PUMP ROOM VÀ BỘ 3 RESISTANCE
    targets = find_up_to_3_resistances(h1_candles, h4_candles, entry_mid)
    tp1_price, tp2_price = None, None
    pump_room_pct = 0.0

    if targets:
        r1_pct = (targets[0] - entry_mid) / entry_mid * 100
        if r1_pct >= 5.0:
            tp1_price = targets[0]
            tp2_price = targets[1] if len(targets) >= 2 else tp1_price * 1.06
            pump_room_pct = r1_pct
        elif len(targets) >= 2 and (targets[1] - entry_mid) / entry_mid * 100 >= 5.0:
            tp1_price = targets[1]
            tp2_price = targets[2] if len(targets) >= 3 else tp1_price * 1.08
            pump_room_pct = (targets[1] - entry_mid) / entry_mid * 100

    if not tp1_price:
        fallback_tp1 = entry_mid + (h1_atr * 2.8)
        fallback_pct = (fallback_tp1 - entry_mid) / entry_mid * 100
        if fallback_pct >= 5.0:
            tp1_price = fallback_tp1
            tp2_price = fallback_tp1 * 1.08
            pump_room_pct = fallback_pct

    # HARD FILTER: PUMP ROOM < 5.0%
    if pump_room_pct < 5.0 or not tp1_price:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Pump Room < 5.0% (+{pump_room_pct:.1f}%)", flush=True)
        return None

    # HARD FILTER: RISK / REWARD TP1 >= 2.5R
    tp1_dist = tp1_price - entry_mid
    sl_dist = entry_mid - sl_price
    rr_ratio = tp1_dist / sl_dist if sl_dist > 0 else 0

    if rr_ratio < 2.5:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Risk/Reward TP1 < 2.5R (Hiện tại: {rr_ratio:.1f}R)", flush=True)
        return None

    # --- SETUP LABEL PHÂN LOẠI ---
    if total_score >= 90:
        setup_label = "🚀 LONG H1 SQUEEZE EXPLOSION"
    elif total_score >= 85:
        setup_label = "🔥 LONG H1 SQUEEZE"
    else:
        setup_label = "LONG CANDIDATE"

    now = time.time()
    symbol_cooldowns[symbol] = now

    return {
        'signal_timeframe': 'H1',
        'engine_tag': 'H1 SQUEEZE EXPLOSION',
        'title': f"🚀 [{setup_label}] #{symbol}",
        'symbol': symbol,
        'entry': entry_mid,
        'entry_low': entry_zone_low,
        'entry_high': entry_zone_high,
        'base_high': b_high,
        'base_low': b_low,
        'sl': sl_price,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': (tp1_price - entry_mid) / entry_mid * 100,
        'tp2_pct': (tp2_price - entry_mid) / entry_mid * 100,
        'risk_pct': risk_pct,
        'score': total_score,
        'setup_label': setup_label,
        'rvol': rvol,
        'oi_pct': oi_pct_h1,
        'taker_pct': taker_h1,
        'pump_room_pct': pump_room_pct,
        'rr_ratio': rr_ratio,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"SQUEEZE_{symbol}_{last_h1['timestamp']}"
    }

# ==============================================================================
# 5. TELEGRAM ALERT FORMATTER (CHUẨN FORMAT 100% YÊU CẦU)
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

    msg = f"""🚀 <b>[{sig['setup_label']}] #{sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín</b>: <code>{sig['score']}/100</code>

🔥 <b>H1 BREAKOUT + ORDER FLOW EXPANSION</b>:
• Volume H1: <code>+{sig['rvol']:.1f}x SMA20</code>
• Open Interest: <code>{sig['oi_pct']:+.1f}%</code>
• Lực Mua CVD: <code>{sig['taker_pct']:.0f}%</code>
• Trạng Thái: <code>Breakout Nền Tích Lũy</code>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG)</b>:

📍 <b>Entry Zone</b>:
<code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss</b>:
<code>{sig['sl']:.4f}</code>

🎯 <b>Take Profit 1</b>:
<code>{sig['tp1']:.4f}</code> <code>(+{sig['tp1_pct']:.1f}%)</code>

🚀 <b>Take Profit 2</b>:
<code>{sig['tp2']:.4f}</code> <code>(+{sig['tp2_pct']:.1f}%)</code>

-------------------------------------
📊 <b>Pump Room</b>: <code>{sig['pump_room_pct']:.1f}%</code>
⚖️ <b>Risk/Reward TP1</b>: <code>{sig['rr_ratio']:.1f}R</code>

👉 <b>CHECK CHART H1 & BẮT SÓNG SỚM</b>
⚠️ <b>KHÔNG CHASE nếu giá rời Entry Zone</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM SQUEEZE EXPLOSION] #{sig['id']} {sig['symbol']} (Score: {sig['score']})", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét H1 Squeeze Explosion (QNT Style)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_tickers = get_all_bybit_usdt_symbols()
        print(f"🔍 [SQUEEZE SCAN] Đang rà soát nền tích lũy & bùng nổ cho {len(all_tickers)} coin...", flush=True)
        
        candidates = []
        for ticker in all_tickers:
            try:
                sig = evaluate_h1_squeeze_explosion(ticker, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
                    candidates.append(sig)
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
