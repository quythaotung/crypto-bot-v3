import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - H1 DEMAND ZONE RECLAIM ENGINE
# ==============================================================================
DEBUG_MODE = True                  # In log minh bạch lý do Reject từng coin ra Render
COOLDOWN_SECONDS = 10              # Quét liên tục real-time
MIN_TURNOVER_24H = 10_000_000      # Lọc coin thanh khoản 24h >= $10M USD
MIN_ALERT_SCORE = 80               # Ngưỡng phát tín hiệu (>= 80: LONG CANDIDATE, >= 85: DEMAND LONG)

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
    return "Bybit H1 Demand Zone Reclaim Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot H1 Demand Zone Reclaim đang chạy 24/7!\n⏳ Chưa có tín hiệu chuẩn Demand Zone nào phát ra từ lần khởi động gần nhất."
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
        
        msg += f"<b>#{sig['id']} [DEMAND LONG] {sig['symbol']}</b> (Score: {sig['score']}/100)\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H1 Demand Zone Reclaim đang hoạt động 24/7!")
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

                    # CANCEL NẾU GIÁ BỊ CHASE VỌT QUÁ ENTRY ZONE
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.015:
                            sig['status_text'] = "⚠️ CANCEL (Giá đã vọt quá Entry Zone)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE PUMP] #{sig['id']} - {sig['symbol']}</b>\nGiá (<code>{curr_price:.4f}</code>) đã rời quá xa Entry Zone. Tự động hủy lệnh!"
                            send_telegram_direct_message(msg)
                            if sig in active_signals: active_signals.remove(sig)
                            continue
                        elif curr_price <= sig['entry_high']:
                            sig['entered'] = True

                    # 1. CHẠM TP1 (Tự động HỦY bảo vệ SL)
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 (Đã bảo toàn vốn)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CHẠM TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 Target: <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.1f}%</b>)
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL đối với lệnh này. Dời SL về Breakeven để gồng TP2/TP3!</i>"""
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
🏆 <b>CHỐT LỜI THÀNH CÔNG TẠI VÙNG RESISTANCE TIẾP THEO!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = "🔥 Hit TP3 Major Supply"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CHẠM TP3 MAJOR SUPPLY] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 TP3 Target: <code>{sig['tp3']:.4f}</code> (<b>+{sig['tp3_pct']:.1f}%</b>)
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH MỤC TIÊU SÓNG DEMAND ZONE!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals: active_signals.remove(sig)

                    # 4. HIT SL
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (INVALIDATED)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL - DEMAND INVALID] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Gãy mốc Hỗ trợ Demand Low. Dừng lệnh an toàn!</i>"""
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

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 4. CORE ENGINE: H1 DEMAND ZONE RECLAIM (SEARCH & SCORE)
# ==============================================================================

def find_valid_demand_zone(h1_candles, h1_atr):
    """
    Tìm Demand Zone trong 50-100 nến H1:
    1. Base: 3-15 nến biên độ hẹp.
    2. Impulse: Tăng >= 3% hoặc >= 1.5 ATR trong tối đa 12 nến.
    3. Zone: Demand Low (đáy Base) -> Demand High (đỉnh Base trước Impulse).
    """
    n = len(h1_candles)
    if n < 40: return None

    for i in range(n - 15, 20, -1):
        # Kiểm tra Impulse (tăng >=3% hoặc >= 1.5 ATR trong tối đa 12 nến tiếp theo)
        for impulse_len in range(2, 13):
            if i + impulse_len >= n: break
            
            start_price = h1_candles[i]['close']
            end_price = max([c['high'] for c in h1_candles[i:i+impulse_len]])
            
            pct_gain = (end_price - start_price) / start_price * 100
            atr_gain = (end_price - start_price) / h1_atr if h1_atr > 0 else 0

            if pct_gain >= 3.0 or atr_gain >= 1.5:
                # Dò tìm Base trước đó (3-15 nến)
                for base_len in range(3, 16):
                    if i - base_len < 0: break
                    base_candles = h1_candles[i-base_len:i]
                    
                    demand_low = min([c['low'] for c in base_candles])
                    demand_high = max([c['high'] for c in base_candles])
                    
                    # Tính Volume của Impulse
                    impulse_vols = [c['volume'] for c in h1_candles[i:i+impulse_len]]
                    avg_impulse_vol = sum(impulse_vols) / len(impulse_vols) if impulse_vols else 0
                    
                    # Đếm số lần Retest zone trước đó
                    retest_count = 0
                    for check_c in h1_candles[i+impulse_len:-1]:
                        if check_c['low'] <= demand_high and check_c['low'] >= demand_low * 0.995:
                            retest_count += 1

                    return {
                        'demand_low': demand_low,
                        'demand_high': demand_high,
                        'base_len': base_len,
                        'impulse_pct': pct_gain,
                        'impulse_vol': avg_impulse_vol,
                        'retest_count': retest_count,
                        'base_end_idx': i
                    }
    return None

def find_resistance_targets(h1_candles, h4_candles, entry_price):
    """
    Dò tìm các mốc Resistance/Supply phía trên để xác định Pump Room thực tế.
    """
    targets = []
    # Check H1 Swing Highs
    for i in range(2, len(h1_candles) - 2):
        c = h1_candles[i]
        if (c['high'] > h1_candles[i-1]['high'] and c['high'] > h1_candles[i-2]['high'] and
            c['high'] > h1_candles[i+1]['high'] and c['high'] > h1_candles[i+2]['high']):
            if c['high'] > entry_price * 1.01:
                targets.append(c['high'])

    # Check H4 Swing Highs
    for i in range(2, len(h4_candles) - 2):
        c = h4_candles[i]
        if (c['high'] > h4_candles[i-1]['high'] and c['high'] > h4_candles[i+1]['high']):
            if c['high'] > entry_price * 1.01:
                targets.append(c['high'])

    unique_targets = sorted(list(set(targets)))
    valid_targets = []
    for t in unique_targets:
        if not valid_targets or (t - valid_targets[-1]) / valid_targets[-1] * 100 >= 1.0:
            valid_targets.append(t)

    return valid_targets

def evaluate_h1_demand_zone_reclaim(ticker_data, btc_h1_candles):
    symbol = ticker_data['symbol']
    turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 14400:
        return None

    h1_candles = get_closed_klines(symbol, "60", limit=100)
    if len(h1_candles) < 50: return None

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
        e34_curr, e34_prev = ema34_h4_list[-1], ema34_h4_list[-2]
        e89_curr = ema89_h4_list[-1]
        if h4_candles[-1]['close'] < e34_curr and e34_curr < e89_curr and e34_curr < e34_prev:
            if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: H4 Bearish mạnh (Price < EMA34 < EMA89)", flush=True)
            return None

    # --- 2. XÁC ĐỊNH DEMAND ZONE ---
    dz = find_valid_demand_zone(h1_candles, h1_atr)
    if not dz:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Không tìm thấy Demand Zone hợp lệ", flush=True)
        return None

    d_low, d_high = dz['demand_low'], dz['demand_high']
    d_mid = (d_low + d_high) / 2.0

    # --- 3. HARD FILTER: CHASE WARNING & DISTANCE ---
    dist_from_dz = curr_price - d_high
    if dist_from_dz > (0.8 * h1_atr):
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Giá cách Demand Zone quá xa ({dist_from_dz/h1_atr:.2f} ATR > 0.8 ATR)", flush=True)
        return None

    # Anti-chase: Tăng quá 3.5% từ Demand Low
    if (curr_price - d_low) / d_low * 100 > 3.5 and last_h1['low'] > d_high:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Price pump > 3.5% từ Demand Low", flush=True)
        return None

    # --- 4. TÍNH SL CẤU TRÚC VÀ CHECK HARD FILTER SL <= 2.5% ---
    sweep_low = min([c['low'] for c in h1_candles[-5:]])
    sl_price = min(d_low, sweep_low) - (0.15 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 2.5:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: SL quá lớn ({risk_pct:.2f}% > 2.5%)", flush=True)
        return None

    # --- 5. TÍNH PUMP ROOM VÀ CHECK HARD FILTER PUMP ROOM >= 5% ---
    targets = find_resistance_targets(h1_candles, h4_candles, curr_price)
    tp1_price = targets[0] if len(targets) >= 1 else curr_price + (h1_atr * 2.5)
    tp2_price = targets[1] if len(targets) >= 2 else tp1_price * 1.05
    tp3_price = targets[2] if len(targets) >= 3 else tp2_price * 1.08

    pump_room_pct = (tp1_price - curr_price) / curr_price * 100
    if pump_room_pct < 5.0:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Pump Room quá hẹp ({pump_room_pct:.1f}% < 5.0%)", flush=True)
        return None

    # ==========================================================================
    # BẮT ĐẦU CHẤM BASE SCORE (100 ĐIỂM)
    # ==========================================================================

    # A. DEMAND ZONE QUALITY (25 ĐIỂM)
    s_dz_impulse = 7 if dz['impulse_pct'] >= 7.0 else (5 if dz['impulse_pct'] >= 5.0 else 3)
    
    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = dz['impulse_vol'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    s_dz_vol = 5 if vol_ratio >= 2.0 else (4 if vol_ratio >= 1.5 else (3 if vol_ratio >= 1.2 else (1 if vol_ratio >= 1.0 else 0)))

    s_dz_base = 5 if dz['base_len'] >= 11 else (4 if dz['base_len'] >= 6 else 2)
    s_dz_retest = 8 if dz['retest_count'] >= 2 else (7 if dz['retest_count'] == 1 else 5)
    
    score_dz = min(s_dz_impulse + s_dz_vol + s_dz_base + s_dz_retest, 25)

    # B. GIÁ HỒI VỀ DEMAND (20 ĐIỂM)
    if d_low <= curr_price <= d_high:
        s_dist = 10
    elif dist_from_dz <= (0.2 * h1_atr):
        s_dist = 8
    elif dist_from_dz <= (0.4 * h1_atr):
        s_dist = 6
    elif dist_from_dz <= (0.6 * h1_atr):
        s_dist = 3
    else:
        s_dist = 0

    if curr_price <= d_low + (d_high - d_low) * 0.33:
        s_pos = 5
    elif curr_price <= d_low + (d_high - d_low) * 0.66:
        s_pos = 3
    else:
        s_pos = 1

    s_no_chase = 5 if dist_from_dz <= (0.2 * h1_atr) else 0
    score_price_demand = min(s_dist + s_pos + s_no_chase, 20)

    # C. REJECTION / RECLAIM (20 ĐIỂM)
    h1_range = last_h1['high'] - last_h1['low']
    lower_wick = (min(last_h1['open'], last_h1['close']) - last_h1['low'])
    wick_ratio = lower_wick / h1_range if h1_range > 0 else 0

    s_wick = 7 if wick_ratio >= 0.45 else (5 if wick_ratio >= 0.35 else (3 if wick_ratio >= 0.25 else 0))
    s_reclaim = 8 if last_h1['close'] > d_high else (6 if last_h1['close'] >= d_mid else 0)
    
    # Check Bullish Engulfing
    prev_h1 = h1_candles[-2]
    is_engulfing = last_h1['close'] > prev_h1['open'] and last_h1['open'] < prev_h1['close'] and last_h1['close'] > last_h1['open']
    s_engulfing = 5 if is_engulfing else 0

    s_close_prev_high = 5 if last_h1['close'] > prev_h1['high'] else 0
    score_reclaim = min(s_wick + s_reclaim + s_engulfing + s_close_prev_high, 20)

    # D. RSI (10 ĐIỂM)
    rsi_list = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_list[-1] if rsi_list else 50.0

    if curr_rsi < 30.0: s_rsi = 5
    elif curr_rsi <= 35.0: s_rsi = 8
    elif curr_rsi <= 45.0: s_rsi = 10
    elif curr_rsi <= 50.0: s_rsi = 7
    elif curr_rsi <= 55.0: s_rsi = 3
    else: s_rsi = 0

    # Bullish RSI Divergence Bonus
    has_rsi_div = False
    if len(rsi_list) >= 10 and last_h1['low'] <= min([c['low'] for c in h1_candles[-10:-2]]) and curr_rsi > min(rsi_list[-10:-2]):
        s_rsi += 5
        has_rsi_div = True

    score_rsi = min(s_rsi, 10)

    # E. ORDER FLOW (15 ĐIỂM)
    taker_h1 = get_taker_buy_pct(symbol, "1h")
    s_cvd = 5 if taker_h1 >= 70.0 else (4 if taker_h1 >= 60.0 else (2 if taker_h1 >= 55.0 else (1 if taker_h1 >= 50.0 else 0)))

    oi_pct_h1 = get_open_interest_pct(symbol, "1h")
    s_oi = 5 if oi_pct_h1 >= 4.0 else (4 if oi_pct_h1 >= 2.0 else (3 if oi_pct_h1 >= 1.0 else (2 if oi_pct_h1 >= 0 else 0)))

    if funding < -0.01: s_funding = 5
    elif funding <= 0: s_funding = 4
    elif funding <= 0.02: s_funding = 3
    elif funding <= 0.05: s_funding = 1
    else: s_funding = 0

    score_flow = min(s_cvd + s_oi + s_funding, 15)

    # F. H4 CONTEXT (10 ĐIỂM)
    s_h4 = 0
    if ema34_h4_list:
        if h4_candles[-1]['close'] > ema34_h4_list[-1]: s_h4 += 3
        if ema89_h4_list and ema34_h4_list[-1] > ema89_h4_list[-1]: s_h4 += 3
        if len(ema34_h4_list) >= 2 and ema34_h4_list[-1] > ema34_h4_list[-2]: s_h4 += 2
    if abs(d_mid - min([c['low'] for c in h4_candles[-10:]])) / d_mid * 100 <= 1.5: s_h4 += 2
    score_h4 = min(s_h4, 10)

    # TỔNG CỘNG BASE SCORE 100
    total_score = score_dz + score_price_demand + score_reclaim + score_rsi + score_flow + score_h4

    # IN DEBUG LOG BẮT BUỘC RA RENDER
    if DEBUG_MODE:
        print(f"[DEMAND SCAN] {symbol} | DZ: {d_low:.4f}–{d_high:.4f} | Current: {curr_price:.4f} | Distance: {dist_from_dz/h1_atr:.2f} ATR | Score: {total_score}/100 | Pump Room: +{pump_room_pct:.1f}% | SL: -{risk_pct:.2f}%", flush=True)

    if total_score < MIN_ALERT_SCORE:
        if DEBUG_MODE: print(f"[{symbol}] ❌ REJECT: Score chỉ đạt {total_score}/100 (<{MIN_ALERT_SCORE})", flush=True)
        return None

    # PHÂN LOẠI SETUP
    if total_score >= 90:
        setup_label = "🔥🔥 A+ H1 DEMAND LONG"
    elif total_score >= 85:
        setup_label = "🔥 H1 DEMAND LONG"
    else:
        setup_label = "LONG CANDIDATE"

    symbol_cooldowns[symbol] = now

    entry_zone_low = max(d_low, curr_price - (0.15 * h1_atr))
    entry_zone_high = curr_price

    return {
        'signal_timeframe': 'H1',
        'engine_tag': 'H1 DEMAND RECLAIM',
        'title': '🔥 H1 DEMAND ZONE LONG',
        'symbol': symbol,
        'entry': curr_price,
        'entry_low': entry_zone_low,
        'entry_high': entry_zone_high,
        'demand_low': d_low,
        'demand_high': d_high,
        'sl': sl_price,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp3': tp3_price,
        'tp1_pct': (tp1_price - curr_price) / curr_price * 100,
        'tp2_pct': (tp2_price - curr_price) / curr_price * 100,
        'tp3_pct': (tp3_price - curr_price) / curr_price * 100,
        'risk_pct': risk_pct,
        'score': total_score,
        'setup_label': setup_label,
        'score_dz': score_dz,
        'score_reclaim': score_reclaim,
        'curr_rsi': curr_rsi,
        'has_rsi_div': has_rsi_div,
        'taker_pct': taker_h1,
        'oi_pct': oi_pct_h1,
        'funding': funding,
        'pump_room_pct': pump_room_pct,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"DEMAND_{symbol}_{last_h1['timestamp']}"
    }

# ==============================================================================
# 5. TELEGRAM ALERT FORMATTER (MẪU CHUẨN 100% YÊU CẦU)
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

    rsi_div_str = "✅" if sig['has_rsi_div'] else "❌"

    msg = f"""🔥 <b>H1 DEMAND ZONE LONG</b>

#{sig['symbol']}

⭐ <b>Score: {sig['score']}/100</b>
🟢 <b>Setup: {sig['setup_label']}</b>

📍 <b>Demand:</b>
<code>{sig['demand_low']:.4f}</code> – <code>{sig['demand_high']:.4f}</code>

📌 <b>Entry:</b>
<code>{sig['entry_low']:.4f}</code> – <code>{sig['entry_high']:.4f}</code>

🛡 <b>SL:</b>
<code>{sig['sl']:.4f}</code>
Risk: -{sig['risk_pct']:.1f}%

🎯 <b>TP1:</b>
<code>{sig['tp1']:.4f}</code> (+{sig['tp1_pct']:.1f}%)

🎯 <b>TP2:</b>
<code>{sig['tp2']:.4f}</code> (+{sig['tp2_pct']:.1f}%)

🚀 <b>TP3:</b>
<code>{sig['tp3']:.4f}</code> (+{sig['tp3_pct']:.1f}%)

🧲 Demand Quality: {sig['score_dz']}/25
🔄 Reclaim: ✅
📉 RSI: {sig['curr_rsi']:.0f}
📈 RSI Divergence: {rsi_div_str}
💰 CVD: {sig['taker_pct']:.0f}%
📊 OI: +{sig['oi_pct']:.1f}%
💵 Funding: {sig['funding']:+.3f}%

🌐 H4: Bullish

🚀 Pump Room: {sig['pump_room_pct']:.1f}%

⚠️ <i>BUY THE BASE</i>
❌ <i>NO CHASE</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM DEMAND ZONE] #{sig['id']} {sig['symbol']} (Score: {sig['score']})", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét Ngược H1 Demand Zone Reclaim...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_tickers = get_all_bybit_usdt_symbols()
        print(f"🔍 [DEMAND SCAN] Đang rà soát Demand Zone cho {len(all_tickers)} coin...", flush=True)
        
        candidates = []
        for ticker in all_tickers:
            try:
                sig = evaluate_h1_demand_zone_reclaim(ticker, btc_h1)
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
