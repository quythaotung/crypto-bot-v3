import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết ra Render
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Turnover 24h tối thiểu $5M USD
MAX_ALERTS_PER_CYCLE = 4           # Tối đa gửi Top 4 coin điểm cao nhất mỗi vòng quét

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                # Danh sách theo dõi TP/SL real-time
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl) - FIXED
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Structural Tracker Bot (Fixed Telegram Listener) Active 24/7!"

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
    """Hàm hỗ trợ gửi tin nhắn trực tiếp không bị ngắt"""
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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot vẫn đang chạy 24/7 bình thường!\n⏳ Chưa có tín hiệu nào đạt chuẩn điểm kỹ thuật được phát ra từ lần khởi động gần nhất."
        send_telegram_direct_message(msg)
        return

    msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH REAL-TIME:</b>\n-------------------------------------\n"
    total_pnl_pct = 0.0
    
    for sig in signals_history:
        curr_price = get_current_price(sig['symbol'])
        if not curr_price:
            curr_price = sig['entry_mid']
            
        pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
        status = sig.get('status_text', '⏳ Đang theo dõi')
        
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        
        msg += f"<b>#{sig['id']} [{sig['status_label']}] {sig['symbol']}</b> | Entry: {sig['entry_mid']:.4f}\n"
        msg += f"• Giá hiện tại: {curr_price:.4f} ({icon} {pnl_pct:+.2f}%)\n"
        msg += f"• Trạng thái: <b>{status}</b>\n-------------------------------------\n"
        total_pnl_pct += pnl_pct
        
    total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
    msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"
    send_telegram_direct_message(msg)

def process_telegram_commands():
    """Luồng lắng nghe lệnh Telegram không bao giờ bị Crash"""
    last_update_id = 0
    print("🤖 Đã bật luồng lắng nghe lệnh Telegram (/pnl, /start, /ping)...", flush=True)
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
                        print("📩 Nhận lệnh /pnl từ Telegram -> Đang gửi báo cáo...", flush=True)
                        send_pnl_summary()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Bybit đang hoạt động 24/7 trơn tru!")
        except Exception as e:
            print(f"Lỗi luồng Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# 2. TIẾN TRÌNH THEO DÕI GIÁ REAL-TIME BÁO CHẠM TP / SL
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

                    # 1. KIỂM TRA CHẠM TP1 ZONE
                    if not sig['hit_tp1'] and curr_price >= sig['tp1_low']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 Zone (Đã hủy bảo vệ SL)"
                        pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
                        
                        msg = f"""🎯 <b>[CẢNH BÁO CHẠM TP1 ZONE] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
📍 Entry Mid: <b>{sig['entry_mid']:.4f}</b>
🎯 Giá Hiện Tại: <b>{curr_price:.4f} (+{pnl_pct:.2f}%)</b>
🎯 Vùng TP1 Zone: <b>{sig['tp1_low']:.4f} – {sig['tp1_high']:.4f}</b>
-------------------------------------
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <b>Hệ thống tự động HỦY thông báo SL đối với lệnh này. Bạn có thể dời SL về Breakeven (Hoà vốn) để gồng TP2/TP3!</b>"""
                        send_telegram_direct_message(msg)

                    # 2. KIỂM TRA CHẠM TP2 ZONE
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2_low']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = "🚀 Hit TP2 Zone (Chốt lời thành công)"
                        pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
                        
                        msg = f"""🚀 <b>[CẢNH BÁO CHẠM TP2 ZONE] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
📍 Entry Mid: <b>{sig['entry_mid']:.4f}</b>
🚀 Giá Hiện Tại: <b>{curr_price:.4f} (+{pnl_pct:.2f}%)</b>
🚀 Vùng TP2 Zone: <b>{sig['tp2_low']:.4f} – {sig['tp2_high']:.4f}</b>
-------------------------------------
🔥 <b>ĐÃ CHẠM VÙNG SWING HIGH CŨ! KHUYẾN NGHỊ CHỐN LỜI HOẶC CHỈ GIỮ LẠI TỶ TRỌNG NHỎ!</b>"""
                        send_telegram_direct_message(msg)
                        if not sig['tp3_zone'] and sig in active_signals:
                            active_signals.remove(sig)

                    # 3. KIỂM TRA CHẠM TP3 ZONE (NẾU CÓ)
                    elif sig['hit_tp2'] and sig['tp3_zone'] and curr_price >= sig['tp3_zone'][0]:
                        sig['status_text'] = "🔥 Hit TP3 Zone (Full Target Expansion)"
                        pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
                        
                        msg = f"""🔥 <b>[CẢNH BÁO CHẠM TP3 ZONE - MEGAPUMP] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
📍 Entry Mid: <b>{sig['entry_mid']:.4f}</b>
🔥 Giá Hiện Tại: <b>{curr_price:.4f} (+{pnl_pct:.2f}%)</b>
🔥 Vùng TP3 Zone: <b>{sig['tp3_zone'][0]:.4f} – {sig['tp3_zone'][1]:.4f}</b>
-------------------------------------
🏆 <b>HOÀN THÀNH TOÀN BỘ MỤC TIÊU SÓNG TĂNG KỲ VỌNG!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)

                    # 4. KIỂM TRA HIT SL (CHỈ BÁO KHI CHƯA CẢN TP1)
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (Structural Invalidation)"
                        pnl_pct = ((curr_price - sig['entry_mid']) / sig['entry_mid']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL STRUCTURAL] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
📍 Entry Mid: <b>{sig['entry_mid']:.4f}</b>
🛑 Giá Hiện Tại: <b>{curr_price:.4f} ({pnl_pct:.2f}%)</b>
🛑 Mốc SL Cấu Trúc: <b>{sig['sl']:.4f}</b>
-------------------------------------
⚠️ <b>CẤU TRÚC TÍCH LŨY BỊ VI PHẠM (INVALIDATION). DỪNG LỆNH ĐỂ BẢO VỆ VỐN!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)
                except Exception as inner_e:
                    print(f"Lỗi theo dõi coin {sig.get('symbol')}: {inner_e}", flush=True)
        except Exception as e:
            print(f"Lỗi theo dõi Tracker Loop: {e}", flush=True)
        
        time.sleep(10)

# ==============================================================================
# 3. BYBIT REST API & INDICATORS HELPERS
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

def calculate_h1_atr(candles, period=14):
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low = candles[i]['low']
        prev_close = candles[i-1]['close']
        tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0

def get_open_interest_series(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=5"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 3:
                return [float(x["openInterest"]) for x in reversed(oi_list)]
    except Exception:
        pass
    return []

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def get_taker_buy_series(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit=4"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) >= 3:
                return [float(x.get("buyRatio", 0.5)) * 100 for x in reversed(data)]
    except Exception:
        pass
    return []

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 4. THIẾT LẬP CẤU TRÚC: SUPPORT ZONE, SL KỸ THUẬT VÀ TP ZONES THỰC TẾ
# ==============================================================================

def find_support_zone(h1_candles):
    if len(h1_candles) < 25: return None, None, 0
    recent_lows = [c['low'] for c in h1_candles[-20:]]
    recent_lows.sort()
    
    supp_low = recent_lows[0]
    supp_high = recent_lows[3]
    touch_count = sum(1 for c in h1_candles[-25:] if supp_low * 0.997 <= c['low'] <= supp_high * 1.008)
    return supp_low, supp_high, touch_count

def find_structural_sl_and_tp(h1_candles, supp_low, curr_price):
    h1_atr = calculate_h1_atr(h1_candles, 14)
    buffer = h1_atr * 0.2 if h1_atr > 0 else supp_low * 0.003

    recent_lows = [c['low'] for c in h1_candles[-15:]]
    sweep_low = min(recent_lows)
    
    if sweep_low < supp_low:
        structural_low = sweep_low
        sl_reason = "Dưới Liquidity Sweep Low H1"
    else:
        structural_low = supp_low
        sl_reason = "Dưới đáy Support Zone H1"

    sl_price = structural_low - buffer

    recent_highs = [c['high'] for c in h1_candles[-20:] if c['high'] > curr_price]
    if recent_highs:
        tp1_base = min(recent_highs)
    else:
        tp1_base = max([c['high'] for c in h1_candles[-15:]])

    tp1_low = tp1_base * 0.996
    tp1_high = tp1_base * 1.002
    tp1_mid = (tp1_low + tp1_high) / 2

    all_highs = [c['high'] for c in h1_candles[-45:] if c['high'] > tp1_high]
    if all_highs:
        tp2_base = max(all_highs)
    else:
        tp2_base = tp1_high * 1.035

    tp2_low = tp2_base * 0.995
    tp2_high = tp2_base * 1.003
    tp2_mid = (tp2_low + tp2_high) / 2

    range_height = tp2_base - supp_low
    if range_height > 0 and (tp2_base - curr_price) >= (curr_price - sl_price) * 1.5:
        tp3_base = tp2_base + (range_height * 0.618)
        tp3_low = tp3_base * 0.993
        tp3_high = tp3_base * 1.005
        tp3_zone = (tp3_low, tp3_high)
    else:
        tp3_zone = None

    return sl_price, sl_reason, (tp1_low, tp1_high, tp1_mid), (tp2_low, tp2_high, tp2_mid), tp3_zone

def evaluate_accumulation_reclaim(symbol, h1_candles, h4_candles, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: return None
    if len(h1_candles) < 35 or len(h4_candles) < 35: return None

    supp_low, supp_high, touch_count = find_support_zone(h1_candles)
    if not supp_low or touch_count < 1: return None

    last = h1_candles[-1]
    curr_price = last['close']

    dist_to_supp = (curr_price - supp_high) / supp_high if curr_price >= supp_high else 0.0
    if dist_to_supp > 0.020 or curr_price < supp_low * 0.993:
        return None

    score_support = 20 if touch_count >= 2 else 12

    range_last = last['high'] - last['low']
    range_avg = sum(c['high'] - c['low'] for c in h1_candles[-10:-1]) / 9
    is_compressed = range_last <= range_avg * 1.1
    higher_low = last['low'] >= min(c['low'] for c in h1_candles[-4:-1])
    score_accum = (8 if is_compressed else 0) + (7 if higher_low else 0)

    h1_range = last['high'] - last['low']
    wick_ratio = (min(last['open'], last['close']) - last['low']) / h1_range if h1_range > 0 else 0
    is_sweep = last['low'] < supp_low and last['close'] > supp_low
    score_pa = 15 if is_sweep else (12 if wick_ratio >= 0.35 else (10 if curr_price >= supp_high else 6))

    oi_series = get_open_interest_series(symbol, "1h")
    if len(oi_series) >= 3:
        oi_prev2, oi_prev1, oi_curr = oi_series[-3], oi_series[-2], oi_series[-1]
        score_oi = 15 if (oi_curr > oi_prev1 and oi_prev1 >= oi_prev2 * 0.99) else (10 if oi_curr >= oi_prev1 else 5)
    else: score_oi = 8

    taker_series = get_taker_buy_series(symbol)
    if len(taker_series) >= 2:
        score_cvd = 10 if (taker_series[-1] > taker_series[-2] and taker_series[-1] >= 50.0) else (7 if taker_series[-1] >= 50.0 else 4)
    else: score_cvd = 6

    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    vol_ratio = last['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    score_vol = 10 if 1.2 <= vol_ratio <= 2.2 else (7 if vol_ratio < 1.0 else 5)

    rsi_vals = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_vals[-1] if rsi_vals else 50.0
    score_rsi = 5 if 35.0 <= curr_rsi <= 55.0 else (3 if curr_rsi < 35 else 2)

    sym_chg_6h = (h1_candles[-1]['close'] - h1_candles[-6]['close']) / h1_candles[-6]['close']
    btc_chg_6h = (btc_h1_candles[-1]['close'] - btc_h1_candles[-6]['close']) / btc_h1_candles[-6]['close'] if btc_h1_candles else 0.0
    score_rs = 5 if sym_chg_6h >= btc_chg_6h else 2
    score_btc = 5 if btc_chg_6h >= -0.008 else 2

    total_score = score_support + score_accum + score_pa + score_oi + score_cvd + score_vol + score_rsi + score_rs + score_btc
    total_score = min(total_score, 100)

    if total_score < 70:
        return None

    entry_low = supp_low
    entry_high = min(curr_price, supp_high * 1.008)
    entry_mid = (entry_low + entry_high) / 2

    sl, sl_reason, tp1_data, tp2_data, tp3_zone = find_structural_sl_and_tp(h1_candles, supp_low, curr_price)
    
    risk = entry_mid - sl
    if risk <= 0: return None

    rr_tp1 = (tp1_data[2] - entry_mid) / risk
    rr_tp2 = (tp2_data[2] - entry_mid) / risk

    status_label = "H1 ACCUMULATION BASE / SUPPORT RECLAIM"

    return {
        'status_label': status_label,
        'symbol': symbol,
        'total_score': total_score,
        'vol_ratio': vol_ratio,
        'oi_pct': ((oi_series[-1] - oi_series[-2])/oi_series[-2]*100) if len(oi_series)>=2 else 0.0,
        'taker_pct': taker_series[-1] if taker_series else 50.0,
        'supp_low': supp_low,
        'supp_high': supp_high,
        'entry_low': entry_low,
        'entry_high': entry_high,
        'entry_mid': entry_mid,
        'sl': sl,
        'sl_reason': sl_reason,
        'tp1_low': tp1_data[0], 'tp1_high': tp1_data[1], 'tp1_mid': tp1_data[2],
        'tp2_low': tp2_data[0], 'tp2_high': tp2_data[1], 'tp2_mid': tp2_data[2],
        'tp3_zone': tp3_zone,
        'rr_tp1': rr_tp1,
        'rr_tp2': rr_tp2,
        'risk_pct': (risk / entry_mid) * 100,
        'hit_tp1': False,
        'hit_tp2': False,
        'status_text': '⏳ Đang theo dõi real-time',
        'signal_id': f"ACCUM_{symbol}_{last['timestamp']}"
    }

# ==============================================================================
# 5. FORMATTER TELEGRAM & PHÁT TÍN HIỆU CƠ BẢN
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)
    active_signals.append(sig)

    tp3_str = ""
    if sig['tp3_zone']:
        tp3_str = f"\n🔥 <b>TP3 Zone:</b>\n{sig['tp3_zone'][0]:.4f} – {sig['tp3_zone'][1]:.4f}\n(Major Resistance / Expansion)\n"

    msg = f"""🧱 <b>[{sig['status_label']}] #{sig['id']} - {sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {sig['total_score']}/100</b>
🧱 Support Zone: <b>{sig['supp_low']:.4f} – {sig['supp_high']:.4f}</b>

🔥 <b>TÍN HIỆU TÍCH LŨY / CỨU NỀN HỖ TRỢ:</b>
• Volume H1: <b>+{sig['vol_ratio']:.2f}x (SMA20)</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}% (Tăng/Ngừng giảm)</b>
• Lực Mua CVD: <b>{sig['taker_pct']:.1f}% (CVD hồi phục)</b>
-------------------------------------
🎯 <b>KẾ HOẠCH KỸ THUẬT:</b>

📍 <b>Entry Zone:</b>
{sig['entry_low']:.4f} – {sig['entry_high']:.4f}

🛑 <b>Stop Loss:</b>
{sig['sl']:.4f}
({sig['sl_reason']})

🎯 <b>TP1 Zone:</b>
{sig['tp1_low']:.4f} – {sig['tp1_high']:.4f}
(H1 Immediate Resistance)

🚀 <b>TP2 Zone:</b>
{sig['tp2_low']:.4f} – {sig['tp2_high']:.4f}
(H1/H4 Major Swing High){tp3_str}
📐 <b>R:R Tham Khảo:</b>
TP1: 1:{sig['rr_tp1']:.2f} | TP2: 1:{sig['rr_tp2']:.2f}
-------------------------------------
👉 <b>ĐÃ BẬT THEO DÕI TP/SL REAL-TIME 24/7!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['status_label']}] cho {sig['symbol']} (Score: {sig['total_score']})", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Structural SL/TP Tracking Bot (Fixed Telegram)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=50)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang quét thang điểm Kỹ Thuật cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                h1_candles = get_closed_klines(symbol, "60", limit=50)
                h4_candles = get_closed_klines(symbol, "240", limit=50)
                
                sig = evaluate_accumulation_reclaim(symbol, h1_candles, h4_candles, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.12)

        if candidates:
            candidates.sort(key=lambda x: (x['total_score'], x['oi_pct'], x['vol_ratio']), reverse=True)
            top_candidates = candidates[:MAX_ALERTS_PER_CYCLE]

            for c in top_candidates:
                processed_signal_ids.add(c['signal_id'])
                send_telegram_signal(c)
            
        print(f"✅ Hoàn thành 1 vòng quét. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
