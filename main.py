import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - TWO-TIER ARCHITECTURE (100-POINT QUALITY FILTER + ENTRY ENGINE)
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 10              
MAX_ALERTS_PER_CYCLE = 2           

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
    return "Bybit 2-Tier Coin Quality Filter & H1 Entry Engine Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang vận hành 2-Tier Engine 24/7!\n⏳ Chưa có tín hiệu H1 LONG đạt chuẩn phát ra từ lần khởi động gần nhất."
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
        
        msg += f"<b>#{sig['id']} [{sig['signal_timeframe']} LONG] {sig['symbol']}</b> (Score: {sig['filter_score']}/100)\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot 2-Tier Engine đang hoạt động 24/7!")
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

                    # CANCEL NẾU GIÁ CHẠY QÚA XA ENTRY ZONE
                    if not sig.get('entered', False):
                        if curr_price > sig['entry_high'] * 1.018:
                            sig['status_text'] = f"⚠️ CANCEL (Rời xa {tf} Entry Zone)"
                            msg = f"⚠️ <b>[CẢNH BÁO CHASE PUMP] #{sig['id']} - {sig['symbol']}</b>\nGiá (<code>{curr_price:.4f}</code>) đã rời quá xa Entry Zone. Tự động hủy lệnh!"
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
🎯 {tf} TP1 Target: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
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
⚠️ <i>Vi phạm mốc Hỗ trợ Cấu trúc {tf}. Dừng lệnh an toàn!</i>"""
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

def get_taker_buy_history(symbol, limit=6):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period=1h&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

def find_valid_timeframe_targets(candles, entry_price):
    swing_highs = []
    for i in range(2, len(candles) - 2):
        c = candles[i]
        if (c['high'] > candles[i-1]['high'] and c['high'] > candles[i-2]['high'] and
            c['high'] > candles[i+1]['high'] and c['high'] > candles[i+2]['high']):
            if c['high'] > entry_price * 1.006:
                swing_highs.append(c['high'])
                
    unique_targets = []
    for target in sorted(swing_highs):
        if not unique_targets or (target - unique_targets[-1]) / unique_targets[-1] * 100 >= 0.4:
            unique_targets.append(target)
            
    return unique_targets

# ==============================================================================
# 4. TẦNG 1: H1 COIN QUALITY FILTER (100-POINT SYSTEM)
# ==============================================================================
def run_h1_coin_quality_filter(ticker_data, btc_h1_candles):
    symbol = ticker_data['symbol']
    turnover = ticker_data['turnover24h']
    change_24h = ticker_data['price24hPcnt']
    funding = ticker_data['fundingRate']

    # --- 1. MANDATORY REJECTIONS (BẮT BUỘC LOẠI COIN) ---
    if turnover < 10_000_000: return None # 1. Volume 24h < $10M

    h1_candles = get_closed_klines(symbol, "60", limit=50)
    if len(h1_candles) < 30: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_change = (curr_price - last_h1['open']) / last_h1['open'] * 100

    if h1_change > 6.0: return None # 8. Coin vừa pump > 6% trong H1

    ema34_list = calculate_ema(h1_candles, 34)
    ema89_list = calculate_ema(h1_candles, 89)
    if not ema34_list or not ema89_list: return None

    ema34_val = ema34_list[-1]
    dist_ema34 = (curr_price - ema34_val) / ema34_val * 100

    rsi_list = calculate_rsi(h1_candles, 14)
    curr_rsi = rsi_list[-1] if rsi_list else 50.0

    if funding > 0.08: return None # 5. Funding > +0.08% (Overheat)
    if change_24h > 10.0 and dist_ema34 > 6.0: return None # 6. Pump 24h >10% & cách EMA34 >6%
    if curr_rsi > 75.0 and dist_ema34 > 5.0: return None # 7. RSI >75 & cách EMA34 >5%

    taker_history = get_taker_buy_history(symbol, 6)
    curr_taker = taker_history[0] if taker_history else 50.0

    # BẮT BÚT CÁC MỐC GIÁ - CVD - OI LIÊN TỤC 3 H1
    oi_pct_h1 = get_open_interest_pct(symbol, "1h")
    
    # Check Price ↑ + OI ↑ + CVD ↓ (Flow Divergence)
    is_price_up = last_h1['close'] > h1_candles[-2]['close']
    is_oi_up = oi_pct_h1 > 0.5
    is_cvd_down = curr_taker < 48.0

    if is_price_up and is_cvd_down: return None # 3. CVD giảm mạnh khi giá tăng

    # --- 2. CHẤM ĐIỂM CHI TIẾT (100 ĐIỂM) ---
    score_liquidity = 0
    if turnover >= 100_000_000: score_liquidity += 7
    elif turnover >= 50_000_000: score_liquidity += 6
    elif turnover >= 30_000_000: score_liquidity += 4
    elif turnover >= 10_000_000: score_liquidity += 2

    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    rvol = last_h1['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if rvol > 2.0: score_liquidity += 8 # Chuyển 3đ spread sang Volume 24h/RVOL
    elif rvol >= 1.5: score_liquidity += 6
    elif rvol >= 1.3: score_liquidity += 5
    elif rvol >= 1.0: score_liquidity += 3
    elif rvol >= 0.8: score_liquidity += 1

    # SPOT BUYING FLOW (25đ)
    score_spot_flow = 0
    cvd_trend = "NORMAL"
    if curr_taker >= 58.0 and h1_change <= 3.0:
        score_spot_flow += 10
        cvd_trend = "↑"
    elif curr_taker >= 52.0:
        score_spot_flow += 5
        cvd_trend = "↑"
    else:
        score_spot_flow += 2
        cvd_trend = "→"

    if curr_taker > 62.0: score_spot_flow += 8
    elif curr_taker >= 58.0: score_spot_flow += 7
    elif curr_taker >= 55.0: score_spot_flow += 6
    elif curr_taker >= 52.0: score_spot_flow += 4
    elif curr_taker >= 48.0: score_spot_flow += 2

    high_taker_count = sum(1 for x in taker_history[:6] if x > 55.0)
    if high_taker_count >= 4: score_spot_flow += 7
    elif high_taker_count >= 3: score_spot_flow += 5
    elif high_taker_count >= 2: score_spot_flow += 4
    else: score_spot_flow += 2

    score_spot_flow = min(score_spot_flow, 25)

    # OPEN INTEREST (20đ)
    score_oi = 0
    if 5.0 <= oi_pct_h1 <= 8.0: score_oi += 8
    elif 2.0 <= oi_pct_h1 < 5.0: score_oi += 6
    elif 0.0 <= oi_pct_h1 < 2.0: score_oi += 4
    elif -5.0 <= oi_pct_h1 < 0.0: score_oi += 2

    if is_price_up and is_oi_up: score_oi += 7
    elif not is_price_up and is_oi_up: score_oi += 4
    elif is_price_up and not is_oi_up: score_oi += 3
    else: score_oi += 2

    if is_oi_up and curr_taker >= 52.0: score_oi += 5
    elif is_oi_up: score_oi += 2

    # Trừ điểm Flow Divergence
    if is_price_up and is_oi_up and is_cvd_down: score_oi = max(0, score_oi - 5)
    score_oi = min(score_oi, 20)

    # FUNDING RATE (10đ)
    score_funding = 0
    if -0.03 <= funding <= 0.01: score_funding = 10
    elif 0.01 < funding <= 0.03: score_funding = 8
    elif 0.03 < funding <= 0.05: score_funding = 5
    elif 0.05 < funding <= 0.08: score_funding = 2
    elif funding < -0.03: score_funding = 7

    # RELATIVE STRENGTH (15đ)
    score_rs = 0
    btc_h1_chg = (btc_h1_candles[-1]['close'] - btc_h1_candles[-1]['open']) / btc_h1_candles[-1]['open'] * 100 if btc_h1_candles else 0.0
    if h1_change > btc_h1_chg + 0.5: score_rs += 5
    elif h1_change >= btc_h1_chg: score_rs += 3
    else: score_rs += 1

    ema89_val = ema89_list[-1]
    if curr_price > ema34_val > ema89_val: score_rs += 5
    elif curr_price > ema34_val: score_rs += 3
    elif abs(curr_price - ema34_val)/ema34_val * 100 <= 0.5: score_rs += 2

    if 55.0 <= curr_rsi <= 65.0: score_rs += 5
    elif 45.0 <= curr_rsi < 55.0: score_rs += 4
    elif 40.0 <= curr_rsi < 45.0 or 65.0 < curr_rsi <= 70.0: score_rs += 2
    score_rs = min(score_rs, 15)

    # OVERHEAT & RISK FILTER (15đ)
    score_overheat = 0
    if -2.0 <= h1_change <= 2.0: score_overheat += 5
    elif 2.0 < h1_change <= 4.0: score_overheat += 4
    elif 4.0 < h1_change <= 6.0: score_overheat += 2
    elif -5.0 <= h1_change < -2.0: score_overheat += 2

    if 0.0 <= dist_ema34 <= 2.0: score_overheat += 5
    elif 2.0 < dist_ema34 <= 4.0: score_overheat += 4
    elif 4.0 < dist_ema34 <= 6.0: score_overheat += 2

    if -5.0 <= change_24h <= 3.0: score_overheat += 5
    elif 3.0 < change_24h <= 6.0: score_overheat += 4
    elif 6.0 < change_24h <= 10.0: score_overheat += 2
    score_overheat = min(score_overheat, 15)

    # FLOW QUALITY BONUS (+5 ĐIỂM)
    flow_bonus = 0
    if is_price_up and is_oi_up and curr_taker > 55.0 and rvol > 1.5:
        flow_bonus = 5

    total_score = min(score_liquidity + score_spot_flow + score_oi + score_funding + score_rs + score_overheat + flow_bonus, 100)

    if total_score < 70: return None # REJECT COIN DƯỚI 70 ĐIỂM

    # PHÂN LOẠI FLOW QUALITY CLASSIFICATION
    flow_type = "WATCHLIST"
    if is_price_up and 2.0 <= oi_pct_h1 <= 8.0 and curr_taker > 55.0 and rvol > 1.5 and funding <= 0.03:
        flow_type = "STRONG LONG FLOW"
    elif abs(h1_change) <= 1.0 and is_oi_up and curr_taker >= 52.0 and funding <= 0.03:
        flow_type = "ACCUMULATION"
    elif is_price_up and oi_pct_h1 < -2.0:
        flow_type = "SHORT SQUEEZE"
    elif funding > 0.05 or curr_rsi > 70.0 or dist_ema34 > 5.0:
        flow_type = "CROWDED LONG"

    # PHÂN LOẠI GRADE
    grade = "QUALITY COIN"
    if total_score >= 90: grade = "ELITE FLOW"
    elif total_score >= 80: grade = "STRONG FLOW"

    return {
        'symbol': symbol,
        'filter_score': total_score,
        'grade': grade,
        'flow_type': flow_type,
        'score_liquidity': score_liquidity,
        'score_spot_flow': score_spot_flow,
        'score_oi': score_oi,
        'score_funding': score_funding,
        'score_rs': score_rs,
        'score_overheat': score_overheat,
        'cvd_trend': cvd_trend,
        'taker_pct': curr_taker,
        'oi_pct': oi_pct_h1,
        'rvol': rvol,
        'funding': funding,
        'curr_rsi': curr_rsi,
        'h1_candles': h1_candles,
        'h4_candles': h4_candles,
        'lastPrice': curr_price
    }

# ==============================================================================
# 5. TẦNG 2: H1 ENTRY ENGINE (THỰC THI SETUP VÀO LỆNH THUẦN H1)
# ==============================================================================
def run_h1_entry_engine(quality_data):
    symbol = quality_data['symbol']
    h1_candles = quality_data['h1_candles']
    filter_score = quality_data['filter_score']
    curr_price = quality_data['lastPrice']

    last_h1 = h1_candles[-1]
    now = time.time()

    # --- SETUP 1: H1 SUPPORT SWEEP / RECLAIM ---
    recent_h1_lows = [c['low'] for c in h1_candles[-40:-5]]
    if recent_h1_lows:
        key_support = min(recent_h1_lows)
        h1_range = last_h1['high'] - last_h1['low']
        if h1_range > 0:
            lower_wick = (min(last_h1['open'], last_h1['close']) - last_h1['low'])
            wick_ratio = lower_wick / h1_range
            
            if (last_h1['low'] <= key_support * 1.008) and (curr_price >= key_support) and (wick_ratio >= 0.35):
                h1_atr = calculate_atr(h1_candles, 14)
                sl_price = last_h1['low'] - (0.35 * h1_atr)
                risk = curr_price - sl_price
                if risk > 0:
                    h1_targets = find_valid_timeframe_targets(h1_candles, curr_price)
                    tp1_price = h1_targets[0] if len(h1_targets) >= 1 else curr_price + (h1_atr * 1.8)
                    tp2_price = h1_targets[1] if len(h1_targets) >= 2 else tp1_price + (h1_atr * 2.5)
                    tp3_price = h1_targets[2] if len(h1_targets) >= 3 else tp2_price + (h1_atr * 3.2)

                    if (tp1_price - curr_price) > (risk * 1.0):
                        symbol_cooldowns[symbol] = now
                        return {
                            'signal_timeframe': 'H1',
                            'engine_tag': 'H1 SUPPORT SWEEP',
                            'title': '⚡️ H1 LONG',
                            'symbol': symbol,
                            'entry': curr_price,
                            'entry_low': key_support,
                            'entry_high': curr_price,
                            'sl': sl_price,
                            'sl_reason': 'H1 Sweep Low + ATR Buffer',
                            'tp1': tp1_price,
                            'tp1_reason': 'H1 Key Swing High',
                            'tp2': tp2_price,
                            'tp2_reason': 'H1 Resistance Zone',
                            'tp3': tp3_price,
                            'tp3_reason': 'H1 Supply Zone',
                            'filter_score': filter_score,
                            'grade': quality_data['grade'],
                            'flow_type': quality_data['flow_type'],
                            'quality_data': quality_data,
                            'risk_pct': (risk / curr_price) * 100,
                            'timestamp': last_h1['timestamp'],
                            'signal_id': f"H1_SWEEP_{symbol}_{last_h1['timestamp']}"
                        }

    # --- SETUP 2: H1 BREAKOUT -> BASE -> RECLAIM ---
    recent_h1_highs = [c['high'] for c in h1_candles[-40:-10]]
    if recent_h1_highs:
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

        if breakout_candle:
            base_candles = h1_candles[breakout_idx+1:]
            if 2 <= len(base_candles) <= 9:
                base_low = min([c['low'] for c in base_candles])
                base_high = max([c['high'] for c in base_candles])
                
                reclaim_type_a = (last_h1['low'] < breakout_level and curr_price >= breakout_level)
                reclaim_type_b = (last_h1['low'] >= base_low and curr_price > (base_low + base_high)/2 and last_h1['close'] > last_h1['open'])

                if reclaim_type_a or reclaim_type_b:
                    dist_from_breakout = (curr_price - breakout_level) / breakout_level * 100
                    if dist_from_breakout <= 1.5 and curr_price <= base_high * 1.012:
                        h1_atr = calculate_atr(h1_candles, 14)
                        sl_price = base_low - (0.35 * h1_atr)
                        risk = curr_price - sl_price
                        if risk > 0:
                            h1_targets = find_valid_timeframe_targets(h1_candles, curr_price)
                            tp1_price = h1_targets[0] if len(h1_targets) >= 1 else curr_price + (h1_atr * 1.8)
                            tp2_price = h1_targets[1] if len(h1_targets) >= 2 else tp1_price + (h1_atr * 2.5)
                            tp3_price = h1_targets[2] if len(h1_targets) >= 3 else tp2_price + (h1_atr * 3.2)

                            if (tp1_price - curr_price) > (risk * 1.0):
                                symbol_cooldowns[symbol] = now
                                return {
                                    'signal_timeframe': 'H1',
                                    'engine_tag': 'H1 BREAKOUT BASE RECLAIM',
                                    'title': '🟢 H1 LONG',
                                    'symbol': symbol,
                                    'entry': curr_price,
                                    'entry_low': min(breakout_level, base_low),
                                    'entry_high': curr_price,
                                    'sl': sl_price,
                                    'sl_reason': 'H1 Base Low + ATR Buffer',
                                    'tp1': tp1_price,
                                    'tp1_reason': 'H1 Key Swing High',
                                    'tp2': tp2_price,
                                    'tp2_reason': 'H1 Resistance Zone',
                                    'tp3': tp3_price,
                                    'tp3_reason': 'H1 Supply Zone',
                                    'filter_score': filter_score,
                                    'grade': quality_data['grade'],
                                    'flow_type': quality_data['flow_type'],
                                    'quality_data': quality_data,
                                    'risk_pct': (risk / curr_price) * 100,
                                    'timestamp': last_h1['timestamp'],
                                    'signal_id': f"H1_RECLAIM_{symbol}_{last_h1['timestamp']}"
                                }

    return None

# ==============================================================================
# 6. TELEGRAM OUTPUT FORMATTER (CHUẨN CẤU TRÚC ĐỘC LẬP)
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

    q = sig['quality_data']
    tf = sig['signal_timeframe']

    msg = f"""🔥 <b>H1 COIN QUALITY & LONG SIGNAL</b>

#{sig['symbol']}

Quality Score: <b>{sig['filter_score']}/100</b>
Grade: <b>{sig['grade']}</b> ({sig['flow_type']})

💧 Liquidity: {q['score_liquidity']}/15
💰 Spot Flow: {q['score_spot_flow']}/25
🧠 OI: {q['score_oi']}/20
💸 Funding: {q['score_funding']}/10
📈 Relative Strength: {q['score_rs']}/15
🔥 Overheat: {q['score_overheat']}/15

<b>FLOW DETAILS:</b>
• CVD: {q['cvd_trend']} | Taker Buy: {q['taker_pct']:.1f}%
• OI 24H: +{q['oi_pct']:.2f}% | RVOL: {q['rvol']:.1f}x
• Funding: {q['funding']:+.3f}% | RSI: {q['curr_rsi']:.1f}

━━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>{sig['title']} SETUP EXECUTION</b>

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

📐 <b>Structure:</b>
{sig['engine_tag']}

🌐 <b>Context:</b> H4 Context Bullish

⚠️ <i>Không chase nếu giá rời Entry Zone quá xa.</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT BẮN TELEGRAM 2-TIER] #{sig['id']} {sig['symbol']} (Score: {sig['filter_score']})", flush=True)

# ==============================================================================
# 7. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar 2-Tier System (Coin Quality Filter -> H1 Entry Engine)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_tickers = get_all_bybit_usdt_symbols()
        print(f"🔍 [TIER 1 FILTERING] Đang chấm điểm 100đ cho {len(all_tickers)} coin...", flush=True)
        
        # --- TẦNG 1: FILTER TOÀN SÀN VÀ LẤY TOP COIN CHẤT LƯỢNG ---
        quality_candidates = []
        for ticker in all_tickers:
            try:
                q_res = run_h1_coin_quality_filter(ticker, btc_h1)
                if q_res:
                    quality_candidates.append(q_res)
            except Exception:
                pass
            time.sleep(0.04)

        if quality_candidates:
            # Sắp xếp theo điểm Score Tầng 1 từ cao xuống thấp và lấy Top 15
            quality_candidates.sort(key=lambda x: x['filter_score'], reverse=True)
            top_candidates = quality_candidates[:15]
            print(f"🎯 [TIER 1 COMPLETE] Đã chọn Top {len(top_candidates)} coin chất lượng cao nhất để chuyển Tầng 2...", flush=True)

            # --- TẦNG 2: THỰC THI H1 ENTRY ENGINE TRÊN TOP COIN ---
            for cand in top_candidates:
                try:
                    sig = run_h1_entry_engine(cand)
                    if sig and sig['signal_id'] not in processed_signal_ids:
                        processed_signal_ids.add(sig['signal_id'])
                        send_telegram_signal(sig)
                except Exception:
                    pass

        print(f"✅ Hoàn tất 1 lượt quét 2 Tầng. Chờ {COOLDOWN_SECONDS}s quét tiếp...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
