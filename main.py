import time
import requests
import os
import numpy as np
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - 5M ABNORMAL FLOW & H1/H4 TREND-FLOW ENGINE
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 10_000_000  # $10M USD Minimum Turnover
MIN_FLOW_SCORE = 75                    # Mức điểm tối thiểu để thông báo

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot 5M Abnormal Flow Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# DATA FETCHING HELPERS
# ==============================================================================

def get_all_bybit_crypto_symbols():
    """Lọc chỉ các mã Crypto USDT Perp trên Bybit (Loại bỏ Cổ phiếu, Vàng, Chỉ số, Token đòn bẩy)"""
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto_keywords = [
                'XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD',
                'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', 'AMD', 'INTC',
                '3L', '3S', '2L', '2S'
            ]
            parsed_list = []
            for t in tickers:
                sym = t['symbol']
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')):
                    if not any(k in sym for k in non_crypto_keywords):
                        parsed_list.append({
                            'symbol': sym,
                            'turnover24h': float(t.get('turnover24h', 0)),
                            'fundingRate': float(t.get('fundingRate', 0)) * 100,
                            'lastPrice': float(t.get('lastPrice', 0))
                        })
            return parsed_list
    except Exception as e:
        print(f"Lỗi lấy Tickers: {e}", flush=True)
    return []

def get_closed_klines(symbol, interval, category="linear", limit=60):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=8).json()
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

def get_taker_ratios_5m(symbol, category="linear", limit=3):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period=5m&limit={limit}"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def get_open_interest_pct_5m(symbol):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=5m&limit=3"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

def calculate_ema(candles, period):
    if len(candles) < period: return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema_values = [sum(closes[:period]) / period]
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

# ==============================================================================
# CORE 5M ABNORMAL FLOW DETECTOR WITH RELATIVE STRENGTH & ANTI-CHASE FILTER
# ==============================================================================

def analyze_5m_abnormal_flow(symbol_data, market_rs_map):
    symbol = symbol_data['symbol']
    turnover = symbol_data['turnover24h']
    funding = symbol_data['fundingRate']

    if turnover < MIN_FUTURES_TURNOVER_24H: return None
    if funding > 0.06 or funding < -0.06: return None

    # Lấy Klines 5M, 15M, 1H
    m5_klines = get_closed_klines(symbol, "5", limit=30)
    if not m5_klines or len(m5_klines) < 22: return None

    last_5m = m5_klines[-1]
    close_5m = last_5m['close']
    open_5m = last_5m['open']
    high_5m = last_5m['high']
    low_5m = last_5m['low']

    price_change_5m = (close_5m - open_5m) / open_5m * 100
    range_5m = (high_5m - low_5m) / low_5m * 100

    # Volume Expansion
    vols_20 = [c['volume'] for c in m5_klines[-21:-1]]
    avg_vol_20 = np.mean(vols_20) if vols_20 else 1.0
    vol_expansion = last_5m['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if vol_expansion < 1.5 and price_change_5m < 0.8: return None

    # Wick & Liquidity Sweep Calculation
    body_size = abs(close_5m - open_5m)
    lower_wick = (min(open_5m, close_5m) - low_5m)
    upper_wick = (high_5m - max(open_5m, close_5m))
    total_range = high_5m - low_5m
    
    lower_wick_ratio = (lower_wick / total_range * 100) if total_range > 0 else 0.0
    upper_wick_ratio = (upper_wick / total_range * 100) if total_range > 0 else 0.0

    is_liquidity_sweep = (lower_wick_ratio >= 40.0) and (close_5m > open_5m)

    # Order Flow Data
    taker_ratios = get_taker_ratios_5m(symbol)
    taker_buy_pct = taker_ratios[0] if taker_ratios else 50.0
    oi_change_5m = get_open_interest_pct_5m(symbol)

    # Relative Strength (RS Rank Percentile)
    rs_rank_pct = market_rs_map.get(symbol, 50.0)

    # --------------------------------------------------------------------------
    # ANTI-CHASE FILTER (MULTI-TIMEFRAME CHECK)
    # --------------------------------------------------------------------------
    m15_klines = get_closed_klines(symbol, "15", limit=10)
    h1_klines = get_closed_klines(symbol, "60", limit=10)

    p_change_15m = ((m15_klines[-1]['close'] - m15_klines[-3]['open']) / m15_klines[-3]['open'] * 100) if len(m15_klines) >= 3 else 0.0
    p_change_1h = ((h1_klines[-1]['close'] - h1_klines[-2]['open']) / h1_klines[-2]['open'] * 100) if len(h1_klines) >= 2 else 0.0

    is_chase_risk = False
    if price_change_5m > 3.8 or p_change_15m > 7.5 or p_change_1h > 12.0 or upper_wick_ratio > 45.0:
        is_chase_risk = True

    # --------------------------------------------------------------------------
    # CALCULATE ABNORMAL FLOW SCORE (0 - 100)
    # --------------------------------------------------------------------------
    
    # 1. Price Abnormality (20đ)
    score_price = min(20, int((price_change_5m / 2.5) * 20)) if price_change_5m > 0 else 5

    # 2. Volume Expansion (20đ)
    score_vol = 20 if vol_expansion >= 3.0 else (15 if vol_expansion >= 2.0 else 10)

    # 3. Taker Buy Pressure (15đ)
    score_taker = 15 if taker_buy_pct >= 70.0 else (10 if taker_buy_pct >= 58.0 else 5)

    # 4. OI Expansion (15đ)
    score_oi = 15 if oi_change_5m >= 2.5 else (10 if oi_change_5m >= 1.0 else 5)

    # 5. Wick / Liquidity Sweep (10đ)
    score_sweep = 10 if is_liquidity_sweep else 5

    # 6. Relative Strength (15đ)
    score_rs = 15 if rs_rank_pct <= 5.0 else (12 if rs_rank_pct <= 10.0 else (8 if rs_rank_pct <= 20.0 else 3))

    # 7. Market Regime (5đ)
    score_regime = 5 if funding >= 0 else 3

    flow_score = score_price + score_vol + score_taker + score_oi + score_sweep + score_rs + score_regime

    # Phạt điểm nếu dính CHASE RISK
    if is_chase_risk:
        flow_score = max(30, flow_score - 25)

    # --------------------------------------------------------------------------
    # SIGNAL CLASSIFICATION
    # --------------------------------------------------------------------------
    signal_category = ""
    if is_liquidity_sweep and taker_buy_pct >= 55.0 and rs_rank_pct <= 20.0:
        signal_category = "A. LIQUIDITY SWEEP"
    elif rs_rank_pct <= 5.0 and vol_expansion >= 3.0 and not is_chase_risk:
        signal_category = "C. EXPLOSION"
    elif rs_rank_pct <= 20.0 and vol_expansion >= 1.8 and taker_buy_pct >= 55.0 and not is_chase_risk:
        signal_category = "B. EARLY PUMP / EARLY FLOW" # Priority 1
    else:
        if flow_score >= 80 and not is_chase_risk:
            signal_category = "B. EARLY PUMP / EARLY FLOW"
        else:
            return None

    return {
        'symbol': symbol,
        'category': signal_category,
        'flow_score': flow_score,
        'price_change_5m': price_change_5m,
        'vol_expansion': vol_expansion,
        'oi_change_5m': oi_change_5m,
        'taker_buy_pct': taker_buy_pct,
        'rs_rank_pct': rs_rank_pct,
        'is_sweep': is_liquidity_sweep,
        'is_chase_risk': is_chase_risk,
        'last_price': close_5m,
        'funding': funding,
        'timestamp': last_5m['timestamp']
    }

# ==============================================================================
# H1 / H4 CONFIRMATION ENGINE (VERIFY BEFORE CALLING LONG)
# ==============================================================================

def verify_h1_h4_long_setup(symbol):
    """
    Kiểm tra cấu trúc H1/H4 của coin bất thường 5M để tạo điểm Entry, SL, TP chuẩn xác
    """
    h4_klines = get_closed_klines(symbol, "240", limit=60)
    h1_klines = get_closed_klines(symbol, "60", limit=60)

    if not h4_klines or not h1_klines: return None

    ema34_h4 = calculate_ema(h4_klines, 34)
    ema89_h4 = calculate_ema(h4_klines, 89)
    ema34_h1 = calculate_ema(h1_klines, 34)
    h1_atr = calculate_atr(h1_klines, 14)

    if not ema34_h4 or not ema89_h4 or not ema34_h1 or h1_atr <= 0: return None

    curr_price = h1_klines[-1]['close']

    # Hard Gate check H4 Trend Alignment
    if not (curr_price > ema34_h4[-1] and ema34_h4[-1] >= ema89_h4[-1]):
        return None # H4 chưa đồng thuận Trend

    # Hỗ trợ bảo vệ H1
    recent_lows = [c['low'] for c in h1_klines[-6:]]
    support_low = min(recent_lows)

    sl_price = support_low - (0.2 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 3.8: return None

    tp1_price = curr_price + (h1_atr * 2.8)
    tp2_price = curr_price + (h1_atr * 4.5)

    return {
        'entry_low': support_low,
        'entry_high': curr_price,
        'sl': sl_price,
        'risk_pct': risk_pct,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': (tp1_price - curr_price) / curr_price * 100,
        'tp2_pct': (tp2_price - curr_price) / curr_price * 100
    }

# ==============================================================================
# TELEGRAM ALERT FORMATTER (ĐÚNG 100% MẪU BẠN YÊU CẦU)
# ==============================================================================

def format_and_send_telegram_alert(flow, h1_setup=None):
    global signal_counter
    signal_counter += 1

    chase_warning = "\n⚠️ <b>CẢNH BÁO: CHASE RISK (Giá đã pump xa)</b>" if flow['is_chase_risk'] else ""
    sweep_text = "YES" if flow['is_sweep'] else "NO"

    # Headline dựa theo phân loại
    head_icon = "🚨 EARLY FLOW" if "EARLY" in flow['category'] else ("🧲 LIQUIDITY SWEEP" if "SWEEP" in flow['category'] else "🔥 EXPLOSION")

    msg = f"""<b>{head_icon} — #{flow['symbol']}</b>{chase_warning}
⚡️ <b>5M:</b> <code>{flow['price_change_5m']:+.2f}%</code>
📊 <b>VOL:</b> <code>{flow['vol_expansion']:.1f}x</code>
📈 <b>OI:</b> <code>{flow['oi_change_5m']:+.1f}%</code>
🟢 <b>TAKER BUY:</b> <code>{flow['taker_buy_pct']:.0f}%</code>
🔥 <b>RS:</b> <code>TOP {flow['rs_rank_pct']:.1f}%</code>
🧲 <b>SWEEP:</b> <code>{sweep_text}</code>
⭐️ <b>FLOW SCORE:</b> <code>{flow['flow_score']}/100</code>

🟢 <b>MONEY FLOW IN</b>
🟢 <b>STRONGER THAN ALT MARKET</b>"""

    # Nếu được xác nhận qua bộ lọc H1/H4 -> Bổ sung Kế hoạch Trading
    if h1_setup:
        msg += f"""\n
━━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>H1/H4 CONFIRMED TRADING PLAN:</b>
📍 <b>Entry Zone:</b> <code>{h1_setup['entry_low']:.4f} – {h1_setup['entry_high']:.4f}</code>
🛑 <b>Stop Loss:</b> <code>{h1_setup['sl']:.4f}</code> (-{h1_setup['risk_pct']:.2f}%)
🎯 <b>TP1 Target:</b> <code>{h1_setup['tp1']:.4f}</code> (+{h1_setup['tp1_pct']:.1f}%)
🚀 <b>TP2 Target:</b> <code>{h1_setup['tp2']:.4f}</code> (+{h1_setup['tp2_pct']:.1f}%)
👉 <b>CHECK H1 NGAY!</b>"""
    else:
        msg += f"\n\n👉 <b>CHECK H1 NGAY</b> (Đang theo dõi điểm hồi Retest H1)"

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT SENT] #{flow['symbol']} [{flow['category']}] Score: {flow['flow_score']}", flush=True)

# ==============================================================================
# MANUAL /scan COMMAND PROCESSOR
# ==============================================================================

def execute_manual_scan():
    send_telegram_direct_message("🔍 <b>ĐANG SCAN BIẾN ĐỘNG 5M & RELATIVE STRENGTH TOÀN BỘ ALTCOIN...</b>\n⏳ Vui lòng chờ 10-15 giây!")
    
    all_tickers = get_all_bybit_crypto_symbols()
    
    # 1. Tính toán Relative Strength toàn thị trường Altcoin
    ticker_5m_changes = {}
    for t in all_tickers[:120]: # Sample 120 coin thanh khoản tốt nhất
        klines = get_closed_klines(t['symbol'], "5", limit=3)
        if klines and len(klines) >= 2:
            pct = (klines[-1]['close'] - klines[-1]['open']) / klines[-1]['open'] * 100
            ticker_5m_changes[t['symbol']] = pct

    if not ticker_5m_changes:
        send_telegram_direct_message("⚠️ Không lấy được dữ liệu thị trường 5M. Vui lòng thử lại!")
        return

    # Sắp xếp Rank RS
    sorted_symbols = sorted(ticker_5m_changes.items(), key=lambda x: x[1], reverse=True)
    total_count = len(sorted_symbols)
    rs_map = {}
    for idx, (sym, val) in enumerate(sorted_symbols):
        percentile = ((idx + 1) / total_count) * 100
        rs_map[sym] = percentile

    # 2. Rà soát Abnormal Flow
    matched_results = []
    for t in all_tickers:
        try:
            res = analyze_5m_abnormal_flow(t, rs_map)
            if res and res['flow_score'] >= 70:
                matched_results.append(res)
        except Exception:
            pass
        time.sleep(0.02)

    if not matched_results:
        send_telegram_direct_message("📊 <b>KẾT QUẢ /SCAN REAL-TIME 5M:</b>\n\nThị trường đi ngang tĩnh. Chưa phát hiện Altcoin nào xuất hiện Dòng Tiền Mới Bất Thường trong 5 phút qua.")
        return

    matched_results.sort(key=lambda x: x['flow_score'], reverse=True)
    top_candidates = matched_results[:4]

    for flow in top_candidates:
        h1_confirm = verify_h1_h4_long_setup(flow['symbol'])
        format_and_send_telegram_alert(flow, h1_confirm)
        time.sleep(1)

def process_telegram_commands():
    last_update_id = 0
    print("🤖 Bật luồng Telegram Listener (/scan, /ping)...", flush=True)
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10"
            res = requests.get(url, timeout=12).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip().lower()
                    
                    if text in ["/scan", "scan"]:
                        Thread(target=execute_manual_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot 5M Abnormal Flow Engine đang chạy 24/7. Nhập <b>/scan</b> để tìm dòng tiền sớm!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# MAIN RADAR LOOP
# ==============================================================================

def run_radar_loop():
    print("⚡️ Bắt đầu Luồng Radar 5M Abnormal Flow Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            
            # Tính Relative Strength Map
            ticker_5m_changes = {}
            for t in all_tickers[:120]:
                klines = get_closed_klines(t['symbol'], "5", limit=3)
                if klines and len(klines) >= 2:
                    pct = (klines[-1]['close'] - klines[-1]['open']) / klines[-1]['open'] * 100
                    ticker_5m_changes[t['symbol']] = pct

            total_count = len(ticker_5m_changes) if ticker_5m_changes else 1
            sorted_symbols = sorted(ticker_5m_changes.items(), key=lambda x: x[1], reverse=True)
            rs_map = {sym: ((idx + 1) / total_count) * 100 for idx, (sym, val) in enumerate(sorted_symbols)}

            print(f"🔍 [5M SCANNER] Đang rà soát Abnormal Flow cho {len(all_tickers)} altcoins...", flush=True)
            
            for ticker in all_tickers:
                try:
                    flow = analyze_5m_abnormal_flow(ticker, rs_map)
                    if flow and flow['flow_score'] >= MIN_FLOW_SCORE:
                        # Cooldown check
                        now = time.time()
                        sym = flow['symbol']
                        if sym not in symbol_cooldowns or (now - symbol_cooldowns[sym]) >= 14400:
                            symbol_cooldowns[sym] = now
                            # Chuyển qua bộ xác nhận H1/H4
                            h1_confirm = verify_h1_h4_long_setup(sym)
                            format_and_send_telegram_alert(flow, h1_confirm)
                except Exception:
                    pass
                time.sleep(0.04)

        except Exception as e:
            print(f"Lỗi Radar Loop: {e}", flush=True)

        print(f"✅ Hoàn tất 1 lượt quét 5M. Tạm nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
