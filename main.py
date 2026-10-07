import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - ADAPTIVE SCORING AUTO SCANNER
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Lọc Altcoin thanh khoản >= $8M USD
TOP_TIER1_LIMIT = 30                   # Tầng 1 lọc chọn Top 30 coin biến động nhất
MIN_ALERT_SCORE = 80                   # Báo về Telegram từ mức DEVELOPING (>=80 điểm)
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_alerts = {}                  # Lưu cooldown & score gần nhất
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Adaptive Scoring Multi-Setup Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=6)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# DATA FETCHING HELPERS
# ==============================================================================

def get_all_bybit_crypto_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=5).json()
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
                turnover = float(t.get('turnover24h', 0))
                price_change = float(t.get('price24hPcnt', 0)) * 100
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and turnover >= MIN_FUTURES_TURNOVER_24H:
                    if not any(k in sym for k in non_crypto_keywords):
                        parsed_list.append({
                            'symbol': sym,
                            'turnover24h': turnover,
                            'price24hPcnt': price_change,
                            'fundingRate': float(t.get('fundingRate', 0)) * 100,
                            'lastPrice': float(t.get('lastPrice', 0))
                        })
            return parsed_list
    except Exception as e:
        print(f"Lỗi lấy Tickers: {e}", flush=True)
    return []

def get_klines_with_current(symbol, interval, limit=35):
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            parsed = []
            for c in reversed(raw_list):
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

def get_open_interest_pct(symbol, interval_str="5m"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
    try:
        res = requests.get(url, timeout=3).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

def get_taker_buy_sell_ratio(symbol, interval_str="5m"):
    url = f"{BASE_URL}/v5/market/account-ratio?category=spot&symbol={symbol}&period={interval_str}&limit=2"
    try:
        res = requests.get(url, timeout=3).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("buyRatio", 0.5)) * 100
    except Exception:
        pass
    return 50.0

def calculate_ema(candles, period):
    if len(candles) < period: return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema_values = [sum(closes[:period]) / period]
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

# ==============================================================================
# ADAPTIVE SCORING ENGINE (100-POINT BREAKDOWN)
# ==============================================================================

def analyze_adaptive_scoring_coin(ticker_data, rs_map):
    sym = ticker_data['symbol']
    funding = ticker_data['fundingRate']

    m5_candles = get_klines_with_current(sym, "5", limit=30)
    h1_candles = get_klines_with_current(sym, "60", limit=35)

    if not m5_candles or len(m5_candles) < 22 or not h1_candles or len(h1_candles) < 22:
        return None

    last_5m = m5_candles[-1]
    curr_price = last_5m['close']

    # 1. PRICE STRUCTURE (20 PTS)
    curr_h1 = h1_candles[-1]
    prev_swing_low = min([c['low'] for c in h1_candles[-7:-1]])
    prev_swing_high = max([c['high'] for c in h1_candles[-7:-1]])

    open_h1, high_h1, low_h1, close_h1 = curr_h1['open'], curr_h1['high'], curr_h1['low'], curr_h1['close']
    body_h1 = abs(close_h1 - open_h1)
    lower_wick_h1 = min(open_h1, close_h1) - low_h1
    upper_wick_h1 = high_h1 - max(open_h1, close_h1)

    is_sweep_low = (low_h1 < prev_swing_low) and (close_h1 >= prev_swing_low * 0.998)
    is_sweep_high = (high_h1 > prev_swing_high) and (close_h1 <= prev_swing_high * 1.002)
    wick_ratio_lower = (lower_wick_h1 / body_h1) if body_h1 > 0 else 0.0
    wick_ratio_upper = (upper_wick_h1 / body_h1) if body_h1 > 0 else 0.0

    score_structure = 0
    if is_sweep_low or is_sweep_high: score_structure += 10
    if wick_ratio_lower >= 1.8 or wick_ratio_upper >= 1.8: score_structure += 6
    elif wick_ratio_lower >= 1.2 or wick_ratio_upper >= 1.2: score_structure += 3
    if close_h1 >= prev_swing_low or close_h1 <= prev_swing_high: score_structure += 4

    # 2. VOLUME / MOMENTUM 5M (20 PTS)
    vols_5m_20 = [c['volume'] for c in m5_candles[-21:-1]]
    avg_vol_5m = np.mean(vols_5m_20) if vols_5m_20 else 1.0
    vol_5m_ratio = last_5m['volume'] / avg_vol_5m if avg_vol_5m > 0 else 1.0

    score_vol = 0
    if vol_5m_ratio >= 3.0: score_vol += 20
    elif vol_5m_ratio >= 2.0: score_vol += 15
    elif vol_5m_ratio >= 1.4: score_vol += 10
    else: score_vol += 4

    # 3. OPEN INTEREST (15 PTS)
    oi_pct_5m = get_open_interest_pct(sym, "5m")
    score_oi = 0
    if oi_pct_5m >= 3.0: score_oi += 15
    elif oi_pct_5m >= 1.2: score_oi += 11
    elif oi_pct_5m >= 0.5: score_oi += 6
    else: score_oi += 2

    # 4. CVD / SPOT FLOW (15 PTS)
    taker_buy_pct = get_taker_buy_sell_ratio(sym, "5m")
    score_cvd = 0
    if taker_buy_pct >= 68.0 or taker_buy_pct <= 32.0: score_cvd += 15
    elif taker_buy_pct >= 60.0 or taker_buy_pct <= 40.0: score_cvd += 10
    else: score_cvd += 4

    # 5. RELATIVE STRENGTH (10 PTS)
    rs_percentile = rs_map.get(sym, 50.0)
    score_rs = 0
    if rs_percentile <= 5.0: score_rs += 10
    elif rs_percentile <= 15.0: score_rs += 7
    elif rs_percentile <= 30.0: score_rs += 4
    else: score_rs += 1

    # 6. H1 TREND / EMA / RSI (10 PTS)
    ema34_h1 = calculate_ema(h1_candles, 34)
    rsi_h1_list = calculate_rsi(h1_candles, 14)
    rsi_h1 = rsi_h1_list[-1] if rsi_h1_list else 50.0

    score_trend = 0
    if ema34_h1 and abs(close_h1 - ema34_h1[-1]) / close_h1 <= 0.012: score_trend += 5
    if 45.0 <= rsi_h1 <= 62.0: score_trend += 5

    # 7. BTC REGIME & FUNDING (10 PTS COMBINED)
    score_btc = 5
    score_funding = 5 if abs(funding) <= 0.03 else 2

    # TỔNG ĐIỂM ADAPTIVE SCORE (MAX 100)
    total_score = score_structure + score_vol + score_oi + score_cvd + score_rs + score_trend + score_btc + score_funding

    # XÁC ĐỊNH HƯỚNG VÀO LỆNH (LONG HOẶC SHORT)
    direction = "LONG" if (close_h1 >= open_h1 or taker_buy_pct >= 52.0 or is_sweep_low) else "SHORT"

    # CHỐNG PUMP/DUMP BỊ OVEREXTENDED QUÁ XA (> 9%)
    p_change_3h = (close_h1 - h1_candles[-4]['open']) / h1_candles[-4]['open'] * 100 if len(h1_candles) >= 4 else 0
    is_overextended = abs(p_change_3h) >= 9.0

    # --------------------------------------------------------------------------
    # SURGE OVERRIDE LOGIC (DÒNG TIỀN ĐỘT PHÁ - BẮT SỚM)
    # --------------------------------------------------------------------------
    is_surge_override = False
    if vol_5m_ratio >= 3.0 and oi_pct_5m >= 4.0 and taker_buy_pct >= 65.0 and rs_percentile <= 5.0 and not is_overextended:
        is_surge_override = True
        total_score = max(total_score, 90) # Kích hoạt mức EARLY LONG/SHORT ngay

    # BỎ QUA COIN CHƯA ĐỦ ĐIỂM
    if total_score < 70 or is_overextended: return None

    # NHẬN DIỆN DẠNG SETUP CHÍNH (SETUP TYPE)
    setup_type = "H1 REVERSAL"
    if is_surge_override: setup_type = "MOMENTUM SURGE OVERRIDE"
    elif vol_5m_ratio >= 2.5 and oi_pct_5m >= 2.0: setup_type = "MOMENTUM BREAKOUT"
    elif rs_percentile <= 5.0: setup_type = "RELATIVE STRENGTH"
    elif score_trend == 10: setup_type = "PULLBACK CONTINUATION"
    elif score_structure < 8 and oi_pct_5m >= 1.5: setup_type = "ACCUMULATION → EXPANSION"

    # LIỆT KÊ NGUYÊN NHÂN KÍCH HOẠT (WHY TRIGGERED) & BỊ THIẾU (MISSING)
    why_triggered = []
    missing_factors = []

    if is_sweep_low or is_sweep_high: why_triggered.append("Liquidity Sweep")
    else: missing_factors.append("No Sweep")

    if vol_5m_ratio >= 2.0: why_triggered.append(f"Vol 5M {vol_5m_ratio:.1f}x")
    else: missing_factors.append("Low Vol 5M")

    if oi_pct_5m >= 1.2: why_triggered.append(f"OI +{oi_pct_5m:.1f}%")
    else: missing_factors.append("Low OI Flow")

    if taker_buy_pct >= 60.0 or taker_buy_pct <= 40.0: why_triggered.append(f"CVD Flow {taker_buy_pct:.0f}%")

    if rs_percentile <= 10.0: why_triggered.append(f"Top {rs_percentile:.1f}% RS")

    # XÁC ĐỊNH LEVEL TÊN GỌI THEO TẦNG ĐIỂM
    tier_label = "WATCH"
    if total_score >= 95: tier_label = "🚀 HIGH CONVICTION"
    elif total_score >= 90: tier_label = "🔥 EARLY LONG" if direction == "LONG" else "🔥 EARLY SHORT"
    elif total_score >= 85: tier_label = "🟢 CANDIDATE"
    elif total_score >= 80: tier_label = "🟡 DEVELOPING"

    return {
        'symbol': sym,
        'direction': direction,
        'score': total_score,
        'setup_type': setup_type,
        'tier_label': tier_label,
        'entry': curr_price,
        'why_triggered': ", ".join(why_triggered) if why_triggered else "Base Momentum",
        'missing_factors': ", ".join(missing_factors) if missing_factors else "None",
        'vol_5m_ratio': vol_5m_ratio,
        'oi_pct_5m': oi_pct_5m,
        'taker_buy_pct': taker_buy_pct,
        'rs_percentile': rs_percentile,
        'rsi_h1': rsi_h1,
        'funding': funding,
        'h1_timestamp': curr_h1['timestamp']
    }

# ==============================================================================
# TELEGRAM ALERT FORMATTER (ĐÚNG FORMAT CHI TIẾT)
# ==============================================================================

def send_telegram_adaptive_alert(sig):
    now_time = time.time()
    sym = sig['symbol']
    
    # Anti-spam Cooldown Logic
    if sym in processed_alerts:
        last_time, last_score = processed_alerts[sym]
        if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 6):
            return
    processed_alerts[sym] = (now_time, sig['score'])

    cvd_text = "BUY PRESSURE" if sig['taker_buy_pct'] >= 58 else ("SELL PRESSURE" if sig['taker_buy_pct'] <= 42 else "NEUTRAL")
    risk_text = "NORMAL" if abs(sig['funding']) <= 0.03 else "HIGH FUNDING / OVERHEAT"

    msg = f"""<b>{sig['tier_label']} — {sig['direction']}</b>

🪙 <b>#{sig['symbol']}</b>
⭐ <b>Score:</b> <code>{sig['score']}/100</code>
🎯 <b>Setup Type:</b> <code>{sig['setup_type']}</code>
📍 <b>Entry Zone:</b> <code>{sig['entry']:.4f}</code>

✅ <b>WHY IT TRIGGERED:</b>
<code>{sig['why_triggered']}</code>

⚠️ <b>MISSING FACTORS:</b>
<code>{sig['missing_factors']}</code>

📊 <b>ORDERFLOW & METRICS:</b>
• Volume 5M: <code>{sig['vol_5m_ratio']:.1f}x</code>
• OI Expansion: <code>{sig['oi_pct_5m']:+.1f}%</code>
• CVD Pressure: <code>{cvd_text} ({sig['taker_buy_pct']:.0f}%)</code>
• RS Rank: <code>TOP {sig['rs_percentile']:.1f}%</code>
• RSI H1: <code>{sig['rsi_h1']:.1f}</code>
• Risk / Overheat: <code>{risk_text}</code>

⚡️ <b>EARLY — Check Chart ngay!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ADAPTIVE ALERT SENT] {sig['tier_label']} #{sig['symbol']} ({sig['setup_type']}) Score: {sig['score']}", flush=True)

# ==============================================================================
# AUTOMATIC RUNNER (QUÉT TỰ ĐỘNG MỖI 5 PHÚT)
# ==============================================================================

def run_adaptive_auto_scanner():
    print("⚡️ Bắt đầu Luồng Adaptive Scoring Auto Scanner...", flush=True)
    while True:
        try:
            start_time = time.time()
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"\n🔍 [ADAPTIVE SCANNER] Bắt đầu rà soát {len(all_tickers)} altcoins...", flush=True)

            # Tính Bảng Sức Mạnh Tương Đối (Relative Strength Rank)
            sorted_by_change = sorted(all_tickers, key=lambda x: x['price24hPcnt'], reverse=True)
            total_count = len(sorted_by_change) if sorted_by_change else 1
            rs_map = {t['symbol']: ((idx + 1) / total_count) * 100 for idx, t in enumerate(sorted_by_change)}

            # Tầng 1: Lọc Top 30 coin có xung lực dòng tiền năng động nhất
            top30_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            # Tầng 2: Phân tích sâu Adaptive Scoring
            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(analyze_adaptive_scoring_coin, t, rs_map) for t in top30_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            # Sắp xếp theo Score giảm dần
            scanned_results.sort(key=lambda x: x['score'], reverse=True)

            elapsed = time.time() - start_time
            print(f"✅ Hoàn tất phân tích trong {elapsed:.1f}s. Tìm thấy {len(scanned_results)} coin tiềm năng.", flush=True)

            if scanned_results:
                # Báo Telegram Top 5 coin có score cao nhất
                for sig in scanned_results[:5]:
                    if sig['score'] >= MIN_ALERT_SCORE:
                        send_telegram_adaptive_alert(sig)
            else:
                print("🔎 Không có setup đảo chiều/bùng nổ đáng chú ý trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"Lỗi Adaptive Scanner Loop: {e}", flush=True)

        print(f"⏳ Tạm nghỉ {SCAN_INTERVAL_SECONDS}s (5 phút) trước lượt quét tiếp theo...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_adaptive_auto_scanner()
