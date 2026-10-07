import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - MOMENTUM & VOLUME & OI ENGINE (100-POINT SCORING)
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Tự động scan mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Lọc Altcoin thanh khoản >= $8M USD
TOP_TIER1_LIMIT = 30                   # Lọc Top 30 coin biến động nhất ở Tầng 1
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_alerts = {}                  # Lưu cooldown & score gần nhất
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Premium Momentum + Volume + OI Engine Active 24/7!"

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
# 1. API DATA HELPERS
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

def get_klines(symbol, interval, category="linear", limit=35):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
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

def get_open_interest_pct_30m(symbol):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=30m&limit=3"
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

def get_taker_buy_pct_5m(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=spot&symbol={symbol}&period=5m&limit=2"
    try:
        res = requests.get(url, timeout=3).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("buyRatio", 0.5)) * 100
    except Exception:
        pass
    return 50.0

def get_btc_regime_status():
    btc_klines = get_klines("BTCUSDT", "60", limit=10)
    if not btc_klines or len(btc_klines) < 3: return "NEUTRAL"
    p_change = (btc_klines[-1]['close'] - btc_klines[-2]['open']) / btc_klines[-2]['open'] * 100
    if p_change <= -1.0: return "BEARISH"
    elif p_change >= 1.0: return "BULLISH"
    return "NEUTRAL"

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
# 2. MODULE MOMENTUM SCORE (TỐI ĐA 15 ĐIỂM)
# ==============================================================================

def calculate_momentum_module_score(p_change_1h, spot_vol_ratio, fut_vol_ratio, oi_30m_pct, cvd_bullish, direction):
    score = 0

    # A. Price 1H (Max 3 pts)
    if direction == "LONG":
        if p_change_1h >= 1.00: score += 3
        elif p_change_1h >= 0.50: score += 2
        elif p_change_1h >= 0.30: score += 1
    else: # SHORT
        if p_change_1h <= -1.00: score += 3
        elif p_change_1h <= -0.50: score += 2
        elif p_change_1h <= -0.30: score += 1

    # B. Spot Volume (Max 4 pts)
    if spot_vol_ratio >= 3.0: score += 4
    elif spot_vol_ratio >= 2.0: score += 3
    elif spot_vol_ratio >= 1.5: score += 1

    # C. Futures Volume (Max 3 pts)
    if fut_vol_ratio >= 3.0: score += 3
    elif fut_vol_ratio >= 2.0: score += 2
    elif fut_vol_ratio >= 1.5: score += 1

    # D. OI 30m (Max 3 pts)
    if oi_30m_pct >= 1.00: score += 3
    elif oi_30m_pct >= 0.50: score += 2
    elif oi_30m_pct >= 0.20: score += 1

    # E. Price + Spot Vol + OI Alignment (Max 2 pts)
    align_count = 0
    if (direction == "LONG" and p_change_1h > 0) or (direction == "SHORT" and p_change_1h < 0):
        align_count += 1
    if spot_vol_ratio >= 1.5:
        align_count += 1
    if oi_30m_pct >= 0.20 and cvd_bullish == (direction == "LONG"):
        align_count += 1

    if align_count == 3: score += 2
    elif align_count == 2: score += 1

    return min(15, score)

# ==============================================================================
# 3. CORE EVALUATOR & HARD FILTER PREMIUM ENGINE
# ==============================================================================

def evaluate_setup_with_momentum(ticker_data, btc_regime):
    sym = ticker_data['symbol']
    funding = ticker_data['fundingRate']

    h1_fut = get_klines(sym, "60", category="linear", limit=35)
    h1_spot = get_klines(sym, "60", category="spot", limit=35)

    if not h1_fut or len(h1_fut) < 22: return None

    # THỨ TỰ KHAI BÁO BIẾN CHUẨN XÁC NAY ĐƯỢC ĐẶT LÊN ĐẦU
    last_h1 = h1_fut[-1]
    open_1h = last_h1['open']
    high_1h = last_h1['high']
    low_1h = last_h1['low']
    close_1h = last_h1['close']
    
    p_change_1h = (close_1h - open_1h) / open_1h * 100 if open_1h > 0 else 0.0

    # Volume Ratio Calculation
    fut_vols_20 = [c['volume'] for c in h1_fut[-21:-1]]
    avg_fut_vol = np.mean(fut_vols_20) if fut_vols_20 else 1.0
    fut_vol_ratio = last_h1['volume'] / avg_fut_vol if avg_fut_vol > 0 else 1.0

    spot_vol_ratio = 1.0
    if h1_spot and len(h1_spot) >= 20:
        spot_vols_20 = [c['volume'] for c in h1_spot[-21:-1]]
        avg_spot_vol = np.mean(spot_vols_20) if spot_vols_20 else 1.0
        spot_vol_ratio = h1_spot[-1]['volume'] / avg_spot_vol if avg_spot_vol > 0 else 1.0

    oi_30m_pct = get_open_interest_pct_30m(sym)
    taker_buy_pct = get_taker_buy_pct_5m(sym)
    cvd_bullish = taker_buy_pct >= 55.0

    # Structural Signals (Sweep / BOS / CHoCH)
    prev_swing_low = min([c['low'] for c in h1_fut[-7:-1]])
    prev_swing_high = max([c['high'] for c in h1_fut[-7:-1]])

    is_sweep_low = (low_1h < prev_swing_low) and (close_1h >= prev_swing_low * 0.998)
    is_sweep_high = (high_1h > prev_swing_high) and (close_h1 <= prev_swing_high * 1.002)

    is_choch_bos_long = (close_1h > prev_swing_high)
    is_choch_bos_short = (close_h1 < prev_swing_low)

    # Indicators & Confirmation
    ema34_h1 = calculate_ema(h1_fut, 34)
    rsi_h1_list = calculate_rsi(h1_fut, 14)
    h1_atr = calculate_atr(h1_fut, 14)

    if not ema34_h1 or not rsi_h1_list or h1_atr <= 0: return None
    rsi_h1 = rsi_h1_list[-1]

    is_h1_reclaim_long = close_h1 >= ema34_h1[-1] * 0.998 and 45.0 <= rsi_h1 <= 65.0
    is_h1_reclaim_short = close_h1 <= ema34_h1[-1] * 1.002 and 35.0 <= rsi_h1 <= 55.0

    # Determine Direction
    direction = None
    if (is_sweep_low or is_choch_bos_long or is_h1_reclaim_long) and cvd_bullish:
        direction = "LONG"
    elif (is_sweep_high or is_choch_bos_short or is_h1_reclaim_short) and not cvd_bullish:
        direction = "SHORT"

    if not direction: return None

    # Calculate Risk / Reward (RR)
    if direction == "LONG":
        sl_price = max(low_1h - (0.2 * h1_atr), close_1h * 0.96)
        tp_price = close_1h + (h1_atr * 3.2)
        risk = close_1h - sl_price
        reward = tp_price - close_1h
    else:
        sl_price = min(high_1h + (0.2 * h1_atr), close_1h * 1.04)
        tp_price = close_1h - (h1_atr * 3.2)
        risk = sl_price - close_1h
        reward = close_1h - tp_price

    rr_ratio = (reward / risk) if risk > 0 else 0.0

    # Tính Momentum Score (Max 15)
    momentum_score = calculate_momentum_module_score(
        p_change_1h, spot_vol_ratio, fut_vol_ratio, oi_30m_pct, cvd_bullish, direction
    )

    # Phân loại Momentum
    momentum_confirmed = False
    if direction == "LONG" and spot_vol_ratio >= 2.0 and fut_vol_ratio >= 2.0 and oi_30m_pct >= 0.5:
        momentum_confirmed = True
    elif direction == "SHORT" and spot_vol_ratio >= 2.0 and fut_vol_ratio >= 2.0 and oi_30m_pct >= 0.5:
        momentum_confirmed = True

    # HỆ THỐNG 100 ĐIỂM CHUẨN
    s_sweep = 15 if (is_sweep_low if direction == "LONG" else is_sweep_high) else 5
    s_structure = 20 if (is_choch_bos_long if direction == "LONG" else is_choch_bos_short) else 10
    s_vol = 10 if spot_vol_ratio >= 2.0 else (5 if spot_vol_ratio >= 1.5 else 2)
    s_cvd = 15 if (taker_buy_pct >= 62.0 if direction == "LONG" else taker_buy_pct <= 38.0) else 5
    s_oi = 10 if oi_30m_pct >= 0.50 else 3
    s_h1 = 10 if (is_h1_reclaim_long if direction == "LONG" else is_h1_reclaim_short) else 4
    s_btc = 5 if (btc_regime != "BEARISH" if direction == "LONG" else btc_regime != "BULLISH") else 0
    s_rr = 10 if rr_ratio >= 2.5 else (5 if rr_ratio >= 1.8 else 0)

    total_score = s_sweep + s_structure + s_vol + s_cvd + s_oi + s_h1 + s_btc + s_rr + momentum_score

    # CHECK HARD FILTER CHO KÈO PREMIUM
    is_premium_hard_filter = False
    if direction == "LONG":
        is_premium_hard_filter = (
            is_sweep_low and 
            is_choch_bos_long and 
            is_h1_reclaim_long and 
            cvd_bullish and 
            spot_vol_ratio >= 2.0 and 
            oi_30m_pct >= 0.50 and 
            btc_regime != "BEARISH" and 
            rr_ratio >= 2.5
        )
    else: # SHORT
        is_premium_hard_filter = (
            is_sweep_high and 
            is_choch_bos_short and 
            is_h1_reclaim_short and 
            not cvd_bullish and 
            spot_vol_ratio >= 2.0 and 
            oi_30m_pct >= 0.50 and 
            btc_regime != "BULLISH" and 
            rr_ratio >= 2.5
        )

    return {
        'symbol': sym,
        'direction': direction,
        'score': total_score,
        'momentum_score': momentum_score,
        'momentum_confirmed': momentum_confirmed,
        'is_premium': is_premium_hard_filter,
        'p_change_1h': p_change_1h,
        'spot_vol_ratio': spot_vol_ratio,
        'fut_vol_ratio': fut_vol_ratio,
        'oi_30m_pct': oi_30m_pct,
        'taker_buy_pct': taker_buy_pct,
        'entry': close_h1,
        'sl': sl_price,
        'tp': tp_price,
        'rr_ratio': rr_ratio,
        'h1_timestamp': last_h1['timestamp']
    }

# ==============================================================================
# 4. TELEGRAM ALERT FORMATTER
# ==============================================================================

def send_telegram_momentum_alert(sig):
    now_time = time.time()
    sym = sig['symbol']
    
    # Anti-spam Cooldown Logic
    if sym in processed_alerts:
        last_time, last_score = processed_alerts[sym]
        if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 6):
            return
    processed_alerts[sym] = (now_time, sig['score'])

    mom_tag = "🚀 VERY HIGH MOMENTUM" if sig['momentum_score'] >= 12 else ("🔥 HIGH MOMENTUM" if sig['momentum_score'] >= 9 else "🟢 MODERATE MOMENTUM")
    cvd_text = "BULLISH" if sig['taker_buy_pct'] >= 55.0 else "BEARISH"
    confirmed_tag = " [MOMENTUM CONFIRMED]" if sig['momentum_confirmed'] else ""

    header_title = f"🔥🔥 PREMIUM SETUP {confirmed_tag}" if sig['score'] >= 90 else f"🔥 HIGH CONFIDENCE SETUP"

    msg = f"""<b>{header_title}</b>

🪙 <b>#{sig['symbol']}</b>
⭐ <b>Total Score:</b> <code>{sig['score']}/100</code>
⚡️ <b>Momentum Score:</b> <code>{sig['momentum_score']}/15</code> (<b>{mom_tag}</b>)
🟢 <b>Direction:</b> <code>{sig['direction']}</code>

📍 <b>Entry:</b> <code>{sig['entry']:.4f}</code>
🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code>
🎯 <b>Take Profit:</b> <code>{sig['tp']:.4f}</code> (<b>RR 1:{sig['rr_ratio']:.1f}</b>)

📊 <b>FLOW & MOMENTUM METRICS:</b>
• Price 1H: <code>{sig['p_change_1h']:+.2f}%</code>
• Spot Volume: <code>{sig['spot_vol_ratio']:.2f}x</code>
• Futures Volume: <code>{sig['fut_vol_ratio']:.2f}x</code>
• OI 30m Change: <code>{sig['oi_30m_pct']:+.2f}%</code>
• CVD Direction: <code>{cvd_text} ({sig['taker_buy_pct']:.0f}%)</code>

⚠️ <i>Strict Risk Management Required. Check H1 Chart!</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT SENT] {header_title} #{sig['symbol']} Score: {sig['score']} MomScore: {sig['momentum_score']}", flush=True)

# ==============================================================================
# 5. AUTOMATIC RUNNER (QUÉT TỰ ĐỘNG MỖI 5 PHÚT)
# ==============================================================================

def run_automatic_momentum_scanner():
    print("⚡️ Bắt đầu Luồng Momentum Engine Auto Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            btc_regime = get_btc_regime_status()
            print(f"\n🔍 [AUTOMATIC SCANNER] Đang rà soát {len(all_tickers)} altcoins...", flush=True)

            # Tầng 1: Lọc nhanh Top 30 coin biến động nhất
            top30_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(evaluate_setup_with_momentum, t, btc_regime) for t in top30_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            # Lọc phân loại theo điểm Score & Quy tắc Telegram
            for sig in scanned_results:
                score = sig['score']
                mom_score = sig['momentum_score']

                if score >= 90 and sig['is_premium']:
                    send_telegram_momentum_alert(sig) # PREMIUM
                elif 85 <= score <= 89 and sig['rr_ratio'] >= 2.5:
                    send_telegram_momentum_alert(sig) # HIGH CONFIDENCE
                elif 80 <= score <= 84 and mom_score >= 9 and btc_regime != ("BEARISH" if sig['direction'] == "LONG" else "BULLISH"):
                    send_telegram_momentum_alert(sig) # GOOD SETUP

        except Exception as e:
            print(f"Lỗi Momentum Scanner Loop: {e}", flush=True)

        print(f"⏳ Hoàn tất 1 chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS}s (5 phút)...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_automatic_momentum_scanner()
