import os
import time
import json
import requests
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG & ĐIỀU KIỆN LỌC (CONFIGURATIONS)
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Lọc Altcoin thanh khoản 24h >= $8M USD
TOP_TIER1_LIMIT = 50                   # Lọc Top 50 coin biến động nhất ở Tầng 1
TOP_TELEGRAM_REPORT_LIMIT = 3          # Tối đa gửi Top 1-3 tín hiệu mạnh nhất mỗi chu kỳ
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_ALERT_SCORE = 75                   # Ngưỡng Score tối thiểu để duyệt gửi Alert

processed_alerts = {}                  # Lưu cooldown & score gần nhất
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Futures High-Probability & Short Squeeze Scanner Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("[WARN] Chưa cấu hình BOT_TOKEN hoặc CHAT_ID!", flush=True)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=8)
    except Exception as e:
        print(f"[ERROR] Lỗi gửi Telegram: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 2. API DATA HELPERS (BYBIT API V5)
# ==============================================================================

def get_all_bybit_crypto_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
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
                funding_rate = float(t.get('fundingRate', 0)) * 100
                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and turnover >= MIN_FUTURES_TURNOVER_24H:
                    if not any(k in sym for k in non_crypto_keywords):
                        parsed_list.append({
                            'symbol': sym,
                            'turnover24h': turnover,
                            'price24hPcnt': price_change,
                            'fundingRate': funding_rate,
                            'lastPrice': float(t.get('lastPrice', 0))
                        })
            return parsed_list
    except Exception as e:
        print(f"Lỗi lấy Tickers từ Bybit: {e}", flush=True)
    return []

def get_klines(symbol, interval, category="linear", limit=35):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=5).json()
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
        res = requests.get(url, timeout=4).json()
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
        res = requests.get(url, timeout=4).json()
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
# 3. PATTERN DETECTOR
# ==============================================================================

def detect_h1_reversal_candle(h1_candles):
    if len(h1_candles) < 3: return "None", None, 0.0

    c1 = h1_candles[-1]
    c2 = h1_candles[-2]
    c3 = h1_candles[-3]

    o1, h1, l1, cl1 = c1['open'], c1['high'], c1['low'], c1['close']
    o2, h2, l2, cl2 = c2['open'], c2['high'], c2['low'], c2['close']
    o3, h3, l3, cl3 = c3['open'], c3['high'], c3['low'], c3['close']

    body1 = abs(cl1 - o1)
    range1 = (h1 - l1) if (h1 - l1) > 0 else 1.0
    lower_wick1 = min(o1, cl1) - l1
    upper_wick1 = h1 - max(o1, cl1)

    wick_ratio = (lower_wick1 / body1) if body1 > 0 else (lower_wick1 / range1 * 3)

    if lower_wick1 >= 1.6 * body1 and cl1 >= l1 + (0.50 * range1):
        return "Bullish Pin Bar", "LONG", wick_ratio

    if upper_wick1 >= 1.6 * body1 and cl1 <= l1 + (0.50 * range1):
        return "Bearish Pin Bar", "SHORT", (upper_wick1 / body1 if body1 > 0 else 2.0)

    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        return "Bullish Engulfing", "LONG", 1.5

    if cl2 > o2 and cl1 < o1 and cl1 <= o2 and o1 >= cl2:
        return "Bearish Engulfing", "SHORT", 1.5

    if cl3 < o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.45) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        return "Morning Star", "LONG", 1.8

    if cl3 > o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.45) and cl1 < o1 and cl1 <= (o3 + cl3) / 2:
        return "Evening Star", "SHORT", 1.8

    return "None", None, 0.0

# ==============================================================================
# 4. MULTI-SETUP EVALUATOR (REVERSAL & SHORT SQUEEZE PUMP ENGINE)
# ==============================================================================

def evaluate_adaptive_reversal_candidate(ticker_data):
    try:
        sym = ticker_data['symbol']
        funding = ticker_data['fundingRate']

        h1_candles = get_klines(sym, "60", category="linear", limit=35)
        m5_candles = get_klines(sym, "5", category="linear", limit=25)

        if not h1_candles or len(h1_candles) < 22 or not m5_candles or len(m5_candles) < 15:
            return None

        last_h1 = h1_candles[-1]
        open_h1, high_h1, low_h1, close_h1 = float(last_h1['open']), float(last_h1['high']), float(last_h1['low']), float(last_h1['close'])
        
        last_5m = m5_candles[-1]
        close_now = float(last_5m['close'])

        prev_swing_low = min([c['low'] for c in h1_candles[-8:-1]])
        prev_swing_high = max([c['high'] for c in h1_candles[-8:-1]])

        is_sweep_low = (low_h1 < prev_swing_low) and (close_h1 >= prev_swing_low * 0.998)
        is_sweep_high = (high_h1 > prev_swing_high) and (close_h1 <= prev_swing_high * 1.002)
        
        is_rejection_low = (low_h1 <= prev_swing_low * 1.003) and (close_h1 > low_h1 + (high_h1 - low_h1) * 0.4)
        is_rejection_high = (high_h1 >= prev_swing_high * 0.997) and (close_h1 < high_h1 - (high_h1 - low_h1) * 0.4)

        pattern_name, pattern_dir, wick_ratio = detect_h1_reversal_candle(h1_candles)

        # Volume Expansion
        vols_5m = [c['volume'] for c in m5_candles[-21:-1]]
        avg_vol_5m = np.mean(vols_5m) if vols_5m else 1.0
        vol_5m_ratio = float(last_5m['volume']) / avg_vol_5m if avg_vol_5m > 0 else 1.0

        vols_h1 = [c['volume'] for c in h1_candles[-21:-1]]
        avg_vol_h1 = np.mean(vols_h1) if vols_h1 else 1.0
        vol_h1_ratio = float(last_h1['volume']) / avg_vol_h1 if avg_vol_h1 > 0 else 1.0

        vol_max_ratio = max(vol_5m_ratio, vol_h1_ratio)
        has_vol = vol_max_ratio >= 1.5

        # Orderflow (CVD & OI)
        taker_buy_pct = get_taker_buy_pct_5m(sym)
        oi_30m_pct = get_open_interest_pct_30m(sym)

        # ----------------------------------------------------------------------
        # MODULE A: SPECIAL SHORT SQUEEZE / ACCUMULATION PUMP DETECTOR
        # (Chuyên bắt sớm những con như OGN / WUSDT trước khi dựng cột)
        # ----------------------------------------------------------------------
        is_short_squeeze_setup = False
        if funding <= -0.35 and vol_max_ratio >= 2.0 and taker_buy_pct >= 58.0 and oi_30m_pct >= 2.0:
            is_short_squeeze_setup = True

        # PA Alignment Check
        has_long_pa = (is_sweep_low or is_rejection_low) and (pattern_dir == "LONG")
        has_short_pa = (is_sweep_high or is_rejection_high) and (pattern_dir == "SHORT")

        if not (has_long_pa or has_short_pa or is_short_squeeze_setup):
            return {
                'symbol': sym,
                'score': 0,
                'rejected_reason': "No Price Action or Squeeze Setup",
                'is_sweep': False,
                'has_candle': False,
                'has_reclaim': False,
                'has_vol': False,
                'has_cvd': False
            }

        direction = "LONG" if (has_long_pa or is_short_squeeze_setup) else "SHORT"
        is_reclaim = (close_h1 >= prev_swing_low) if direction == "LONG" else (close_h1 <= prev_swing_high)
        has_cvd = (taker_buy_pct >= 54.0) if direction == "LONG" else (taker_buy_pct <= 46.0)

        # ----------------------------------------------------------------------
        # ADAPTIVE SCORING (MAX 100 PTS)
        # ----------------------------------------------------------------------
        score = 0
        is_sweep = is_sweep_low if direction == "LONG" else is_sweep_high

        if is_sweep: score += 25
        elif (is_rejection_low if direction == "LONG" else is_rejection_high): score += 18

        if pattern_dir: score += 20
        if is_reclaim: score += 15

        if wick_ratio >= 2.0: score += 10
        elif wick_ratio >= 1.3: score += 5

        if vol_max_ratio >= 2.5: score += 10
        elif vol_max_ratio >= 1.5: score += 6

        if (direction == "LONG" and taker_buy_pct >= 60.0) or (direction == "SHORT" and taker_buy_pct <= 40.0): score += 10
        elif has_cvd: score += 5

        if oi_30m_pct >= 3.0: score += 10
        elif oi_30m_pct >= 1.0: score += 5

        # Thưởng điểm cho Short Squeeze
        if is_short_squeeze_setup:
            score = max(score, 88)
            pattern_name = f"SHORT SQUEEZE PUMP (Funding: {funding:.2f}%)"

        # OVEREXTENDED CHECK (> 4.5% từ điểm Reversal)
        p_change_from_sweep = abs(close_now - (low_h1 if direction == "LONG" else high_h1)) / (low_h1 if direction == "LONG" else high_h1) * 100
        if p_change_from_sweep >= 4.5 and not is_short_squeeze_setup:
            return {
                'symbol': sym,
                'score': score,
                'rejected_reason': "Pumped/Dumped too far (>4.5% from reversal)",
                'is_sweep': is_sweep,
                'has_candle': True,
                'has_reclaim': is_reclaim,
                'has_vol': has_vol,
                'has_cvd': has_cvd
            }

        is_eligible = False
        rejected_reason = ""

        if score >= 80 or is_short_squeeze_setup:
            is_eligible = True
        elif 75 <= score <= 79:
            if is_sweep and pattern_dir:
                is_eligible = True
            else:
                rejected_reason = "Score 75-79 missing Liquidity Sweep"
        else:
            rejected_reason = f"Score < 75 (Score = {score})"

        tier_title = "🚀 EXTREME SHORT SQUEEZE" if is_short_squeeze_setup else ("🔥 VERY STRONG REVERSAL" if score >= 85 else "🚨 STRONG REVERSAL")

        return {
            'symbol': sym,
            'direction': direction,
            'score': score,
            'is_eligible': is_eligible,
            'rejected_reason': rejected_reason,
            'tier_title': tier_title,
            'pattern': pattern_name if pattern_name else "Price Action Alignment",
            'is_sweep': is_sweep,
            'has_candle': True if pattern_dir else False,
            'has_reclaim': is_reclaim,
            'has_vol': has_vol,
            'has_cvd': has_cvd,
            'vol_max_ratio': vol_max_ratio,
            'oi_30m_pct': oi_30m_pct,
            'taker_buy_pct': taker_buy_pct,
            'funding': funding,
            'entry': close_now
        }
    except Exception:
        return None

# ==============================================================================
# 5. TELEGRAM ALERT FORMATTER
# ==============================================================================

def send_telegram_reversal_alert(sig):
    now_time = time.time()
    sym = sig['symbol']
    
    if sym in processed_alerts:
        last_time, last_score = processed_alerts[sym]
        if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 5):
            return
    processed_alerts[sym] = (now_time, sig['score'])

    sweep_icon = "✅" if sig['is_sweep'] else "❌"
    reclaim_icon = "✅" if sig['has_reclaim'] else "❌"
    cvd_icon = "Buy ✅" if sig['direction'] == "LONG" and sig['has_cvd'] else ("Sell ✅" if sig['has_cvd'] else "Neutral")

    msg = f"""🚨 <b>{sig['tier_title']}</b>

#<b>{sig['symbol']}</b>
Score: <b>{sig['score']}/100</b>
Direction: <b>{sig['direction']}</b>

🕯 <b>{sig['pattern']}</b>
🧲 <b>Liquidity Sweep:</b> {sweep_icon}
📍 <b>Support Reclaim:</b> {reclaim_icon}
📊 <b>Volume:</b> <code>{sig['vol_max_ratio']:.1f}x</code>
💰 <b>CVD:</b> <code>{cvd_icon} ({sig['taker_buy_pct']:.0f}%)</code>
📈 <b>OI 30m:</b> <code>{sig['oi_30m_pct']:+.1f}%</code>
💸 <b>Funding Rate:</b> <code>{sig['funding']:.3f}%</code>

⚡ <b>EARLY ENTRY — CHECK CHART NGAY!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT SENT] {sig['tier_title']} #{sig['symbol']} Score: {sig['score']}", flush=True)

# ==============================================================================
# 6. AUTOMATIC SCANNER LOOP WITH DEBUG LOGGING
# ==============================================================================

def run_automatic_adaptive_scanner():
    print("⚡️ Bắt đầu Bybit Futures Scanner Engine Active...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"\n🔍 [SCANNER] Đang rà soát {len(all_tickers)} Altcoins đạt chuẩn Turnover >= 8M USD...", flush=True)

            top_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(evaluate_adaptive_reversal_candidate, t) for t in top_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            scanned_results.sort(key=lambda x: x['score'], reverse=True)

            # PRINT DEBUG LOGS TOP 10 CANDIDATES TRÊN RENDER
            print("\n==================================================", flush=True)
            print("📋 TOP CANDIDATES (DEBUG LOG)", flush=True)
            print("==================================================", flush=True)
            
            top10_candidates = scanned_results[:10]
            if top10_candidates and top10_candidates[0]['score'] > 0:
                for idx, c in enumerate(top10_candidates, 1):
                    if c['score'] == 0: continue
                    sweep_str = "YES" if c['is_sweep'] else "NO"
                    candle_str = "YES" if c['has_candle'] else "NO"
                    reclaim_str = "YES" if c['has_reclaim'] else "NO"
                    vol_str = "YES" if c['has_vol'] else "NO"
                    cvd_str = "YES" if c['has_cvd'] else "NO"
                    
                    print(f"{idx}. {c['symbol']} Score: {c['score']}", flush=True)
                    print(f"   Sweep: {sweep_str} | Candle: {candle_str} | Reclaim: {reclaim_str} | Volume: {vol_str} | CVD: {cvd_str}", flush=True)
                    if not c.get('is_eligible'):
                        print(f"   ❌ Rejected reason: {c.get('rejected_reason')}", flush=True)
            else:
                print("🔎 Không có coin nào đạt cấu trúc Reversal hoặc Short Squeeze.", flush=True)
            
            print("==================================================\n", flush=True)

            # GỬI TELEGRAM TOP 1-3 TÍN HIỆU ĐỦ ĐIỀU KIỆN
            eligible_signals = [c for c in scanned_results if c.get('is_eligible', False)]

            if eligible_signals:
                for sig in eligible_signals[:TOP_TELEGRAM_REPORT_LIMIT]:
                    send_telegram_reversal_alert(sig)
            else:
                highest_score = scanned_results[0]['score'] if scanned_results else 0
                print(f"🔎 Không có tín hiệu reversal đủ mạnh để gửi Telegram. (Highest score = {highest_score})", flush=True)

        except Exception as e:
            print(f"Lỗi Adaptive Scanner Loop: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_automatic_adaptive_scanner()
