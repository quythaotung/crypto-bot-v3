import os
import time
import requests
import numpy as np
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from flask import Flask

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG DYNAMIC MULTI-SETUP SCANNER
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 5_000_000   # Quét các Altcoin có Turnover 24h >= $5M USD
TOP_TIER1_LIMIT = 60                   # Lọc Top 60 coin biến động nhất ở Tầng 1
TOP_TELEGRAM_REPORT_LIMIT = 3          # Tối đa gửi Top 1-3 tín hiệu mạnh nhất mỗi chu kỳ
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

processed_alerts = {}
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit Dynamic Multi-Setup Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=8)
    except Exception as e:
        print(f"[ERROR] Lỗi gửi Telegram: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# Safe float helper tránh sập luồng do Bybit trả về chuỗi rỗng ''
def safe_float(val, default=0.0):
    try:
        if val is None or val == '':
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

# ==============================================================================
# 2. API DATA HELPERS (BYBIT API V5)
# ==============================================================================

def get_all_bybit_crypto_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto = ['XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD',
                          'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                          'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', '3L', '3S']
            parsed_list = []
            for t in tickers:
                sym = t.get('symbol', '')
                turnover = safe_float(t.get('turnover24h', 0))
                price_change = safe_float(t.get('price24hPcnt', 0)) * 100
                funding_rate = safe_float(t.get('fundingRate', 0)) * 100
                last_price = safe_float(t.get('lastPrice', 0))

                if sym.endswith('USDT') and not sym.startswith(('USDC', 'BUSD', 'DAI')) and turnover >= MIN_FUTURES_TURNOVER_24H:
                    if not any(k in sym for k in non_crypto):
                        parsed_list.append({
                            'symbol': sym,
                            'turnover24h': turnover,
                            'price24hPcnt': price_change,
                            'fundingRate': funding_rate,
                            'lastPrice': last_price
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
                    'open': safe_float(c[1]),
                    'high': safe_float(c[2]),
                    'low': safe_float(c[3]),
                    'close': safe_float(c[4]),
                    'volume': safe_float(c[5])
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
                curr_oi = safe_float(oi_list[0].get("openInterest", 0))
                prev_oi = safe_float(oi_list[1].get("openInterest", 0))
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

def get_taker_buy_pct_5m(symbol):
    url = f"{BASE_URL}/v5/market/account-ratio?category=spot&symbol={symbol}&period=5m&limit=2"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return safe_float(res["result"]["list"][0].get("buyRatio", 0.5)) * 100
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

# ==============================================================================
# 3. ADVANCED REVERSAL & ACCUMULATION CANDLE DETECTOR
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

    # 1. Bullish Pin Bar / Hammer (Chuẩn mẫu MET/ALGO)
    if lower_wick1 >= 1.5 * body1 and cl1 >= l1 + (0.48 * range1):
        return "Bullish Pin Bar / Hammer", "LONG", wick_ratio

    # 2. Bearish Shooting Star / Pin Bar
    if upper_wick1 >= 1.5 * body1 and cl1 <= l1 + (0.52 * range1):
        return "Bearish Pin Bar", "SHORT", (upper_wick1 / body1 if body1 > 0 else 2.0)

    # 3. Bullish Engulfing
    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        return "Bullish Engulfing", "LONG", 1.5

    # 4. Bearish Engulfing
    if cl2 > o2 and cl1 < o1 and cl1 <= o2 and o1 >= cl2:
        return "Bearish Engulfing", "SHORT", 1.5

    # 5. Morning Star
    if cl3 < o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.45) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        return "Morning Star", "LONG", 1.8

    # 6. Evening Star
    if cl3 > o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.45) and cl1 < o1 and cl1 <= (o3 + cl3) / 2:
        return "Evening Star", "SHORT", 1.8

    return "None", None, 0.0

# ==============================================================================
# 4. DYNAMIC MULTI-SETUP EVALUATOR (KÈO CHẤT LƯỢNG + HARD GATE LỰC BUY)
# ==============================================================================

def evaluate_multi_setup_candidate(ticker_data):
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

        is_break_res = close_now > prev_swing_high
        is_break_supp = close_now < prev_swing_low

        pattern_name, pattern_dir, wick_ratio = detect_h1_reversal_candle(h1_candles)

        # Volume Multipliers
        vols_5m = [c['volume'] for c in m5_candles[-21:-1]]
        avg_vol_5m = np.mean(vols_5m) if vols_5m else 1.0
        vol_5m_ratio = float(last_5m['volume']) / avg_vol_5m if avg_vol_5m > 0 else 1.0

        vols_h1 = [c['volume'] for c in h1_candles[-21:-1]]
        avg_vol_h1 = np.mean(vols_h1) if vols_h1 else 1.0
        vol_h1_ratio = float(last_h1['volume']) / avg_vol_h1 if avg_vol_h1 > 0 else 1.0

        vol_max_ratio = max(vol_5m_ratio, vol_h1_ratio)

        taker_buy_pct = get_taker_buy_pct_5m(sym)
        oi_30m_pct = get_open_interest_pct_30m(sym)

        ema34 = calculate_ema(h1_candles, 34)

        # ----------------------------------------------------------------------
        # DYNAMIC SETUP DETECTOR (5 MÔ HÌNH BỨT PHÁ)
        # ----------------------------------------------------------------------
        setup_type = None
        direction = "LONG"
        score = 0

        # MÔ HÌNH 1: ACCUMULATION → EXPANSION (Kiểu METUSDT)
        if vol_max_ratio >= 2.2 and (is_break_res or pattern_dir == "LONG") and taker_buy_pct >= 55.0:
            setup_type = "🔥 ACCUMULATION → EXPANSION"
            direction = "LONG"
            score = 88

        # MÔ HÌNH 2: SHORT SQUEEZE PUMP (Kiểu OGNUSDT)
        elif funding <= -0.35 and vol_max_ratio >= 1.8 and taker_buy_pct >= 58.0 and oi_30m_pct >= 1.5:
            setup_type = "🚀 SHORT SQUEEZE PUMP"
            direction = "LONG"
            score = 90

        # MÔ HÌNH 3: LIQUIDITY SWEEP REVERSAL (Kiểu ALGO/WUSDT)
        elif (is_sweep_low or is_rejection_low) and pattern_dir == "LONG":
            setup_type = "🧲 LIQUIDITY SWEEP REVERSAL"
            direction = "LONG"
            score = 82 + (8 if vol_max_ratio >= 2.0 else 0)
        elif (is_sweep_high or is_rejection_high) and pattern_dir == "SHORT":
            setup_type = "🧲 LIQUIDITY SWEEP REVERSAL"
            direction = "SHORT"
            score = 82 + (8 if vol_max_ratio >= 2.0 else 0)

        # MÔ HÌNH 4: MOMENTUM BREAKOUT
        elif is_break_res and vol_max_ratio >= 2.0 and taker_buy_pct >= 60.0:
            setup_type = "⚡️ MOMENTUM BREAKOUT"
            direction = "LONG"
            score = 83

        # MÔ HÌNH 5: PULLBACK CONTINUATION
        elif ema34 and abs(close_now - ema34[-1]) / close_now <= 0.008 and pattern_dir == "LONG" and vol_max_ratio >= 1.5:
            setup_type = "📈 PULLBACK CONTINUATION"
            direction = "LONG"
            score = 78

        if not setup_type:
            return {
                'symbol': sym,
                'score': 0,
                'rejected_reason': "Không thuộc mô hình bứt phá",
                'is_sweep': False,
                'has_candle': False,
                'has_reclaim': False,
                'has_vol': False,
                'has_cvd': False
            }

        # ----------------------------------------------------------------------
        # HARD GATE: LỌC TRIỆT ĐỂ CÁC KÈO YẾU LỰC (NHƯ SUI)
        # ----------------------------------------------------------------------
        is_green_candle = close_h1 > open_h1
        is_strong_wick = (min(open_h1, close_h1) - low_h1) >= 0.35 * (high_h1 - low_h1)

        if direction == "LONG" and "SQUEEZE" not in setup_type:
            # 1. Bắt buộc Volume phải có sự bùng nổ (Loại nến kiệt Volume)
            if vol_max_ratio < 1.6:
                return {
                    'symbol': sym,
                    'score': 0,
                    'rejected_reason': "Volume quá yếu (< 1.6x)",
                    'is_sweep': is_sweep_low,
                    'has_candle': False,
                    'has_reclaim': False,
                    'has_vol': False,
                    'has_cvd': False
                }
            # 2. Bắt buộc H1 phải đóng xanh HOẶC có rút chân rõ (Loại nến đỏ gập xả dốc)
            if not (is_green_candle or is_strong_wick or pattern_dir == "LONG"):
                return {
                    'symbol': sym,
                    'score': 0,
                    'rejected_reason': "Nến H1 đỏ gập, chưa có lực rút chân",
                    'is_sweep': is_sweep_low,
                    'has_candle': False,
                    'has_reclaim': False,
                    'has_vol': False,
                    'has_cvd': False
                }

        if is_sweep_low or is_sweep_high: score += 5
        if oi_30m_pct >= 1.5: score += 5

        # CHẶN KÈO ĐÃ PUMP QUÁ XA (> 4.5%)
        p_change_from_low = abs(close_now - low_h1) / low_h1 * 100
        if p_change_from_low >= 4.5 and "EXPANSION" not in setup_type and "SQUEEZE" not in setup_type:
            return {
                'symbol': sym,
                'score': score,
                'rejected_reason': "Giá đã bay quá xa (>4.5%)",
                'is_sweep': is_sweep_low or is_sweep_high,
                'has_candle': True,
                'has_reclaim': True,
                'has_vol': True,
                'has_cvd': True
            }

        return {
            'symbol': sym,
            'direction': direction,
            'score': min(100, score),
            'is_eligible': True,
            'tier_title': setup_type,
            'pattern': pattern_name if pattern_name != "None" else "Bứt phá dòng tiền H1",
            'is_sweep': is_sweep_low or is_sweep_high,
            'has_reclaim': close_h1 >= prev_swing_low if direction == "LONG" else close_h1 <= prev_swing_high,
            'vol_max_ratio': vol_max_ratio,
            'oi_30m_pct': oi_30m_pct,
            'taker_buy_pct': taker_buy_pct,
            'funding': funding,
            'entry': close_now
        }
    except Exception:
        return None

# ==============================================================================
# 5. TELEGRAM ALERT FORMATTER & AUTOMATIC LOOP
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

    msg = f"""<b>{sig['tier_title']}</b>

#<b>{sig['symbol']}</b>
⭐ Score: <b>{sig['score']}/100</b>
🟢 Direction: <b>{sig['direction']}</b>

🕯 Pattern: <b>{sig['pattern']}</b>
🧲 Liquidity Sweep: {sweep_icon}
📍 Support Reclaim: {reclaim_icon}
📊 Volume Expansion: <code>{sig['vol_max_ratio']:.1f}x</code>
💰 CVD Buy Pressure: <code>{sig['taker_buy_pct']:.0f}%</code>
📈 OI 30m Change: <code>{sig['oi_30m_pct']:+.1f}%</code>
💸 Funding Rate: <code>{sig['funding']:.3f}%</code>

⚡ <b>EARLY ENTRY — CHECK CHART NGAY!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT SENT] {sig['tier_title']} #{sig['symbol']} Score: {sig['score']}", flush=True)

def run_automatic_adaptive_scanner():
    print("⚡️ Bắt đầu Bybit Dynamic Multi-Setup Engine Active...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"\n🔍 [SCANNER] Đang rà soát mô hình bứt phá cho {len(all_tickers)} Altcoins...", flush=True)

            top_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(evaluate_multi_setup_candidate, t) for t in top_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            scanned_results.sort(key=lambda x: x['score'], reverse=True)

            print("\n==================================================", flush=True)
            print("📋 TOP MULTI-SETUP CANDIDATES (DEBUG LOG)", flush=True)
            print("==================================================", flush=True)
            
            top10 = scanned_results[:10]
            if top10 and top10[0]['score'] > 0:
                for idx, c in enumerate(top10, 1):
                    if c['score'] == 0: continue
                    print(f"{idx}. {c['symbol']} | Score: {c['score']} | Setup: {c.get('tier_title')}", flush=True)
            else:
                print("🔎 Chưa có coin nào đạt chuẩn trong các mô hình bứt phá.", flush=True)
            
            print("==================================================\n", flush=True)

            eligible_signals = [c for c in scanned_results if c.get('is_eligible', False)]

            if eligible_signals:
                for sig in eligible_signals[:TOP_TELEGRAM_REPORT_LIMIT]:
                    send_telegram_reversal_alert(sig)
            else:
                print("🔎 Không có tín hiệu mô hình đủ mạnh trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"Lỗi Adaptive Scanner Loop: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_automatic_adaptive_scanner()
