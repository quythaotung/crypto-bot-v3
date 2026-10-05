import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - AUTOMATIC 2-TIER REVERSAL SCANNER ENGINE
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Tự động scan mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Lọc Altcoin thanh khoản >= $8M USD
TOP_TIER1_LIMIT = 25                   # Tầng 1 lọc chọn Top 25 coin biến động nhất
TOP_TELEGRAM_REPORT_LIMIT = 5          # Tầng 2 chọn Top 5 coin đẹp nhất gửi Telegram
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_alerts = {}                  # Lưu trạng thái cooldown & score gần nhất
confirmed_h1_set = set()               # Đánh dấu các nến H1 đã gửi CONFIRMED

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Automatic 2-Tier Reversal Scanner Active 24/7!"

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
# API HELPERS (PURE CRYPTO ONLY)
# ==============================================================================

def get_all_bybit_crypto_symbols():
    """Lấy danh sách Altcoin USDT Perp thuần Crypto"""
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
                price_change = abs(float(t.get('price24hPcnt', 0)) * 100)
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
    """Lấy Klines bao gồm cả nến đang chạy real-time"""
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
# CANDLESTICK PATTERN RECOGNITION ENGINE (CÁC MÔ HÌNH NẾN H1)
# ==============================================================================

def detect_h1_candle_pattern(h1_candles):
    if len(h1_candles) < 3:
        return "Unknown", "NONE"

    c1 = h1_candles[-1] # Nến hiện tại (in-progress hoặc vừa đóng)
    c2 = h1_candles[-2] # Nến trước
    c3 = h1_candles[-3]

    o1, h1, l1, cl1 = c1['open'], c1['high'], c1['low'], c1['close']
    o2, h2, l2, cl2 = c2['open'], c2['high'], c2['low'], c2['close']
    o3, h3, l3, cl3 = c3['open'], c3['high'], c3['low'], c3['close']

    body1 = abs(cl1 - o1)
    range1 = h1 - l1 if (h1 - l1) > 0 else 1.0
    lower_wick1 = min(o1, cl1) - l1
    upper_wick1 = h1 - max(o1, cl1)

    body2 = abs(cl2 - o2)

    # 1. Bullish Pin Bar / Hammer
    if lower_wick1 >= 2.0 * body1 and cl1 >= l1 + (0.6 * range1):
        return "Bullish Pin Bar", "LONG"

    # 2. Bearish Shooting Star / Pin Bar
    if upper_wick1 >= 2.0 * body1 and cl1 <= l1 + (0.4 * range1):
        return "Shooting Star", "SHORT"

    # 3. Bullish Engulfing
    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        return "Bullish Engulfing", "LONG"

    # 4. Bearish Engulfing
    if cl2 > o2 and cl1 < o1 and cl1 <= o2 and o1 >= cl2:
        return "Bearish Engulfing", "SHORT"

    # 5. Morning Star
    if cl3 < o3 and body2 < (abs(cl3 - o3) * 0.4) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        return "Morning Star", "LONG"

    # 6. Evening Star
    if cl3 > o3 and body2 < (abs(cl3 - o3) * 0.4) and cl1 < o1 and cl1 <= (o3 + cl3) / 2:
        return "Evening Star", "SHORT"

    # 7. Tweezer Bottom
    if abs(l1 - l2) / cl1 <= 0.0015 and cl1 > o1:
        return "Tweezer Bottom", "LONG"

    # 8. Tweezer Top
    if abs(h1 - h2) / cl1 <= 0.0015 and cl1 < o1:
        return "Tweezer Top", "SHORT"

    # 9. Doji Rejection
    if body1 <= range1 * 0.15:
        if lower_wick1 > upper_wick1:
            return "Doji Bullish Rejection", "LONG"
        elif upper_wick1 > lower_wick1:
            return "Doji Bearish Rejection", "SHORT"

    return "Standard Reversal", "NEUTRAL"

# ==============================================================================
# TẦNG 1 — FAST SCANNER (LỌC NHANH TOÀN BỘ THỊ TRƯỜNG CHỌN TOP 25 COIN)
# ==============================================================================

def execute_tier1_fast_scan(all_tickers):
    candidates = []
    for t in all_tickers:
        sym = t['symbol']
        try:
            m5_candles = get_klines_with_current(sym, "5", limit=15)
            if not m5_candles or len(m5_candles) < 10: continue

            last_5m = m5_candles[-1]
            p_change_5m = abs((last_5m['close'] - last_5m['open']) / last_5m['open'] * 100)

            vols_5m = [c['volume'] for c in m5_candles[-11:-1]]
            avg_vol_5m = np.mean(vols_5m) if vols_5m else 1.0
            vol_expansion_5m = last_5m['volume'] / avg_vol_5m if avg_vol_5m > 0 else 1.0

            # Điểm hấp dẫn Tầng 1 (Volume Expansion 5M + Biến động giá + Turnover)
            tier1_score = (vol_expansion_5m * 30) + (p_change_5m * 25) + (t['price24hPcnt'] * 20)
            
            if vol_expansion_5m >= 1.3 or p_change_5m >= 0.5:
                candidates.append({
                    'ticker': t,
                    'tier1_score': tier1_score,
                    'vol_expansion_5m': vol_expansion_5m,
                    'p_change_5m': p_change_5m
                })
        except Exception:
            pass

    candidates.sort(key=lambda x: x['tier1_score'], reverse=True)
    return [c['ticker'] for c in candidates[:TOP_TIER1_LIMIT]]

# ==============================================================================
# TẦNG 2 — DEEP REVERSAL ENGINE (CHẤM ĐIỂM 100 & NHẬN DIỆN MÔ HÌNH NẾN H1)
# ==============================================================================

def analyze_tier2_deep_reversal(ticker_data):
    sym = ticker_data['symbol']
    funding = ticker_data['fundingRate']

    m5_candles = get_klines_with_current(sym, "5", limit=30)
    h1_candles = get_klines_with_current(sym, "60", limit=35)

    if not m5_candles or len(m5_candles) < 22 or not h1_candles or len(h1_candles) < 22:
        return None

    last_5m = m5_candles[-1]
    curr_price = last_5m['close']

    # Volume 5M
    vols_5m_20 = [c['volume'] for c in m5_candles[-21:-1]]
    avg_vol_5m = np.mean(vols_5m_20) if vols_5m_20 else 1.0
    vol_5m_ratio = last_5m['volume'] / avg_vol_5m if avg_vol_5m > 0 else 1.0

    # Kline H1
    curr_h1 = h1_candles[-1]
    prev_h1_swing_low = min([c['low'] for c in h1_candles[-7:-1]])
    prev_h1_swing_high = max([c['high'] for c in h1_candles[-7:-1]])

    open_h1, high_h1, low_h1, close_h1 = curr_h1['open'], curr_h1['high'], curr_h1['low'], curr_h1['close']
    body_h1 = abs(close_h1 - open_h1)
    lower_wick_h1 = min(open_h1, close_h1) - low_h1
    upper_wick_h1 = high_h1 - max(open_h1, close_h1)

    # Technical Indicators
    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)
    rsi_h1_list = calculate_rsi(h1_candles, 14)

    if not ema34_h1 or not rsi_h1_list: return None
    rsi_h1 = rsi_h1_list[-1]

    # Orderflow Data
    oi_pct_5m = get_open_interest_pct(sym, "5m")
    taker_buy_pct = get_taker_buy_sell_ratio(sym, "5m")

    # Anti-Overextended Filter (Loại bỏ coin đã pump/dump quá xa > 8%)
    p_change_3h = (close_h1 - h1_candles[-4]['open']) / h1_candles[-4]['open'] * 100 if len(h1_candles) >= 4 else 0
    if p_change_3h > 8.5 or p_change_3h < -8.5:
        return None

    # Nhận diện mô hình nến H1
    pattern_name, pattern_dir = detect_h1_candle_pattern(h1_candles)

    # --------------------------------------------------------------------------
    # CHECK LONG REVERSAL
    # --------------------------------------------------------------------------
    is_long_sweep = (low_h1 < prev_h1_swing_low) and (close_h1 >= prev_h1_swing_low * 0.998)
    wick_ratio_long = (lower_wick_h1 / body_h1) if body_h1 > 0 else 0.0
    is_support_reclaim = close_h1 >= prev_h1_swing_low

    if is_long_sweep or (wick_ratio_long >= 1.4 and close_h1 >= open_h1) or pattern_dir == "LONG":
        s_sweep = 25 if is_long_sweep else 10
        s_wick = 15 if wick_ratio_long >= 2.0 else (10 if wick_ratio_long >= 1.2 else 5)
        s_reclaim = 15 if is_support_reclaim else 8
        s_vol = 10 if vol_5m_ratio >= 2.0 else (6 if vol_5m_ratio >= 1.4 else 2)
        s_oi = 10 if oi_pct_5m >= 1.0 else 5
        s_cvd = 10 if taker_buy_pct >= 60.0 else 4
        s_ema = 5 if (abs(close_h1 - ema34_h1[-1])/close_h1 < 0.012 or (ema89_h1 and abs(close_h1 - ema89_h1[-1])/close_h1 < 0.012)) else 2
        s_rsi = 5 if 36.0 <= rsi_h1 <= 58.0 else 2
        s_btc = 5 if funding >= 0 else 3

        score_long = s_sweep + s_wick + s_reclaim + s_vol + s_oi + s_cvd + s_ema + s_rsi + s_btc

        if score_long >= 65:
            return {
                'symbol': sym,
                'direction': 'LONG',
                'score': score_long,
                'entry': curr_price,
                'pattern': pattern_name if pattern_dir == "LONG" else "Bullish Reclaim",
                'is_sweep': is_long_sweep,
                'is_reclaim': is_support_reclaim,
                'wick_ratio': wick_ratio_long,
                'vol_5m_ratio': vol_5m_ratio,
                'oi_pct': oi_pct_5m,
                'taker_buy': taker_buy_pct,
                'rsi_h1': rsi_h1,
                'funding': funding,
                'h1_timestamp': curr_h1['timestamp']
            }

    # --------------------------------------------------------------------------
    # CHECK SHORT REVERSAL
    # --------------------------------------------------------------------------
    is_short_sweep = (high_h1 > prev_h1_swing_high) and (close_h1 <= prev_h1_swing_high * 1.002)
    wick_ratio_short = (upper_wick_h1 / body_h1) if body_h1 > 0 else 0.0
    is_resistance_reject = close_h1 <= prev_h1_swing_high

    if is_short_sweep or (wick_ratio_short >= 1.4 and close_h1 <= open_h1) or pattern_dir == "SHORT":
        s_sweep = 25 if is_short_sweep else 10
        s_wick = 15 if wick_ratio_short >= 2.0 else (10 if wick_ratio_short >= 1.2 else 5)
        s_reclaim = 15 if is_resistance_reject else 8
        s_vol = 10 if vol_5m_ratio >= 2.0 else (6 if vol_5m_ratio >= 1.4 else 2)
        s_oi = 10 if oi_pct_5m >= 1.0 else 5
        s_cvd = 10 if taker_buy_pct <= 40.0 else 4
        s_ema = 5 if (abs(close_h1 - ema34_h1[-1])/close_h1 < 0.012 or (ema89_h1 and abs(close_h1 - ema89_h1[-1])/close_h1 < 0.012)) else 2
        s_rsi = 5 if 42.0 <= rsi_h1 <= 64.0 else 2
        s_btc = 5 if funding < 0 else 3

        score_short = s_sweep + s_wick + s_reclaim + s_vol + s_oi + s_cvd + s_ema + s_rsi + s_btc

        if score_short >= 65:
            return {
                'symbol': sym,
                'direction': 'SHORT',
                'score': score_short,
                'entry': curr_price,
                'pattern': pattern_name if pattern_dir == "SHORT" else "Bearish Rejection",
                'is_sweep': is_short_sweep,
                'is_reclaim': is_resistance_reject,
                'wick_ratio': wick_ratio_short,
                'vol_5m_ratio': vol_5m_ratio,
                'oi_pct': oi_pct_5m,
                'taker_buy': taker_buy_pct,
                'rsi_h1': rsi_h1,
                'funding': funding,
                'h1_timestamp': curr_h1['timestamp']
            }

    return None

# ==============================================================================
# TELEGRAM AUTOMATIC ALERT FORMATTER
# ==============================================================================

def send_telegram_reversal_alert(sig, is_confirmed=False):
    now_time = time.time()
    sym = sig['symbol']
    
    # Anti-spam Cooldown Logic
    if not is_confirmed:
        if sym in processed_alerts:
            last_time, last_score = processed_alerts[sym]
            if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 8):
                return
        processed_alerts[sym] = (now_time, sig['score'])

    sweep_icon = "✅" if sig['is_sweep'] else "❌"
    reclaim_icon = "✅" if sig['is_reclaim'] else "❌"
    cvd_text = "BUY PRESSURE" if sig['taker_buy'] >= 58 else ("SELL PRESSURE" if sig['taker_buy'] <= 42 else "NEUTRAL")

    header_title = f"✅ H1 CONFIRMED REVERSAL — {sig['direction']}" if is_confirmed else f"🔥 EARLY H1 REVERSAL"

    msg = f"""<b>{header_title}</b>

🪙 <b>#{sig['symbol']}</b>
⭐ <b>Score:</b> <code>{sig['score']}/100</code>
🟢 <b>Direction:</b> <code>{sig['direction']}</code>

🕯 <b>Pattern:</b> <code>{sig['pattern']}</code>
🧲 <b>Liquidity Sweep:</b> {sweep_icon}
📍 <b>Support/Resistance Reclaim:</b> {reclaim_icon}
📊 <b>Volume 5M:</b> <code>{sig['vol_5m_ratio']:.1f}x</code>
📈 <b>OI:</b> <code>{sig['oi_pct']:+.1f}%</code>
💰 <b>CVD:</b> <code>{cvd_text}</code>
📐 <b>EMA34:</b> NEAR / RECLAIM
📊 <b>RSI H1:</b> <code>{sig['rsi_h1']:.1f} (Recovery)</code>

⚡️️ <b>EARLY — Check H1 ngay</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [AUTOMATIC ALERT SENT] {header_title} #{sig['symbol']} Score: {sig['score']}", flush=True)

# ==============================================================================
# MAIN AUTOMATIC SCAN ENGINE (2 TẦNG CHẠY LIÊN TỤC MỖI 5 PHÚT)
# ==============================================================================

def run_automatic_2tier_scanner():
    print("⚡️ Bắt đầu Luồng Auto Scanner 2 Tầng Chạy Liên Tục Mỗi 5 Phút...", flush=True)
    while True:
        try:
            start_time = time.time()
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"\n🔍 [AUTOMATIC SCANNER] Bắt đầu quét {len(all_tickers)} altcoins...", flush=True)

            # BƯỚC 1: Tầng 1 — Fast Scanner (Lọc Top 25 coin biến động nhất)
            top25_candidates = execute_tier1_fast_scan(all_tickers)
            print(f"🎯 [TẦNG 1] Lọc thành công Top {len(top25_candidates)} coin biến động nhất.", flush=True)

            # BƯỚC 2: Tầng 2 — Deep Reversal Engine (Chạy đa luồng phân tích sâu Top 25)
            tier2_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(analyze_tier2_deep_reversal, t) for t in top25_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        tier2_results.append(res)

            # BƯỚC 3: Tự Động Xếp Hạng & Chọn Top 5 Tốt Nhất
            tier2_results.sort(key=lambda x: x['score'], reverse=True)
            top5_final = tier2_results[:TOP_TELEGRAM_REPORT_LIMIT]

            elapsed = time.time() - start_time
            print(f"✅ [TẦNG 2] Hoàn tất phân tích sâu trong {elapsed:.1f}s. Tìm thấy {len(top5_final)} setup đạt yêu cầu.", flush=True)

            # BƯỚC 4: Tự Động Bắn Telegram (Không cần thao tác thủ công)
            if top5_final:
                for sig in top5_final:
                    send_telegram_reversal_alert(sig, is_confirmed=False)
            else:
                print("🔎 Không có setup reversal đáng chú ý trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"Lỗi Radar Loop: {e}", flush=True)

        print(f"⏳ Hoàn tất 1 chu kỳ 5M. Tạm nghỉ {SCAN_INTERVAL_SECONDS}s...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_automatic_2tier_scanner()
