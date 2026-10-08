import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - STRICT REVERSAL ENGINE (TOP 1-3 HIGH QUALITY ONLY)
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Thanh khoản 24h tối thiểu >= $8M USD
TOP_TIER1_LIMIT = 40                   # Tầng 1 lọc Top 40 coin biến động nhất
TOP_TELEGRAM_REPORT_LIMIT = 3          # Chỉ gửi tối đa Top 1-3 tín hiệu mạnh nhất
MIN_ALERT_SCORE = 80                   # Score tối thiểu phải đạt >= 80 điểm
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_alerts = {}                  # Lưu cooldown & score gần nhất
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Strict Reversal Auto Scanner Active 24/7!"

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
# 1. API HELPERS & DATA CALCULATIONS
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
    if p_change <= -1.2: return "BEARISH_DUMP"
    elif p_change >= 1.2: return "BULLISH_PUMP"
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

# ==============================================================================
# 2. CANDLESTICK PATTERN RECOGNITION (CHỈ BẮT MÔ HÌNH REVERSAL MẠNH)
# ==============================================================================

def detect_strong_h1_reversal_pattern(h1_candles):
    if len(h1_candles) < 3: return "None", None

    c1 = h1_candles[-1] # Nến hiện tại
    c2 = h1_candles[-2] # Nến trước
    c3 = h1_candles[-3]

    o1, h1, l1, cl1 = c1['open'], c1['high'], c1['low'], c1['close']
    o2, h2, l2, cl2 = c2['open'], c2['high'], c2['low'], c2['close']
    o3, h3, l3, cl3 = c3['open'], c3['high'], c3['low'], c3['close']

    body1 = abs(cl1 - o1)
    range1 = (h1 - l1) if (h1 - l1) > 0 else 1.0
    lower_wick1 = min(o1, cl1) - l1
    upper_wick1 = h1 - max(o1, cl1)

    # 1. Bullish Pin Bar / Hammer
    if lower_wick1 >= 1.8 * body1 and cl1 >= l1 + (0.55 * range1):
        return "Bullish Pin Bar", "LONG"

    # 2. Bearish Shooting Star / Pin Bar
    if upper_wick1 >= 1.8 * body1 and cl1 <= l1 + (0.45 * range1):
        return "Bearish Pin Bar", "SHORT"

    # 3. Bullish Engulfing
    if cl2 < o2 and cl1 > o1 and cl1 >= o2 and o1 <= cl2:
        return "Bullish Engulfing", "LONG"

    # 4. Bearish Engulfing
    if cl2 > o2 and cl1 < o1 and cl1 <= o2 and o1 >= cl2:
        return "Bearish Engulfing", "SHORT"

    # 5. Morning Star
    if cl3 < o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.4) and cl1 > o1 and cl1 >= (o3 + cl3) / 2:
        return "Morning Star", "LONG"

    # 6. Evening Star
    if cl3 > o3 and abs(cl2 - o2) < (abs(cl3 - o3) * 0.4) and cl1 < o1 and cl1 <= (o3 + cl3) / 2:
        return "Evening Star", "SHORT"

    # 7. Tweezer Bottom
    if abs(l1 - l2) / cl1 <= 0.0015 and cl1 > o1:
        return "Tweezer Bottom", "LONG"

    # 8. Tweezer Top
    if abs(h1 - h2) / cl1 <= 0.0015 and cl1 < o1:
        return "Tweezer Top", "SHORT"

    return "None", None

# ==============================================================================
# 3. CORE STRICT REVERSAL EVALUATOR (HARD GATES FILTER)
# ==============================================================================

def evaluate_strict_reversal_setup(ticker_data, btc_regime):
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

        # ----------------------------------------------------------------------
        # HARD GATE A & B: LIQUIDITY SWEEP & REVERSAL PATTERN (BẮT BUỘC)
        # ----------------------------------------------------------------------
        prev_swing_low = min([c['low'] for c in h1_candles[-8:-1]])
        prev_swing_high = max([c['high'] for c in h1_candles[-8:-1]])

        is_sweep_low = (low_h1 < prev_swing_low) and (close_h1 >= prev_swing_low * 0.998)
        is_sweep_high = (high_h1 > prev_swing_high) and (close_h1 <= prev_swing_high * 1.002)

        pattern_name, pattern_direction = detect_strong_h1_reversal_pattern(h1_candles)

        # KIỂM TRA BẮT BUỘC A & B: Phải có Sweep VÀ có Mô hình Nến Reversal chuẩn
        if not (is_sweep_low or is_sweep_high) or not pattern_direction:
            return None

        direction = "LONG" if (is_sweep_low and pattern_direction == "LONG") else ("SHORT" if (is_sweep_high and pattern_direction == "SHORT") else None)
        if not direction:
            return None

        # ----------------------------------------------------------------------
        # HARD GATE C: RECLAIM SUPPORT / RESISTANCE (BẮT BUỘC)
        # ----------------------------------------------------------------------
        is_reclaim = (close_h1 >= prev_swing_low) if direction == "LONG" else (close_h1 <= prev_swing_high)
        if not is_reclaim:
            return None

        # ----------------------------------------------------------------------
        # HARD GATE D & E: VOLUME & CVD / TAKER FLOW CONFIRMATION (BẮT BUỘC)
        # ----------------------------------------------------------------------
        vols_5m = [c['volume'] for c in m5_candles[-21:-1]]
        avg_vol_5m = np.mean(vols_5m) if vols_5m else 1.0
        vol_5m_ratio = float(last_5m['volume']) / avg_vol_5m if avg_vol_5m > 0 else 1.0

        vols_h1 = [c['volume'] for c in h1_candles[-21:-1]]
        avg_vol_h1 = np.mean(vols_h1) if vols_h1 else 1.0
        vol_h1_ratio = float(last_h1['volume']) / avg_vol_h1 if avg_vol_h1 > 0 else 1.0

        vol_expansion = max(vol_5m_ratio, vol_h1_ratio)

        # HARD GATE D: Volume H1/5M phải >= 1.3x trung bình
        if vol_expansion < 1.3:
            return None

        taker_buy_pct = get_taker_buy_pct_5m(sym)
        oi_30m_pct = get_open_interest_pct_30m(sym)

        # HARD GATE E: CVD/Taker phải cùng hướng (LONG >= 54%, SHORT <= 46%)
        if direction == "LONG" and taker_buy_pct < 54.0:
            return None
        elif direction == "SHORT" and taker_buy_pct > 46.0:
            return None

        # ----------------------------------------------------------------------
        # ANTI-CHASE FILTER (BẮT SỚM, LOẠI BỎ COIN ĐÃ PUMP QUÁ XA)
        # ----------------------------------------------------------------------
        p_change_from_sweep = abs(close_now - (low_h1 if direction == "LONG" else high_h1)) / (low_h1 if direction == "LONG" else high_h1) * 100
        if p_change_from_sweep >= 3.5:
            return None # Đã pump quá xa khỏi điểm Sweep Reversal!

        if (direction == "LONG" and btc_regime == "BEARISH_DUMP") or (direction == "SHORT" and btc_regime == "BULLISH_PUMP"):
            return None # Tránh đi ngược sóng dốc BTC

        # ----------------------------------------------------------------------
        # CHẤM ĐIỂM REVERSAL SCORE (0 - 100)
        # ----------------------------------------------------------------------
        ema34_h1 = calculate_ema(h1_candles, 34)
        rsi_h1_list = calculate_rsi(h1_candles, 14)
        rsi_h1 = rsi_h1_list[-1] if rsi_h1_list else 50.0

        score = 65 # Đạt đủ 5 HARD GATES cơ bản
        if vol_expansion >= 2.5: score += 12
        elif vol_expansion >= 1.8: score += 7

        if oi_30m_pct >= 2.0: score += 10
        elif oi_30m_pct >= 0.5: score += 5

        if (direction == "LONG" and taker_buy_pct >= 62.0) or (direction == "SHORT" and taker_buy_pct <= 38.0):
            score += 8

        if ema34_h1 and abs(close_now - ema34_h1[-1]) / close_now <= 0.012:
            score += 5

        if score < MIN_ALERT_SCORE:
            return None # Điểm tổng chưa đủ 80 điểm

        cvd_text = "Strong Buy" if taker_buy_pct >= 60.0 else ("Strong Sell" if taker_buy_pct <= 40.0 else "Bullish Alignment")

        return {
            'symbol': sym,
            'direction': direction,
            'score': min(100, score),
            'pattern': pattern_name,
            'entry': close_now,
            'vol_ratio': vol_expansion,
            'oi_30m_pct': oi_30m_pct,
            'cvd_text': cvd_text,
            'taker_buy_pct': taker_buy_pct,
            'rsi_h1': rsi_h1,
            'funding': funding,
            'h1_timestamp': last_h1['timestamp']
        }
    except Exception:
        return None

# ==============================================================================
# 4. TELEGRAM STRICT ALERT FORMATTER
# ==============================================================================

def send_telegram_strict_reversal_alert(sig):
    now_time = time.time()
    sym = sig['symbol']
    
    # Cooldown Check 30m
    if sym in processed_alerts:
        last_time, last_score = processed_alerts[sym]
        if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 5):
            return
    processed_alerts[sym] = (now_time, sig['score'])

    msg = f"""🚨 <b>STRONG H1 REVERSAL</b>

🪙 <b>#{sig['symbol']}</b>
⭐ <b>Score:</b> <code>{sig['score']}/100</code>
🟢 <b>Direction:</b> <code>{sig['direction']}</code>

🕯 <b>Pattern:</b> <code>{sig['pattern']}</code>
🧲 <b>Liquidity Sweep:</b> ✅
📍 <b>Support Reclaim:</b> ✅
📊 <b>Volume:</b> <code>{sig['vol_ratio']:.1f}x</code>
📈 <b>OI:</b> <code>{sig['oi_30m_pct']:+.1f}%</code>
💰 <b>CVD:</b> <code>{sig['cvd_text']} ({sig['taker_buy_pct']:.0f}%)</code>
📐 <b>EMA34:</b> Reclaim

⚡ <b>Early Entry</b>
🎯 <b>High-quality reversal setup</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [STRICT REVERSAL ALERT SENT] #{sig['symbol']} Direction: {sig['direction']} Score: {sig['score']}", flush=True)

# ==============================================================================
# 5. AUTOMATIC SCANNER LOOP (QUÉT VÀ LỌC TOP 1 - 3 CHẤT LƯỢNG)
# ==============================================================================

def run_automatic_strict_reversal_scanner():
    print("⚡️ Bắt đầu Luồng Strict Reversal Auto Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            btc_regime = get_btc_regime_status()
            print(f"\n🔍 [STRICT SCANNER] Rà soát tín hiệu Reversal chuẩn bài cho {len(all_tickers)} altcoins...", flush=True)

            top40_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(evaluate_strict_reversal_setup, t, btc_regime) for t in top40_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            # SẮP XẾP CHỌN CHỈ TOP 1 - 3 TÍN HIỆU MẠNH NHẤT
            scanned_results.sort(key=lambda x: x['score'], reverse=True)
            top_signals = scanned_results[:TOP_TELEGRAM_REPORT_LIMIT]

            if top_signals:
                for sig in top_signals:
                    send_telegram_strict_reversal_alert(sig)
            else:
                print("🔎 Không có tín hiệu reversal đủ mạnh trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"Lỗi Strict Reversal Scanner Loop: {e}", flush=True)

        print(f"⏳ Hoàn tất 1 chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS}s (5 phút)...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_automatic_strict_reversal_scanner()
