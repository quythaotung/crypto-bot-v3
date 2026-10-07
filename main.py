import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - ADAPTIVE RUNNER SCANNER & PERFORMANCE TRACKER
# ==============================================================================
SCAN_INTERVAL_SECONDS = 300            # Quét tự động mỗi 5 phút (300 giây)
MIN_FUTURES_TURNOVER_24H = 8_000_000   # Thanh khoản 24h tối thiểu >= $8M USD
TOP_TIER1_LIMIT = 35                   # Tầng 1 lọc Top 35 coin có đà dòng tiền năng động nhất
TOP_TELEGRAM_REPORT_LIMIT = 5          # Tự chọn Top 5 Runner mạnh nhất gửi Telegram
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút chống spam cùng 1 coin

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_alerts = {}                  # Lưu cooldown & score gần nhất
alert_performance_history = []         # Lưu lịch sử alert để tracking hiệu suất 15m/30m/1h/4h

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Adaptive Runner Scanner & Auto-Tracker Active 24/7!"

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
# 2. CORE ADAPTIVE RUNNER SCORING ENGINE (100 PTS SYSTEM)
# ==============================================================================

def analyze_adaptive_runner_coin(ticker_data, rs_map):
    try:
        sym = ticker_data['symbol']
        funding = ticker_data['fundingRate']

        m5_candles = get_klines(sym, "5", limit=30)
        h1_candles = get_klines(sym, "60", limit=35)
        h1_spot = get_klines(sym, "60", category="spot", limit=35)

        if not m5_candles or len(m5_candles) < 20 or not h1_candles or len(h1_candles) < 22:
            return None

        # Price Measurements
        last_5m = m5_candles[-1]
        close_now = float(last_5m['close'])

        p_change_5m = (close_now - float(m5_candles[-2]['open'])) / float(m5_candles[-2]['open']) * 100
        p_change_15m = (close_now - float(m5_candles[-4]['open'])) / float(m5_candles[-4]['open']) * 100
        p_change_1h = (close_now - float(h1_candles[-2]['open'])) / float(h1_candles[-2]['open']) * 100

        # Volume Expansion
        vols_5m_20 = [c['volume'] for c in m5_candles[-21:-1]]
        avg_vol_5m = np.mean(vols_5m_20) if vols_5m_20 else 1.0
        vol_5m_ratio = float(last_5m['volume']) / avg_vol_5m if avg_vol_5m > 0 else 1.0

        spot_vol_ratio = 1.0
        if h1_spot and len(h1_spot) >= 20:
            spot_vols_20 = [c['volume'] for c in h1_spot[-21:-1]]
            avg_spot_vol = np.mean(spot_vols_20) if spot_vols_20 else 1.0
            spot_vol_ratio = float(h1_spot[-1]['volume']) / avg_spot_vol if avg_spot_vol > 0 else 1.0

        # Orderflow
        oi_30m_pct = get_open_interest_pct_30m(sym)
        taker_buy_pct = get_taker_buy_pct_5m(sym)

        # Structure H1
        last_h1 = h1_candles[-1]
        open_h1, high_h1, low_h1, close_h1 = float(last_h1['open']), float(last_h1['high']), float(last_h1['low']), float(last_h1['close'])
        body_h1 = abs(close_h1 - open_h1)
        lower_wick_h1 = min(open_h1, close_h1) - low_h1
        upper_wick_h1 = high_h1 - max(open_h1, close_h1)

        prev_swing_low = min([c['low'] for c in h1_candles[-7:-1]])
        prev_swing_high = max([c['high'] for c in h1_candles[-7:-1]])

        is_sweep_low = (low_h1 < prev_swing_low) and (close_h1 >= prev_swing_low * 0.998)
        is_sweep_high = (high_h1 > prev_swing_high) and (close_h1 <= prev_swing_high * 1.002)
        is_reclaim_support = close_h1 >= prev_swing_low
        is_break_resistance = close_h1 >= prev_swing_high

        # Room To Run & Resistance Calculation
        next_resistance = max([c['high'] for c in h1_candles[-20:-1]])
        room_to_run_pct = ((next_resistance - close_now) / close_now * 100) if next_resistance > close_now else 1.5

        # EMA & RSI H1
        ema34_h1 = calculate_ema(h1_candles, 34)
        ema89_h1 = calculate_ema(h1_candles, 89)
        rsi_h1_list = calculate_rsi(h1_candles, 14)
        rsi_h1 = rsi_h1_list[-1] if rsi_h1_list else 50.0

        # Relative Strength Rank
        rs_rank = rs_map.get(sym, 50.0)

        # --------------------------------------------------------------------------
        # STAGE & OVERHEAT CLASSIFICATION
        # --------------------------------------------------------------------------
        overheat_score = 0
        if rsi_h1 >= 72.0: overheat_score += 30
        if p_change_1h >= 8.5: overheat_score += 40
        if funding >= 0.05: overheat_score += 30

        stage = "EARLY"
        if overheat_score >= 60 or room_to_run_pct < 0.8:
            stage = "LATE/CHASE"
        elif p_change_1h >= 2.5 and vol_5m_ratio >= 2.0:
            stage = "CONFIRMED"

        # TỰ ĐỘNG CHẶN KÈO ĐÃ CHẠY QUÁ XA (LATE / CHASE)
        if stage == "LATE/CHASE": return None

        # --------------------------------------------------------------------------
        # TÍNH TOÁN 100 ĐIỂM RUNNER SCORE
        # --------------------------------------------------------------------------
        s_structure = 15 if (is_sweep_low and is_reclaim_support) or is_break_resistance else (10 if is_reclaim_support else 5)
        s_vol = 20 if vol_5m_ratio >= 3.0 else (15 if vol_5m_ratio >= 2.0 else (8 if vol_5m_ratio >= 1.4 else 3))
        s_cvd = 15 if taker_buy_pct >= 62.0 else (10 if taker_buy_pct >= 55.0 else 4) # Chỉ cộng/trừ điểm, không bắt buộc!
        s_oi = 10 if oi_30m_pct >= 1.5 else (7 if oi_30m_pct >= 0.5 else 2)          # Chỉ cộng/trừ điểm, không bắt buộc!
        s_spot = 10 if spot_vol_ratio >= 2.0 else (6 if spot_vol_ratio >= 1.4 else 2)
        s_rs = 10 if rs_rank <= 10.0 else (7 if rs_rank <= 25.0 else 3)
        s_trend = 10 if (ema34_h1 and abs(close_now - ema34_h1[-1])/close_now < 0.012 and 45 <= rsi_h1 <= 65) else 4
        s_volatility = 5 if abs(p_change_5m) >= 0.8 or abs(p_change_15m) >= 1.5 else 2
        s_funding = 5 if overheat_score == 0 else 2

        runner_score = s_structure + s_vol + s_cvd + s_oi + s_spot + s_rs + s_trend + s_volatility + s_funding

        # SURGE OVERRIDE (Dòng tiền gia tốc vượt trội bắt sớm)
        if vol_5m_ratio >= 3.0 and p_change_5m >= 1.2 and taker_buy_pct >= 65.0 and rs_rank <= 10.0:
            runner_score = max(runner_score, 88)
            stage = "EARLY"

        if runner_score < 80: return None # Chỉ lấy từ DEVELOPING trở lên

        # CHỌN DẠNG SETUP CHÍNH
        setup_type = "H1 REVERSAL"
        if vol_5m_ratio >= 2.5 and is_break_resistance: setup_type = "MOMENTUM BREAKOUT"
        elif rs_rank <= 8.0: setup_type = "RELATIVE STRENGTH"
        elif ema34_h1 and abs(close_now - ema34_h1[-1])/close_now <= 0.008: setup_type = "PULLBACK CONTINUATION"
        elif s_structure < 10 and oi_30m_pct >= 1.0: setup_type = "ACCUMULATION → EXPANSION"

        # MỨC PHÂN LOẠI
        tier_label = "DEVELOPING"
        if runner_score >= 95: tier_label = "🚀 EXPLOSIVE RUNNER"
        elif runner_score >= 90: tier_label = "🔥 HIGH CONVICTION RUNNER"
        elif runner_score >= 85: tier_label = "⚡️ EARLY RUNNER"

        # LÍ DO ĐÁNH GIÁ & YẾU TỐ THIẾU
        why_list = []
        missing_list = []

        if is_sweep_low: why_list.append("Liquidity Sweep")
        else: missing_list.append("Chưa Sweep")

        if vol_5m_ratio >= 2.0: why_list.append(f"Vol 5M {vol_5m_ratio:.1f}x")
        else: missing_list.append("Vol 5M Vừa")

        if taker_buy_pct >= 60.0: why_list.append(f"CVD Buy {taker_buy_pct:.0f}%")
        else: missing_list.append("CVD Trung Tính")

        if oi_30m_pct >= 0.8: why_list.append(f"OI +{oi_30m_pct:.1f}%")
        else: missing_list.append("OI Đi Ngang")

        if rs_rank <= 15.0: why_list.append(f"Top {rs_rank:.1f}% RS")

        return {
            'symbol': sym,
            'direction': "LONG",
            'runner_score': runner_score,
            'setup_type': setup_type,
            'stage': stage,
            'tier_label': tier_label,
            'entry': close_now,
            'p_change_5m': p_change_5m,
            'p_change_15m': p_change_15m,
            'p_change_1h': p_change_1h,
            'vol_5m_ratio': vol_5m_ratio,
            'spot_vol_ratio': spot_vol_ratio,
            'oi_30m_pct': oi_30m_pct,
            'taker_buy_pct': taker_buy_pct,
            'rs_rank': rs_rank,
            'rsi_h1': rsi_h1,
            'next_resistance': next_resistance,
            'room_to_run_pct': room_to_run_pct,
            'overheat_score': overheat_score,
            'why_list': ", ".join(why_list) if why_list else "Base Momentum",
            'missing_list': ", ".join(missing_list) if missing_list else "Không",
            'h1_timestamp': last_h1['timestamp']
        }
    except Exception:
        return None

# ==============================================================================
# 3. TELEGRAM ALERT FORMATTER
# ==============================================================================

def send_telegram_runner_alert(sig):
    now_time = time.time()
    sym = sig['symbol']
    
    # Cooldown Check
    if sym in processed_alerts:
        last_time, last_score = processed_alerts[sym]
        if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['runner_score'] <= last_score + 5):
            return
    processed_alerts[sym] = (now_time, sig['runner_score'])

    cvd_text = "BUY PRESSURE" if sig['taker_buy_pct'] >= 58.0 else ("SELL PRESSURE" if sig['taker_buy_pct'] <= 42.0 else "NEUTRAL")

    msg = f"""🔥 <b>HIGH-PROBABILITY RUNNER — {sig['tier_label']}</b>

🪙 <b>#{sig['symbol']}</b>
⭐ <b>Runner Score:</b> <code>{sig['runner_score']}/100</code>
🎯 <b>Setup Type:</b> <code>{sig['setup_type']}</code>
🚦 <b>Stage:</b> <b>{sig['stage']}</b>
📍 <b>Entry Price:</b> <code>{sig['entry']:.4f}</code>

📊 <b>RUNNER DYNAMICS & METRICS:</b>
• Relative Strength: <code>TOP {sig['rs_rank']:.1f}% Market</code>
• Volume 5M: <code>{sig['vol_5m_ratio']:.1f}x Expansion</code>
• OI 30m Change: <code>{sig['oi_30m_pct']:+.2f}%</code>
• CVD Flow: <code>{cvd_text} ({sig['taker_buy_pct']:.0f}%)</code>
• Spot Flow Ratio: <code>{sig['spot_vol_ratio']:.2f}x</code>
• Price Change: <code>5M ({sig['p_change_5m']:+.2f}%) | 15M ({sig['p_change_15m']:+.2f}%) | 1H ({sig['p_change_1h']:+.2f}%)</code>
• RSI H1: <code>{sig['rsi_h1']:.1f}</code>
• Next Resistance: <code>{sig['next_resistance']:.4f}</code> (<b>Room To Run: +{sig['room_to_run_pct']:.1f}%</b>)
• Overheat Status: <code>{sig['overheat_score']}/100 (Safe)</code>

⚠️ <b>YẾU TỐ CÒN THIẾU:</b> <code>{sig['missing_list']}</code>

🧠 <b>VERDICT:</b> {sig['why_list']}. Coin đang ở giai đoạn <b>{sig['stage']}</b> có cấu trúc & dòng tiền tích lũy mạnh, dư địa chạy còn tới kháng cự H1!"""

    send_telegram_direct_message(msg)
    
    # Lưu vết Performance Tracking sau 15m/30m/1h/4h
    alert_performance_history.append({
        'symbol': sym,
        'entry_price': sig['entry'],
        'alert_time': now_time,
        'score': sig['runner_score'],
        'setup_type': sig['setup_type']
    })
    print(f"✅ [ALERT RUNNER SENT] {sig['tier_label']} #{sig['symbol']} Score: {sig['runner_score']}", flush=True)

# ==============================================================================
# 4. AUTO SCANNER RUNNER & TRACKER
# ==============================================================================

def run_adaptive_runner_scanner():
    print("⚡️ Bắt đầu Luồng Adaptive Runner Scanner & Auto Tracker...", flush=True)
    while True:
        try:
            start_time = time.time()
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"\n🔍 [RUNNER SCANNER] Bắt đầu rà soát {len(all_tickers)} altcoins...", flush=True)

            # BẢNG MẠNH TƯƠNG ĐỐI (RS RANKING)
            sorted_by_change = sorted(all_tickers, key=lambda x: x['price24hPcnt'], reverse=True)
            total_count = len(sorted_by_change) if sorted_by_change else 1
            rs_map = {t['symbol']: ((idx + 1) / total_count) * 100 for idx, t in enumerate(sorted_by_change)}

            # TẦNG 1: LỌC TOP 35 COIN NĂNG ĐỘNG DÒNG TIỀN
            top35_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:TOP_TIER1_LIMIT]

            # TẦNG 2: PHÂN TÍCH ADAPTIVE RUNNER SCORE
            scanned_results = []
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(analyze_adaptive_runner_coin, t, rs_map) for t in top35_candidates]
                for f in futures:
                    res = f.result()
                    if res:
                        scanned_results.append(res)

            # TỰ ĐỘNG CHỌN TOP 5 KÈO TỐT NHẤT CHU KỲ
            scanned_results.sort(key=lambda x: x['runner_score'], reverse=True)
            top5_runners = scanned_results[:TOP_TELEGRAM_REPORT_LIMIT]

            elapsed = time.time() - start_time
            print(f"✅ Hoàn tất trong {elapsed:.1f}s. Tìm thấy {len(top5_runners)} Runner xuất sắc.", flush=True)

            if top5_runners:
                for sig in top5_runners:
                    if sig['runner_score'] >= 80:
                        send_telegram_runner_alert(sig)
            else:
                print("🔎 Chưa có Runner nào đạt ngưỡng Score >= 80 trong chu kỳ này.", flush=True)

        except Exception as e:
            print(f"Lỗi Runner Scanner Loop: {e}", flush=True)

        print(f"⏳ Tạm nghỉ {SCAN_INTERVAL_SECONDS}s (5 phút)...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_adaptive_runner_scanner()
