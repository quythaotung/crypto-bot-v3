import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - H1/5M REALTIME REVERSAL ENGINE (LONG & SHORT)
# ==============================================================================
DEBUG_MODE = True                  
SCAN_INTERVAL_SECONDS = 300            # Cứ mỗi 5 phút quét 1 lần toàn thị trường
MIN_FUTURES_TURNOVER_24H = 8_000_000   # $8M USD Minimum Turnover
MIN_ALERT_SCORE = 80                   # Chỉ gửi Telegram chính từ 80 điểm trở lên
COOLDOWN_ALERT_SECONDS = 1800          # Cooldown 30 phút cùng 1 coin trừ khi score tăng mạnh

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_signals = {}                 # Lưu cooldown & score gần nhất
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Realtime H1/5M Reversal Engine (LONG & SHORT) Active 24/7!"

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

def get_klines_with_current(symbol, interval, limit=30):
    """Lấy danh sách Klines bao gồm cả nến đang chạy real-time"""
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

def get_btc_regime_context():
    """Kiểm tra xu hướng BTC 5M/H1 để tính 5 điểm Market Regime"""
    btc_klines = get_klines_with_current("BTCUSDT", "5", limit=10)
    if not btc_klines or len(btc_klines) < 3: return "NEUTRAL", 3
    change_5m = (btc_klines[-1]['close'] - btc_klines[-2]['open']) / btc_klines[-2]['open'] * 100
    if change_5m <= -1.2:
        return "BTC_DUMPING", 0
    elif change_5m >= 1.2:
        return "BTC_PUMPING", 5
    return "STABLE", 3

# ==============================================================================
# CORE REVERSAL EVALUATOR (LONG & SHORT 100-POINT SYSTEM)
# ==============================================================================

def evaluate_coin_reversal(ticker_data, btc_regime):
    sym = ticker_data['symbol']
    funding = ticker_data['fundingRate']

    # 1. LẤY DỮ LIỆU KLINE 5M VÀ H1
    m5_candles = get_klines_with_current(sym, "5", limit=30)
    h1_candles = get_klines_with_current(sym, "60", limit=30)

    if not m5_candles or len(m5_candles) < 22 or not h1_candles or len(h1_candles) < 22:
        return None

    last_5m = m5_candles[-1]
    curr_price = last_5m['close']

    # Kiểm tra biến động 5M
    vols_5m_20 = [c['volume'] for c in m5_candles[-21:-1]]
    avg_vol_5m = np.mean(vols_5m_20) if vols_5m_20 else 1.0
    vol_5m_ratio = last_5m['volume'] / avg_vol_5m if avg_vol_5m > 0 else 1.0

    # Lấy dữ liệu H1 đang chạy (In-progress Candle)
    curr_h1 = h1_candles[-1]
    prev_h1_swing_low = min([c['low'] for c in h1_candles[-6:-1]])
    prev_h1_swing_high = max([c['high'] for c in h1_candles[-6:-1]])

    open_h1, high_h1, low_h1, close_h1 = curr_h1['open'], curr_h1['high'], curr_h1['low'], curr_h1['close']
    body_h1 = abs(close_h1 - open_h1)
    lower_wick_h1 = min(open_h1, close_h1) - low_h1
    upper_wick_h1 = high_h1 - max(open_h1, close_h1)

    # Indicator H1
    ema34_h1 = calculate_ema(h1_candles, 34)
    ema89_h1 = calculate_ema(h1_candles, 89)
    rsi_h1_list = calculate_rsi(h1_candles, 14)

    if not ema34_h1 or not rsi_h1_list: return None
    rsi_h1 = rsi_h1_list[-1]

    # Orderflow
    oi_pct_5m = get_open_interest_pct(sym, "5m")
    taker_buy_pct = get_taker_buy_sell_ratio(sym, "5m")

    # Anti-chase / Overextended Check
    p_change_1h = (close_h1 - h1_candles[-3]['open']) / h1_candles[-3]['open'] * 100 if len(h1_candles) >= 3 else 0
    if p_change_1h > 12.0 or p_change_1h < -12.0:
        return None # Đã overextended quá xa

    # --------------------------------------------------------------------------
    # CHECK LONG REVERSAL
    # --------------------------------------------------------------------------
    is_long_sweep = (low_h1 < prev_h1_swing_low) and (close_h1 >= prev_h1_swing_low * 0.998)
    wick_ratio_long = (lower_wick_h1 / body_h1) if body_h1 > 0 else 0.0

    if is_long_sweep or (wick_ratio_long >= 1.5 and close_h1 >= open_h1):
        # Chấm điểm LONG (Max 100)
        s_sweep = 25 if is_long_sweep else 10
        s_wick = 15 if wick_ratio_long >= 2.0 else (10 if wick_ratio_long >= 1.2 else 5)
        s_reclaim = 15 if close_h1 >= prev_h1_swing_low else 8
        s_vol = 10 if vol_5m_ratio >= 2.0 else (6 if vol_5m_ratio >= 1.4 else 2)
        s_oi = 10 if oi_pct_5m >= 1.0 else 5
        s_cvd = 10 if taker_buy_pct >= 62.0 else 4
        s_ema = 5 if (abs(close_h1 - ema34_h1[-1])/close_h1 < 0.01 or (ema89_h1 and abs(close_h1 - ema89_h1[-1])/close_h1 < 0.01)) else 2
        s_rsi = 5 if 35.0 <= rsi_h1 <= 58.0 else 2
        s_btc = btc_regime[1]

        score_long = s_sweep + s_wick + s_reclaim + s_vol + s_oi + s_cvd + s_ema + s_rsi + s_btc

        if score_long >= MIN_ALERT_SCORE:
            return {
                'symbol': sym,
                'side': 'LONG',
                'score': score_long,
                'entry': curr_price,
                'is_sweep': is_long_sweep,
                'wick_ratio': wick_ratio_long,
                'vol_5m_ratio': vol_5m_ratio,
                'oi_pct': oi_pct_5m,
                'taker_buy': taker_buy_pct,
                'rsi_h1': rsi_h1,
                'funding': funding,
                'timestamp': curr_h1['timestamp']
            }

    # --------------------------------------------------------------------------
    # CHECK SHORT REVERSAL
    # --------------------------------------------------------------------------
    is_short_sweep = (high_h1 > prev_h1_swing_high) and (close_h1 <= prev_h1_swing_high * 1.002)
    wick_ratio_short = (upper_wick_h1 / body_h1) if body_h1 > 0 else 0.0

    if is_short_sweep or (wick_ratio_short >= 1.5 and close_h1 <= open_h1):
        s_sweep = 25 if is_short_sweep else 10
        s_wick = 15 if wick_ratio_short >= 2.0 else (10 if wick_ratio_short >= 1.2 else 5)
        s_reclaim = 15 if close_h1 <= prev_h1_swing_high else 8
        s_vol = 10 if vol_5m_ratio >= 2.0 else (6 if vol_5m_ratio >= 1.4 else 2)
        s_oi = 10 if oi_pct_5m >= 1.0 else 5
        s_cvd = 10 if taker_buy_pct <= 38.0 else 4
        s_ema = 5 if (abs(close_h1 - ema34_h1[-1])/close_h1 < 0.01 or (ema89_h1 and abs(close_h1 - ema89_h1[-1])/close_h1 < 0.01)) else 2
        s_rsi = 5 if 42.0 <= rsi_h1 <= 65.0 else 2
        s_btc = 5 if btc_regime[0] == "BTC_DUMPING" else 3

        score_short = s_sweep + s_wick + s_reclaim + s_vol + s_oi + s_cvd + s_ema + s_rsi + s_btc

        if score_short >= MIN_ALERT_SCORE:
            return {
                'symbol': sym,
                'side': 'SHORT',
                'score': score_short,
                'entry': curr_price,
                'is_sweep': is_short_sweep,
                'wick_ratio': wick_ratio_short,
                'vol_5m_ratio': vol_5m_ratio,
                'oi_pct': oi_pct_5m,
                'taker_buy': taker_buy_pct,
                'rsi_h1': rsi_h1,
                'funding': funding,
                'timestamp': curr_h1['timestamp']
            }

    return None

# ==============================================================================
# TELEGRAM ALERT FORMATTER
# ==============================================================================

def send_telegram_reversal_alert(sig, is_confirmed=False):
    now_time = time.time()
    sym = sig['symbol']
    
    # Anti-spam Cooldown Logic
    if not is_confirmed:
        if sym in processed_signals:
            last_time, last_score = processed_signals[sym]
            if (now_time - last_time < COOLDOWN_ALERT_SECONDS) and (sig['score'] <= last_score + 8):
                return # Bỏ qua nếu chưa đủ 30p và score không tăng đột biến
        processed_signals[sym] = (now_time, sig['score'])

    sweep_icon = "✅" if sig['is_sweep'] else "❌"
    side_icon = "🟢 LONG" if sig['side'] == 'LONG' else "🔴 SHORT"
    header_title = f"✅ H1 REVERSAL CONFIRMED — {sig['side']}" if is_confirmed else f"🔥 EARLY H1 REVERSAL — {sig['side']}"
    cvd_text = "Strong Buy" if sig['taker_buy'] >= 60 else ("Strong Sell" if sig['taker_buy'] <= 40 else "Neutral")

    msg = f"""<b>{header_title}</b>
🪙 <b>#{sig['symbol']}</b>
⭐ <b>Score:</b> <code>{sig['score']}/100</code>
📍 <b>Entry Zone:</b> <code>{sig['entry']:.4f}</code>
🧲 <b>Liquidity Sweep:</b> {sweep_icon}
🕯 <b>Wick Ratio:</b> <code>{sig['wick_ratio']:.1f}x Body</code>
📈 <b>Volume 5M:</b> <code>{sig['vol_5m_ratio']:.1f}x</code>
📊 <b>OI 5M:</b> <code>{sig['oi_pct']:+.1f}%</code>
💰 <b>CVD Flow:</b> <code>{cvd_text} ({sig['taker_buy']:.0f}%)</code>
📐 <b>EMA Status:</b> Reclaim Confluence
📊 <b>RSI H1:</b> <code>{sig['rsi_h1']:.1f}</code>
⚡️ <b>5M Momentum:</b> STRONG
🧠 <b>H1 Structure:</b> REVERSAL ({sig['side']})

⚠️ <b>Check H1 ngay.</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [SENT REVERSAL] {header_title} {sig['symbol']} Score: {sig['score']}", flush=True)

# ==============================================================================
# MANUAL /scan SNAPSHOT PROCESSOR
# ==============================================================================

def execute_manual_snapshot_scan():
    send_telegram_direct_message("⚡️ <b>ĐANG QUÉT SNAPSHOT ĐẢO CHIỀU REVERSAL 5M/H1...</b>\n⏳ Lọc ra Top coin có Liquidity Sweep & Dòng tiền nổ đà trong 2-3s!")
    start_time = time.time()
    
    all_tickers = get_all_bybit_crypto_symbols()
    btc_regime = get_btc_regime_context()

    sorted_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.5 + (x['turnover24h']/1e7) * 0.5), reverse=True)[:40]

    scanned_results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(evaluate_coin_reversal, t, btc_regime) for t in sorted_candidates]
        for f in futures:
            res = f.result()
            if res:
                scanned_results.append(res)

    scanned_results.sort(key=lambda x: x['score'], reverse=True)
    top_candidates = scanned_results[:3]
    elapsed = time.time() - start_time

    if not top_candidates:
        send_telegram_direct_message(f"📊 <b>SNAPSHOT REVERSAL:</b>\n\nThị trường chưa có coin nào thỏa điểm Reversal (Score ≥ 80). Quét xong trong {elapsed:.1f}s.")
        return

    msg = f"⚠️ <b>SNAPSHOT REAL-TIME — TOP {len(top_candidates)} COIN ĐẢO CHIỀU POTENTIAL</b>\n"
    msg += f"UTC Dữ Liệu: <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d T%H:%M:%S')}</code> | Quét: <b>{elapsed:.1f}s</b>\n\n"

    for c in top_candidates:
        sweep_str = "YES" if c['is_sweep'] else "NO"
        msg += f"🔥 <b>#{c['symbol']}</b> ({c['side']}) — <b>{c['score']}/100</b>\n"
        msg += f"Price: <code>{c['entry']:.4f}</code> | Sweep: <b>{sweep_str}</b>\n"
        msg += f"Vol 5M: <code>+{c['vol_5m_ratio']:.1f}x</code> | OI 5M: <code>{c['oi_pct']:+.1f}%</code> | Taker: <code>{c['taker_buy']:.0f}%</code>\n\n"

    msg += "👉 <i>Nhập /scan bất kỳ lúc nào để cập nhật Top 3 Snapshot đảo chiều mới nhất!</i>"
    send_telegram_direct_message(msg)

# ==============================================================================
# MAIN RADAR LOOP (QUÉT 5 PHÚT TỰ ĐỘNG)
# ==============================================================================

def run_radar_loop():
    print("⚡️ Bắt đầu Luồng Radar H1/5M Reversal Engine...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            btc_regime = get_btc_regime_context()
            print(f"🔍 [5M REVERSAL RADAR] Đang rà soát đảo chiều cho {len(all_tickers)} altcoins...", flush=True)

            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = [executor.submit(evaluate_coin_reversal, t, btc_regime) for t in all_tickers]
                for f in futures:
                    sig = f.result()
                    if sig and sig['score'] >= MIN_ALERT_SCORE:
                        send_telegram_alert(sig, is_confirmed=False)

        except Exception as e:
            print(f"Lỗi Radar Loop: {e}", flush=True)

        print(f"✅ Hoàn tất 1 lượt quét. Tạm nghỉ {SCAN_INTERVAL_SECONDS}s (5 phút)...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

def process_telegram_commands():
    last_update_id = 0
    print("🤖 Bật luồng Telegram Listener (/scan, /ping)...", flush=True)
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=8"
            res = requests.get(url, timeout=10).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip().lower()
                    
                    if text in ["/scan", "scan"]:
                        Thread(target=execute_manual_snapshot_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H1/5M Reversal Engine đang chạy 24/7. Nhập <b>/scan</b> để xem Snapshot!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(1)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
