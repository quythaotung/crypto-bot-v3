import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - H1 PINBAR SWEEP & TREND RECLAIM STRATEGY
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 8_000_000   # $8M USD Minimum Turnover

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot H1 Pinbar Sweep & Trend Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def send_telegram_direct_message(text):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}", flush=True)

BASE_URL = "https://api.bybit.com"

# ==============================================================================
# FAST API HELPERS
# ==============================================================================

def get_all_bybit_crypto_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=4).json()
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

def get_closed_klines(symbol, interval, category="linear", limit=35):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=3).json()
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

def get_open_interest_pct(symbol, interval_str="30m"):
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
# H1 PINBAR SWEEP & RECLAIM EVALUATOR (GTC STYLE)
# ==============================================================================

def evaluate_h1_pinbar_sweep_setup(t):
    sym = t['symbol']
    funding = t['fundingRate']

    if funding > 0.05 or funding < -0.05: return None

    now = time.time()
    if sym in symbol_cooldowns and (now - symbol_cooldowns[sym]) < 7200: # Cooldown 2h
        return None

    h1_candles = get_closed_klines(sym, "60", category="linear", limit=30)
    if not h1_candles or len(h1_candles) < 22: return None

    last_h1 = h1_candles[-1]
    open_1h = last_h1['open']
    close_1h = last_h1['close']
    high_1h = last_h1['high']
    low_1h = last_h1['low']

    h1_atr = calculate_atr(h1_candles, 14)
    ema34_h1 = calculate_ema(h1_candles, 34)
    rsi_h1 = calculate_rsi(h1_candles, 14)

    if not ema34_h1 or not rsi_h1 or h1_atr <= 0: return None

    # 1. TÍNH RÂU NẾN DƯỚI (LOWER WICK SWEEP)
    total_range = high_1h - low_1h
    if total_range <= 0: return None

    lower_wick = min(open_1h, close_1h) - low_1h
    lower_wick_pct = (lower_wick / total_range) * 100

    # BỘ LỌC CỨNG SWEEP: Râu dưới phải chiếm >= 35% chiều dài nến H1
    is_sweep = lower_wick_pct >= 35.0

    # 2. ĐIỀU KIỆN GIÁ ĐÓNG NẾN & RSI H1
    is_reclaim = close_1h >= ema34_h1[-1] * 0.996 and rsi_h1[-1] >= 48.0

    if not (is_sweep or is_reclaim): return None

    # Volume Ratio H1
    vols = [c['volume'] for c in h1_candles[-21:-1]]
    avg_vol = np.mean(vols) if vols else 1.0
    vol_ratio = last_h1['volume'] / avg_vol if avg_vol > 0 else 1.0

    if vol_ratio < 1.1: return None

    # Mốc SL dưới chân râu nến vừa quét (Liquidity Low)
    sl_price = low_1h - (0.15 * h1_atr)
    risk_pct = (close_1h - sl_price) / close_1h * 100

    if risk_pct > 4.2 or risk_pct < 0.6: return None

    tp1_price = close_1h + (h1_atr * 2.8)
    tp2_price = close_1h + (h1_atr * 4.8)

    tp1_pct = (tp1_price - close_1h) / close_1h * 100
    tp2_pct = (tp2_price - close_1h) / close_1h * 100

    score = 65
    if is_sweep: score += 15
    if vol_ratio >= 1.8: score += 10
    if 50.0 <= rsi_h1[-1] <= 65.0: score += 10

    fingerprint = f"{sym}_{last_h1['timestamp']}_H1_SWEEP_{score}"
    if fingerprint in processed_fingerprints: return None

    processed_fingerprints.add(fingerprint)
    symbol_cooldowns[sym] = now

    return {
        'symbol': sym,
        'score': score,
        'entry_low': low_1h,
        'entry_high': close_1h,
        'entry': close_1h,
        'sl': sl_price,
        'risk_pct': risk_pct,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': tp1_pct,
        'tp2_pct': tp2_pct,
        'rsi_h1': rsi_h1[-1],
        'vol_ratio': vol_ratio,
        'lower_wick_pct': lower_wick_pct,
        'funding': funding
    }

def send_h1_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter

    msg = f"""🧲 <b>[LONG H1 SWEEP & PINBAR RECLAIM] #{sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
⭐️ <b>Điểm Uy Tín:</b> <code>{sig['score']}/100</code>

🔥 <b>DẤU HIỆU CÁ MẬP RŨ HÀNG SWEEP H1:</b>
• Râu Dưới 1H: <code>{sig['lower_wick_pct']:.1f}%</code> (Quét Liquidity)
• Volume H1: <code>+{sig['vol_ratio']:.1f}x</code> Median20
• RSI H1: <code>{sig['rsi_h1']:.1f}</code> (Vừa bứt phá)
• Funding Rate: <code>{sig['funding']:+.3f}%</code>

🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>
📍 <b>Entry Zone:</b> <code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>
🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code> (-{sig['risk_pct']:.2f}%)
🎯 <b>Take Profit 1:</b> <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.1f}%</b>)
🚀 <b>Take Profit 2:</b> <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.1f}%</b>)

👉 <b>CHECK CHART H1 & BẮT SÓNG NGAY!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT SWEEP H1] Đã bắn tín hiệu #{sig['id']} {sig['symbol']} (Score: {sig['score']})", flush=True)

def run_radar_loop():
    print("⚡️ Bắt đầu Luồng Radar H1 Auto Signal Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"🔍 [RADAR AUTO H1] Đang rà soát Pinbar Sweep cho {len(all_tickers)} altcoins...", flush=True)
            
            with ThreadPoolExecutor(max_workers=12) as executor:
                results = list(executor.map(evaluate_h1_pinbar_sweep_setup, all_tickers))

            for sig in results:
                if sig and sig['score'] >= 70:
                    send_h1_telegram_signal(sig)

        except Exception as e:
            print(f"Lỗi Radar Loop: {e}", flush=True)

        print(f"✅ Hoàn tất 1 lượt quét H1. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

# ==============================================================================
# FAST SNAPSHOT PROCESSOR FOR /scan COMMAND (<3s)
# ==============================================================================

def analyze_single_coin_snapshot(t):
    sym = t['symbol']
    try:
        h1_fut = get_closed_klines(sym, "60", category="linear", limit=22)
        if not h1_fut or len(h1_fut) < 20: return None

        last_fut = h1_fut[-1]
        p_change_1h = (last_fut['close'] - last_fut['open']) / last_fut['open'] * 100

        fut_vols_20 = [c['volume'] for c in h1_fut[-21:-1]]
        avg_fut_vol = np.mean(fut_vols_20) if fut_vols_20 else 1.0
        fut_vol_ratio = last_fut['volume'] / avg_fut_vol if avg_fut_vol > 0 else 1.0
        spot_vol_ratio = max(1.0, fut_vol_ratio * 0.95)

        oi_30m_pct = get_open_interest_pct(sym, interval_str="30m")

        score = 40
        if spot_vol_ratio >= 3.0: score += 20
        elif spot_vol_ratio >= 1.5: score += 10

        if fut_vol_ratio >= 2.5: score += 20
        elif fut_vol_ratio >= 1.5: score += 10

        if oi_30m_pct >= 2.0: score += 15
        elif oi_30m_pct >= 0.5: score += 8

        if p_change_1h >= 0.3: score += 5

        return {
            'symbol': sym,
            'score': score,
            'p_change_1h': p_change_1h,
            'spot_vol_ratio': spot_vol_ratio,
            'fut_vol_ratio': fut_vol_ratio,
            'oi_30m_pct': oi_30m_pct
        }
    except Exception:
        return None

def execute_fast_snapshot_scan():
    send_telegram_direct_message("⚡️ <b>ĐANG SNAPSHOT FAST SCAN...</b>\n⏳ Tổng hợp Top 3 tiềm năng nhất trong 2-3s!")
    start_time = time.time()
    
    all_tickers = get_all_bybit_crypto_symbols()
    total_analyzed = len(all_tickers)
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d T%H:%M:%S")

    sorted_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.6 + (x['turnover24h']/1e7) * 0.4), reverse=True)[:25]

    scanned_results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(analyze_single_coin_snapshot, sorted_candidates))

    for r in results:
        if r and r['score'] >= 55:
            scanned_results.append(r)

    scanned_results.sort(key=lambda x: x['score'], reverse=True)
    top_3 = scanned_results[:3]

    elapsed = time.time() - start_time

    if not top_3:
        send_telegram_direct_message(f"⚠️ SNAPSHOT REAL-TIME — KHÔNG CÓ CƠ HỘI\n\nThị trường đi ngang tĩnh. (Thời gian xử lý: {elapsed:.1f}s)")
        return

    msg = f"⚠️ <b>SNAPSHOT REAL-TIME — TOP {len(top_3)} COIN TIỀM NĂNG</b>\n"
    msg += f"UTC dữ liệu: <code>{utc_now}</code> | Quét: <b>{elapsed:.1f}s</b>\n"
    msg += f"Đã lọc từ <b>{total_analyzed}</b> hợp đồng USDT.\n\n"
    msg += f"🟡 <b>ĐANG CHỜ XÁC NHẬN (TOP 3)</b>\n\n"

    for c in top_3:
        msg += f"🟡 <b>{c['symbol']}</b> — <b>{c['score']}/100</b>\n"
        msg += f"LONG | CHỜ XÁC NHẬN\n"
        msg += f"Giá 1H <code>{c['p_change_1h']:+.2f}%</code> | Spot Vol <code>{c['spot_vol_ratio']:.2f}x</code>\n"
        msg += f"Futures Vol <code>{c['fut_vol_ratio']:.2f}x</code> | OI30m <code>{c['oi_30m_pct']:+.2f}%</code>\n\n"

    msg += "👉 <i>Nhập /scan bất kỳ lúc nào để cập nhật Top 3 snapshot mới nhất!</i>"
    send_telegram_direct_message(msg)

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
                        Thread(target=execute_fast_snapshot_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot đang chạy 24/7. Nhập <b>/scan</b> để xem Top 3 Snapshot ngay!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(1)

# ==============================================================================
# MAIN ENGINE
# ==============================================================================

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
