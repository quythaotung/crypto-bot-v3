import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 10_000_000  # $10M USD Minimum Turnover

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot H1 Strategy + Top 3 Snapshot Scanner Active 24/7!"

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
# API HELPERS
# ==============================================================================

def get_all_bybit_crypto_symbols():
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
            closed_candles = raw_list[1:]
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

def get_open_interest_pct(symbol, interval_str="30m"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
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
# H1 STRATEGY AUTOMATIC EVALUATOR
# ==============================================================================

def evaluate_h1_trend_strategy(ticker_data):
    symbol = ticker_data['symbol']
    turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    if turnover < MIN_FUTURES_TURNOVER_24H or funding > 0.05 or funding < -0.05:
        return None

    now = time.time()
    if symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 14400:
        return None

    h4_candles = get_closed_klines(symbol, "240", limit=60)
    h1_candles = get_closed_klines(symbol, "60", limit=60)
    if not h4_candles or not h1_candles or len(h1_candles) < 40:
        return None

    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if not ema34_h4 or not ema89_h4: return None
    if not (h4_candles[-1]['close'] > ema34_h4[-1] and ema34_h4[-1] >= ema89_h4[-1]):
        return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_atr = calculate_atr(h1_candles, 14)
    ema34_h1 = calculate_ema(h1_candles, 34)

    if not ema34_h1 or h1_atr <= 0: return None

    if curr_price < ema34_h1[-1] * 0.998: return None

    recent_lows = [c['low'] for c in h1_candles[-6:]]
    support_low = min(recent_lows)
    sl_price = support_low - (0.2 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 3.5: return None

    tp1_price = curr_price + (h1_atr * 3.0)
    tp2_price = curr_price + (h1_atr * 4.8)

    fingerprint = f"{symbol}_{last_h1['timestamp']}_H1_TREND"
    if fingerprint in processed_fingerprints: return None

    processed_fingerprints.add(fingerprint)
    symbol_cooldowns[symbol] = now

    return {
        'symbol': symbol,
        'entry_low': support_low,
        'entry_high': curr_price,
        'entry': curr_price,
        'sl': sl_price,
        'risk_pct': risk_pct,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': (tp1_price - curr_price) / curr_price * 100,
        'tp2_pct': (tp2_price - curr_price) / curr_price * 100,
        'funding': funding
    }

# ==============================================================================
# MANUAL /scan SNAPSHOT PROCESSOR (ĐỦ CHUẨN ĐỊNH DẠNG SNAPSHOT TOP 3)
# ==============================================================================

def execute_manual_snapshot_scan():
    send_telegram_direct_message("🔍 <b>ĐANG TẠO SNAPSHOT SCAN REAL-TIME...</b>\n⏳ Vui lòng chờ 10-15 giây để tổng hợp dữ liệu Top 3 coin tiềm năng nhất!")
    
    all_tickers = get_all_bybit_crypto_symbols()
    total_analyzed = len(all_tickers)
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d T%H:%M:%S")

    scanned_results = []
    xang_count = 0
    cho_count = 0

    for t in all_tickers:
        try:
            sym = t['symbol']
            h1_fut = get_closed_klines(sym, "60", category="linear", limit=25)
            h1_spot = get_closed_klines(sym, "60", category="spot", limit=25)

            if not h1_fut or len(h1_fut) < 21: continue

            last_fut = h1_fut[-1]
            p_change_1h = (last_fut['close'] - last_fut['open']) / last_fut['open'] * 100

            # Futures Volume Ratio
            fut_vols_20 = [c['volume'] for c in h1_fut[-21:-1]]
            avg_fut_vol = np.mean(fut_vols_20) if fut_vols_20 else 1.0
            fut_vol_ratio = last_fut['volume'] / avg_fut_vol if avg_fut_vol > 0 else 1.0

            # Spot Volume Ratio
            spot_vol_ratio = 1.0
            if h1_spot and len(h1_spot) >= 21:
                spot_vols_20 = [c['volume'] for c in h1_spot[-21:-1]]
                avg_spot_vol = np.mean(spot_vols_20) if spot_vols_20 else 1.0
                spot_vol_ratio = h1_spot[-1]['volume'] / avg_spot_vol if avg_spot_vol > 0 else 1.0

            # OI 30m Change
            oi_30m_pct = get_open_interest_pct(sym, interval_str="30m")

            # Chấm điểm Score 0 - 100
            score = 40
            if spot_vol_ratio >= 3.0: score += 20
            elif spot_vol_ratio >= 1.5: score += 10

            if fut_vol_ratio >= 2.5: score += 20
            elif fut_vol_ratio >= 1.5: score += 10

            if oi_30m_pct >= 2.0: score += 15
            elif oi_30m_pct >= 0.5: score += 8

            if p_change_1h >= 0.3: score += 5

            if score >= 80:
                xang_count += 1
            elif score >= 65:
                cho_count += 1

            if score >= 70:
                scanned_results.append({
                    'symbol': sym,
                    'score': score,
                    'p_change_1h': p_change_1h,
                    'spot_vol_ratio': spot_vol_ratio,
                    'fut_vol_ratio': fut_vol_ratio,
                    'oi_30m_pct': oi_30m_pct
                })

        except Exception:
            pass
        time.sleep(0.02)

    loai_count = max(0, total_analyzed - xang_count - cho_count)

    if not scanned_results:
        send_telegram_direct_message("⚠️ SNAPSHOT REAL-TIME — KHÔNG CÓ CƠ HỘI\n\nThị trường đi ngang tĩnh. Chưa có coin nào đủ tiêu chí tích lũy bùng nổ.")
        return

    # Sắp xếp và lấy chính xác TOP 3
    scanned_results.sort(key=lambda x: x['score'], reverse=True)
    top_3 = scanned_results[:3]

    msg = f"⚠️ <b>SNAPSHOT REAL-TIME — TOP {len(top_3)} COIN TIỀM NĂNG</b>\n"
    msg += f"UTC dữ liệu: <code>{utc_now}</code>\n"
    msg += f"Đã phân tích <b>{total_analyzed}</b> hợp đồng USDT.\n\n"
    msg += f"🟢 XĂNG: {xang_count} | 🟡 CHỜ: {cho_count} | ⚪️ LOẠI: {loai_count}\n\n"
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
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10"
            res = requests.get(url, timeout=12).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip().lower()
                    
                    if text in ["/scan", "scan"]:
                        Thread(target=execute_manual_snapshot_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H1 Strategy + Top 3 Snapshot Scanner đang hoạt động. Nhập <b>/scan</b> để xem Top 3!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# TELEGRAM ALERT FORMATTER (BẮN TÍN HIỆU LONG TỰ ĐỘNG CỦA BOT)
# ==============================================================================

def send_h1_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter

    msg = f"""🟢 <b>[H1 TREND RECLAIM LONG] #{sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
🟢 <b>Setup: H1 Trend-Flow Reclaim</b>

📍 <b>Entry Zone:</b>
<code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code> (-{sig['risk_pct']:.2f}%)

🎯 <b>Take Profit 1:</b> <code>{sig['tp1']:.4f}</code> (+{sig['tp1_pct']:.1f}%)
🚀 <b>Take Profit 2:</b> <code>{sig['tp2']:.4f}</code> (+{sig['tp2_pct']:.1f}%)

📊 <b>Funding:</b> <code>{sig['funding']:+.3f}%</code>
⚠️️ <i>Kế hoạch tự động. Kiểm tra chart H1 trước khi vào lệnh.</i>"""

    send_telegram_direct_message(msg)

# ==============================================================================
# MAIN RADAR LOOP (RÀ SOÁT AUTOMATIC CÁC LỆNH H1)
# ==============================================================================

def run_radar_loop():
    print("⚡️ Bắt đầu Luồng Radar H1 Strategy Automatic Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            for ticker in all_tickers:
                try:
                    sig = evaluate_h1_trend_strategy(ticker)
                    if sig:
                        send_h1_telegram_signal(sig)
                except Exception:
                    pass
                time.sleep(0.04)
        except Exception as e:
            print(f"Lỗi Radar Loop: {e}", flush=True)

        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
