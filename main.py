import time
import requests
import os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
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
    return "Bot Fast Snapshot Scanner Active 24/7!"

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

def get_closed_klines(symbol, interval, category="linear", limit=25):
    url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=4).json()
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

# ==============================================================================
# FAST SINGLE COIN EVALUATOR (DÙNG CHO THREADING)
# ==============================================================================

def analyze_single_coin(t):
    sym = t['symbol']
    try:
        h1_fut = get_closed_klines(sym, "60", category="linear", limit=22)
        if not h1_fut or len(h1_fut) < 20: return None

        last_fut = h1_fut[-1]
        p_change_1h = (last_fut['close'] - last_fut['open']) / last_fut['open'] * 100

        # Futures Vol Ratio
        fut_vols_20 = [c['volume'] for c in h1_fut[-21:-1]]
        avg_fut_vol = np.mean(fut_vols_20) if fut_vols_20 else 1.0
        fut_vol_ratio = last_fut['volume'] / avg_fut_vol if avg_fut_vol > 0 else 1.0

        # Spot Vol Ratio
        h1_spot = get_closed_klines(sym, "60", category="spot", limit=22)
        spot_vol_ratio = 1.0
        if h1_spot and len(h1_spot) >= 20:
            spot_vols_20 = [c['volume'] for c in h1_spot[-21:-1]]
            avg_spot_vol = np.mean(spot_vols_20) if spot_vols_20 else 1.0
            spot_vol_ratio = h1_spot[-1]['volume'] / avg_spot_vol if avg_spot_vol > 0 else 1.0

        oi_30m_pct = get_open_interest_pct(sym, interval_str="30m")

        # Scoring
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

# ==============================================================================
# FAST SNAPSHOT PROCESSOR (< 3 GIÂY)
# ==============================================================================

def execute_fast_snapshot_scan():
    send_telegram_direct_message("⚡️ <b>ĐANG SNAPSHOT FAST SCAN...</b>\n⏳ Tổng hợp Top 3 tiềm năng nhất trong 2-3s!")
    start_time = time.time()
    
    all_tickers = get_all_bybit_crypto_symbols()
    total_analyzed = len(all_tickers)
    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d T%H:%M:%S")

    # BƯỚC 1: Lọc sơ bộ Top 20 coin có biến động & volume lớn nhất 24h
    sorted_candidates = sorted(all_tickers, key=lambda x: (x['price24hPcnt'] * 0.6 + (x['turnover24h']/1e7) * 0.4), reverse=True)[:20]

    # BƯỚC 2: Chạy đa luồng (ThreadPool) cho 20 coin này song song
    scanned_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(analyze_single_coin, sorted_candidates))

    for r in results:
        if r and r['score'] >= 65:
            scanned_results.append(r)

    scanned_results.sort(key=lambda x: x['score'], reverse=True)
    top_3 = scanned_results[:3]

    elapsed = time.time() - start_time

    if not top_3:
        send_telegram_direct_message(f"⚠️ SNAPSHOT REAL-TIME — KHÔNG CÓ CƠ HỘI\n\nThị trường đi ngang tĩnh. (Thời gian xử lý: {elapsed:.1f}s)")
        return

    msg = f"⚠️ <b>SNAPSHOT REAL-TIME — TOP {len(top_3)} COIN TIỀM NĂNG</b>\n"
    msg += f"UTC dữ liệu: <code>{utc_now}</code> | Quét: <code>{elapsed:.1f}s</code>\n"
    msg += f"Đã lọc sơ bộ từ <b>{total_analyzed}</b> hợp đồng USDT.\n\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Fast Snapshot Scanner đang chạy. Nhập <b>/scan</b> để xem Top 3 ngay!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(1)

# ==============================================================================
# MAIN ENGINE
# ==============================================================================

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    
    # Luồng giữ bot chạy liên tục
    while True:
        time.sleep(60)
