import time
import requests
import os
import numpy as np
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - H1 STRATEGY + 5M SCANNER RADAR
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 10_000_000  # $10M USD Minimum Turnover

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                
processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot H1 Trend-Flow + 5M Manual Scanner Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def get_current_price(symbol):
    try:
        url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0]["lastPrice"])
    except Exception:
        pass
    return None

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
# 1. API HELPERS (PURE CRYPTO ONLY)
# ==============================================================================

def get_all_bybit_crypto_symbols():
    """Lọc chỉ các mã Crypto USDT Perp trên Bybit (Loại bỏ Cổ phiếu, Vàng, Chỉ số, Token đòn bẩy)"""
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

def get_taker_ratios(symbol, category="linear", interval_str="1h", limit=5):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period={interval_str}&limit={limit}"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def get_open_interest_pct(symbol, interval_str="1h"):
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
# 2. H1 STRATEGY ENGINE (CHỦ ĐỘNG QUÉT TỰ ĐỘNG NHƯ CŨ)
# ==============================================================================

def check_h4_hard_gate(h4_candles):
    if len(h4_candles) < 50: return False
    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if not ema34_h4 or not ema89_h4: return False

    curr_close = h4_candles[-1]['close']
    if not (curr_close > ema34_h4[-1] and ema34_h4[-1] >= ema89_h4[-1]):
        return False
    return True

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

    if not check_h4_hard_gate(h4_candles): return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_atr = calculate_atr(h1_candles, 14)
    ema34_h1 = calculate_ema(h1_candles, 34)
    rsi_h1 = calculate_rsi(h1_candles, 14)

    if not ema34_h1 or not rsi_h1 or h1_atr <= 0: return None

    # Điều kiện H1 Reclaim / Trend Support
    is_h1_reclaim = (curr_price >= ema34_h1[-1] * 0.998) and (rsi_h1[-1] >= 50.0)
    if not is_h1_reclaim: return None

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
        'rsi_h1': rsi_h1[-1],
        'funding': funding
    }

# ==============================================================================
# 3. MANUAL /scan RADAR (CHỈ QUÉT COIN ĐỂ BẠN TỰ CHECK CHART)
# ==============================================================================

def execute_manual_5m_scan():
    send_telegram_direct_message("🔍 <b>ĐANG SCAN 5M REAL-TIME TOÀN BỘ ALTCOIN...</b>\n⏳ Vui lòng chờ 10-15 giây để lọc coin biến động mạnh & dòng tiền dồn!")
    
    all_tickers = get_all_bybit_crypto_symbols()
    
    # 1. Tính Relative Strength (RS)
    ticker_5m_changes = {}
    for t in all_tickers[:120]:
        klines = get_closed_klines(t['symbol'], "5", limit=3)
        if klines and len(klines) >= 2:
            pct = (klines[-1]['close'] - klines[-1]['open']) / klines[-1]['open'] * 100
            ticker_5m_changes[t['symbol']] = pct

    if not ticker_5m_changes:
        send_telegram_direct_message("⚠️ Không thể lấy dữ liệu 5M. Vui lòng thử lại sau!")
        return

    sorted_symbols = sorted(ticker_5m_changes.items(), key=lambda x: x[1], reverse=True)
    total_count = len(sorted_symbols)
    rs_map = {sym: ((idx + 1) / total_count) * 100 for idx, (sym, val) in enumerate(sorted_symbols)}

    matched_candidates = []

    for t in all_tickers:
        try:
            sym = t['symbol']
            m5_klines = get_closed_klines(sym, "5", limit=25)
            if not m5_klines or len(m5_klines) < 21: continue

            last_5m = m5_klines[-1]
            close_5m = last_5m['close']
            open_5m = last_5m['open']
            high_5m = last_5m['high']
            low_5m = last_5m['low']

            change_5m = (close_5m - open_5m) / open_5m * 100
            
            # Volume 5M / SMA20
            vols_20 = [c['volume'] for c in m5_klines[-21:-1]]
            avg_vol_20 = np.mean(vols_20) if vols_20 else 1.0
            vol_expansion = last_5m['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0

            if vol_expansion < 1.8 and change_5m < 1.0: continue

            taker_ratios = get_taker_ratios(sym, category="spot", interval_str="5m", limit=3)
            taker_buy_pct = taker_ratios[0] if taker_ratios else 50.0

            if taker_buy_pct < 58.0: continue

            oi_change_5m = get_open_interest_pct(sym, interval_str="5m")
            rs_rank_pct = rs_map.get(sym, 50.0)

            # Rút chân (Sweep)
            total_range = high_5m - low_5m
            lower_wick = (min(open_5m, close_5m) - low_5m)
            is_sweep = (lower_wick / total_range * 100 >= 38.0) if total_range > 0 else False

            # Tính Flow Score
            score = 40
            if vol_expansion >= 3.0: score += 20
            elif vol_expansion >= 2.0: score += 12

            if oi_change_5m >= 2.0: score += 15
            elif oi_change_5m >= 0.8: score += 10

            if taker_buy_pct >= 70.0: score += 15
            elif taker_buy_pct >= 60.0: score += 8

            if rs_rank_pct <= 10.0: score += 10

            if score >= 70:
                matched_candidates.append({
                    'symbol': sym,
                    'score': score,
                    'change_5m': change_5m,
                    'vol_expansion': vol_expansion,
                    'oi_change_5m': oi_change_5m,
                    'taker_buy_pct': taker_buy_pct,
                    'rs_rank_pct': rs_rank_pct,
                    'is_sweep': is_sweep,
                    'price': close_5m
                })

        except Exception:
            pass
        time.sleep(0.02)

    if not matched_candidates:
        send_telegram_direct_message("📊 <b>KẾT QUẢ /SCAN 5M:</b>\n\nThị trường tĩnh. Chưa thấy Altcoin nào có dòng tiền nổ đà bất thường trong 5M.")
        return

    matched_candidates.sort(key=lambda x: x['score'], reverse=True)
    top_list = matched_candidates[:5]

    msg = f"🚀 <b>KẾT QUẢ SCAN 5M REAL-TIME — TOP {len(top_list)} COIN BÙNG NỔ</b>\n"
    msg += "👉 <i>Danh sách coin có dòng tiền dồn mạnh để bạn TỰ CHECK CHART:</i>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for c in top_list:
        sweep_str = "YES" if c['is_sweep'] else "NO"
        msg += f"🔥 <b>#{c['symbol']}</b> (Score: <code>{c['score']}/100</code>)\n"
        msg += f"• ⚡️ 5M: <code>{c['change_5m']:+.2f}%</code> | Price: <code>{c['price']:.4f}</code>\n"
        msg += f"• 📊 VOL 5M: <code>+{c['vol_expansion']:.1f}x</code> | OI 5M: <code>{c['oi_change_5m']:+.1f}%</code>\n"
        msg += f"• 🟢 TAKER BUY: <code>{c['taker_buy_pct']:.0f}%</code> | RS: <code>TOP {c['rs_rank_pct']:.1f}%</code>\n"
        msg += f"• 🧲 SWEEP: <code>{sweep_str}</code>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"

    msg += "👀 <b>CHECK CHART H1 NGAY ĐỂ TÌM GIÁ VÀO ĐẸP!</b>"
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
                        Thread(target=execute_manual_5m_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H1 Strategy + 5M Manual Scanner đang chạy 24/7. Nhập <b>/scan</b> để lọc coin tự check!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# TELEGRAM ALERT FORMATTER (BẮN TÍN HIỆU TỰ ĐỘNG CỦA STRATEGY H1)
# ==============================================================================

def send_h1_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""🟢 <b>[H1 TREND RECLAIM LONG] #{sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
🟢 <b>Setup: H1 Trend-Flow Reclaim</b>

📍 <b>Entry Zone:</b>
<code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code> (-{sig['risk_pct']:.2f}%)

🎯 <b>Take Profit 1:</b> <code>{sig['tp1']:.4f}</code> (+{sig['tp1_pct']:.1f}%)
🚀 <b>Take Profit 2:</b> <code>{sig['tp2']:.4f}</code> (+{sig['tp2_pct']:.1f}%)

📊 <b>Metrics:</b>
• RSI H1: <code>{sig['rsi_h1']:.1f}</code>
• Funding: <code>{sig['funding']:+.3f}%</code>

⚠️ <i>Dữ liệu thử nghiệm PAPER. Kiểm tra Chart H1 trước khi giao dịch.</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT AUTOMATIC H1] Đã bắn tín hiệu #{sig['id']} {sig['symbol']}", flush=True)

# ==============================================================================
# MAIN RADAR LOOP (RÀ SOÁT TỰ ĐỘNG STRATEGY H1)
# ==============================================================================

def run_radar_loop():
    print("⚡️ Bắt đầu Luồng Radar H1 Strategy Automatic Scanner...", flush=True)
    while True:
        try:
            all_tickers = get_all_bybit_crypto_symbols()
            print(f"🔍 [H1 SCANNER] Đang rà soát Strategy H1 cho {len(all_tickers)} altcoins...", flush=True)
            
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

        print(f"✅ Hoàn tất 1 lượt quét H1. Tạm nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
