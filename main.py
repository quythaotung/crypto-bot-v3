import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - CRYPTO PURE ENGINE WITH REAL-TIME /scan COMMAND
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 10_000_000  # $10M USD 24h Crypto Turnover
MIN_ALERT_SCORE = 80               # Chỉ bắn Telegram tự động từ 80 điểm trở lên
MIN_TP1_PERCENT = 3.5              # TP1 tối thiểu từ +3.5% trở lên
MIN_CVD_PERCENT = 60.0             # Lực mua CVD tối thiểu từ 60%

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Crypto Pure H1 Squeeze Explosion with /scan Command Active 24/7!"

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

def get_all_bybit_crypto_symbols():
    """
    Chỉ lấy các cặp Crypto USDT Perp.
    Loại bỏ tuyệt đối: Cổ phiếu, Vàng (XAU), Chỉ số, Kim loại quý, Hàng hóa.
    """
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
        print(f"Lỗi lấy Tickers từ Bybit: {e}", flush=True)
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

def get_taker_ratios(symbol, category="linear", limit=5):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period=1h&limit={limit}"
    try:
        res = requests.get(url, timeout=8).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return [float(x.get("buyRatio", 0.5)) * 100 for x in res["result"]["list"]]
    except Exception:
        pass
    return [50.0] * limit

def calculate_atr(candles, period=14):
    if len(candles) < period + 1: return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low = candles[i]['low']
        prev_close = candles[i-1]['close']
        tr_list.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0

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

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# EVALUATE HIGH-PROFIT & HIGH-MOMENTUM SQUEEZE SETUP
# ==============================================================================
def evaluate_squeeze_explosion_setup(ticker_data, is_manual_scan=False):
    symbol = ticker_data['symbol']
    fut_turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    if fut_turnover < MIN_FUTURES_TURNOVER_24H: return None
    if funding > 0.06 or funding < -0.06: return None

    now = time.time()
    if not is_manual_scan and symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 21600:
        return None

    h1_candles = get_closed_klines(symbol, "60", category="linear", limit=60)
    if not h1_candles or len(h1_candles) < 30: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']
    h1_atr = calculate_atr(h1_candles, 14)
    if h1_atr <= 0: return None

    spot_taker = get_taker_ratios(symbol, category="spot", limit=5)
    cvd_buy_pct = spot_taker[0] if spot_taker else 50.0

    if cvd_buy_pct < MIN_CVD_PERCENT: return None

    oi_h1_pct = get_open_interest_pct(symbol, "1h")
    if oi_h1_pct < 1.0: return None

    avg_vol_20 = calculate_sma_volume(h1_candles[:-1], 20)
    vvol_ratio = last_h1['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if vvol_ratio < 1.3: return None

    recent_lows = [c['low'] for c in h1_candles[-12:-1]]
    recent_highs = [c['high'] for c in h1_candles[-12:-1]]
    base_low = min(recent_lows)
    base_high = max(recent_highs)
    base_range_pct = (base_high - base_low) / base_low * 100

    if base_range_pct > 6.0: return None

    is_breakout = last_h1['close'] > base_high
    is_holding = (curr_price >= base_low + (base_high - base_low)*0.35) and last_h1['close'] > last_h1['open']

    if not (is_breakout or is_holding): return None

    sl_price = base_low - (0.15 * h1_atr)
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 3.5: return None

    tp1_price = max(curr_price + (h1_atr * 3.2), curr_price * (1 + MIN_TP1_PERCENT / 100))
    tp2_price = tp1_price + (h1_atr * 4.5)

    tp1_pct = (tp1_price - curr_price) / curr_price * 100
    tp2_pct = (tp2_price - curr_price) / curr_price * 100

    if tp1_pct < MIN_TP1_PERCENT: return None

    score = 40

    if vvol_ratio >= 2.5: score += 20
    elif vvol_ratio >= 2.0: score += 16
    elif vvol_ratio >= 1.3: score += 12

    if oi_h1_pct >= 5.0: score += 20
    elif oi_h1_pct >= 2.5: score += 15
    elif oi_h1_pct >= 1.0: score += 10

    if cvd_buy_pct >= 80.0: score += 20
    elif cvd_buy_pct >= 70.0: score += 16
    elif cvd_buy_pct >= 60.0: score += 12

    if is_breakout: score += 10

    # Đối với lệnh tự động, bắt buộc Score >= 80; Đối với /scan thủ công, lấy từ Score >= 75
    min_threshold = MIN_ALERT_SCORE if not is_manual_scan else 75
    if score < min_threshold: return None

    entry_zone_low = base_high if is_breakout else base_low
    entry_zone_high = curr_price

    fingerprint = f"{symbol}_{last_h1['timestamp']}_CRYPTO_SQUEEZE_{score}"
    if not is_manual_scan:
        if fingerprint in processed_fingerprints: return None
        processed_fingerprints.add(fingerprint)
        symbol_cooldowns[symbol] = now

    return {
        'symbol': symbol,
        'score': score,
        'vvol_ratio': vvol_ratio,
        'oi_pct': oi_h1_pct,
        'cvd_buy': cvd_buy_pct,
        'status_text': "Breakout Nền Tích Lũy" if is_breakout else "Test Nền Tích Lũy Holding",
        'entry_low': entry_zone_low,
        'entry_high': entry_zone_high,
        'entry': curr_price,
        'sl': sl_price,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': tp1_pct,
        'tp2_pct': tp2_pct
    }

# ==============================================================================
# MANUAL SCANNER PROCESSOR (/scan COMMAND)
# ==============================================================================
def execute_manual_scan():
    send_telegram_direct_message("🔍 <b>ĐANG SCAN REAL-TIME TOÀN BỘ THỊ TRƯỜNG CRYPTO...</b>\n⏳ Vui lòng chờ trong 15-30 giây để lọc ra các mã bùng nổ tốt nhất!")
    
    all_crypto_tickers = get_all_bybit_crypto_symbols()
    matched_coins = []

    for ticker in all_crypto_tickers:
        try:
            sig = evaluate_squeeze_explosion_setup(ticker, is_manual_scan=True)
            if sig:
                matched_coins.append(sig)
        except Exception:
            pass
        time.sleep(0.03)

    if not matched_coins:
        send_telegram_direct_message("📊 <b>KẾT QUẢ /SCAN REAL-TIME:</b>\n\nChưa có coin Crypto nào đáp ứng đủ tiêu chí bùng nổ (Score ≥ 75, TP1 ≥ +3.5%, Volume/CVD mạnh) tại thời điểm này.")
        return

    # Sắp xếp theo điểm Uy Tín giảm dần
    matched_coins.sort(key=lambda x: x['score'], reverse=True)
    top_candidates = matched_coins[:5]

    msg = f"🚀 <b>KẾT QUẢ SCAN REAL-TIME — TOP {len(top_candidates)} COIN BÙNG NỔ</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for sig in top_candidates:
        msg += f"🔥 <b>#{sig['symbol']}</b> (Điểm: <code>{sig['score']}/100</code>)\n"
        msg += f"• Volume H1: <code>+{sig['vvol_ratio']:.1f}x</code> | OI: <code>{sig['oi_pct']:+.1f}%</code> | CVD Buy: <code>{sig['cvd_buy']:.0f}%</code>\n"
        msg += f"📍 Entry Zone: <code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>\n"
        msg += f"🛑 Stop Loss: <code>{sig['sl']:.4f}</code>\n"
        msg += f"🎯 TP1: <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.1f}%</b>) | TP2: <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.1f}%</b>)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"

    msg += "👉 <i>Gửi tín hiệu chi tiết tự động khi có biến động nổ đà sóng tăng!</i>"
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
                        Thread(target=execute_manual_scan, daemon=True).start()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Crypto Squeeze Scanner đang chạy 24/7. Nhập <b>/scan</b> để tìm coin ngay!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# TELEGRAM ALERT FORMATTER (TỰ ĐỘNG BẮN SIGNAL)
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""🚀 <b>[LONG H1 SQUEEZE EXPLOSION] #{sig['symbol']}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín</b>: <code>{sig['score']}/100</code>

🔥 <b>DẤU HIỆU CÁ MẬP GOM SPOT & SHORT SQUEEZE</b>:
• Volume H1: <code>+{sig['vvol_ratio']:.1f}x</code> (Dốc đứng)
• Open Interest (OI): <code>{sig['oi_pct']:+.1f}%</code> (Tiền bơm cực mạnh)
• Lực Mua CVD: <code>{sig['cvd_buy']:.0f}%</code> (Taker Buy ép đỉnh)
• Trạng Thái: <code>{sig['status_text']}</code>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG)</b>:

📍 <b>Entry Zone</b>:
<code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss (Cắt lỗ)</b>:
<code>{sig['sl']:.4f}</code> (Thủng chân nến gom H1)

🎯 <b>Take Profit 1 (Chốt 1)</b>:
<code>{sig['tp1']:.4f}</code> <code>(+{sig['tp1_pct']:.1f}%)</code> (Đỉnh cũ cản H1)

🚀 <b>Take Profit 2 (Chốt 2)</b>:
<code>{sig['tp2']:.4f}</code> <code>(+{sig['tp2_pct']:.1f}%)</code> (Vùng nổ đà sóng tăng)
-------------------------------------
👉 <b>CHECK CHART H1 & BẮT SÓNG NGAY!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT TELEGRAM CRYPTO] Đã bắn tín hiệu #{sig['id']} {sig['symbol']} (Score: {sig['score']})", flush=True)

# ==============================================================================
# MAIN RADAR LOOP
# ==============================================================================
def run_radar_loop():
    print(f"⚡️ Bắt đầu Radar Crypto Squeeze Explosion (TP1 >= {MIN_TP1_PERCENT}% & CVD >= {MIN_CVD_PERCENT}%)...", flush=True)
    while True:
        try:
            all_crypto_tickers = get_all_bybit_crypto_symbols()
            print(f"🔍 [CRYPTO SCANNER] Đang rà soát {len(all_crypto_tickers)} coin Crypto...", flush=True)
            for ticker in all_crypto_tickers:
                try:
                    sig = evaluate_squeeze_explosion_setup(ticker, is_manual_scan=False)
                    if sig:
                        send_telegram_signal(sig)
                except Exception:
                    pass
                time.sleep(0.05)
        except Exception as e:
            print(f"Lỗi luồng Radar Loop: {e}", flush=True)
        
        print(f"✅ Hoàn tất 1 lượt quét. Tạm nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
