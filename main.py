import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - 5M MOMENTUM & H1 CRYPTO EXPLOSION ENGINE
# ==============================================================================
DEBUG_MODE = True                  
COOLDOWN_SECONDS = 15              
MIN_FUTURES_TURNOVER_24H = 10_000_000  # $10M USD 24h Crypto Turnover
MIN_ALERT_SCORE = 80               # Bắn Telegram tự động từ 80 điểm
MIN_TP1_PERCENT_5M = 2.0           # TP1 tối thiểu cho lướt sóng 5M (>= 2.0%)
MIN_CVD_PERCENT_5M = 60.0          # Lực mua CVD 5M tối thiểu >= 60%

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
processed_fingerprints = set()
symbol_cooldowns = {}              
signal_counter = 0

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Crypto 5M Momentum & H1 Squeeze Scanner Active 24/7!"

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
    Lọc duy nhất Crypto USDT Perp.
    Loại bỏ hoàn toàn: Cổ phiếu, Vàng (XAU), Chỉ số, Kim loại quý, Hàng hóa.
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

def get_taker_ratios_5m(symbol, category="linear", limit=5):
    url = f"{BASE_URL}/v5/market/account-ratio?category={category}&symbol={symbol}&period=5m&limit={limit}"
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

def get_open_interest_pct_5m(symbol):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=5m&limit=3"
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
# EVALUATE 5M MOMENTUM EXPLOSION (SÓNG ĐỘT BIẾN KHUNG 5 PHÚT)
# ==============================================================================
def evaluate_5m_momentum_setup(ticker_data, is_manual_scan=False):
    symbol = ticker_data['symbol']
    fut_turnover = ticker_data['turnover24h']
    funding = ticker_data['fundingRate']

    if fut_turnover < MIN_FUTURES_TURNOVER_24H: return None
    if funding > 0.06 or funding < -0.06: return None

    now = time.time()
    if not is_manual_scan and symbol in symbol_cooldowns and (now - symbol_cooldowns[symbol]) < 7200: # Cooldown 2h cho 5M
        return None

    # Lấy Klines 5M
    m5_candles = get_closed_klines(symbol, "5", category="linear", limit=40)
    if not m5_candles or len(m5_candles) < 25: return None

    last_5m = m5_candles[-1]
    curr_price = last_5m['close']
    m5_atr = calculate_atr(m5_candles, 14)
    if m5_atr <= 0: return None

    # 1. TÍNH BIẾN ĐỘNG GIÁ 5M
    change_5m_pct = (last_5m['close'] - last_5m['open']) / last_5m['open'] * 100
    if change_5m_pct < 0.8 or change_5m_pct > 5.0: return None # Bắt nến 5M tăng từ +0.8% đến +5.0%

    # 2. TÍNH TỈ LỆ VOLUME 5M
    avg_vol_20 = calculate_sma_volume(m5_candles[:-1], 20)
    vvol_ratio_5m = last_5m['volume'] / avg_vol_20 if avg_vol_20 > 0 else 1.0
    if vvol_ratio_5m < 1.8: return None # Volume 5M gấp ít nhất 1.8x SMA20

    # 3. LỰC MUA CVD 5M
    spot_taker_5m = get_taker_ratios_5m(symbol, category="spot", limit=3)
    cvd_buy_5m = spot_taker_5m[0] if spot_taker_5m else 50.0
    if cvd_buy_5m < MIN_CVD_PERCENT_5M: return None

    # 4. OPEN INTEREST 5M
    oi_5m_pct = get_open_interest_pct_5m(symbol)

    # ĐIỂM SỐ UY TÍN (0 - 100)
    score = 45
    if vvol_ratio_5m >= 3.5: score += 25
    elif vvol_ratio_5m >= 2.5: score += 20
    elif vvol_ratio_5m >= 1.8: score += 12

    if oi_5m_pct >= 2.0: score += 15
    elif oi_5m_pct >= 0.8: score += 10

    if cvd_buy_5m >= 75.0: score += 15
    elif cvd_buy_5m >= 60.0: score += 10

    # Tính SL & TP cho 5M
    sl_price = min(last_5m['low'], curr_price - (1.2 * m5_atr))
    risk_pct = (curr_price - sl_price) / curr_price * 100

    if risk_pct > 2.5: return None # Risk 5M tối đa 2.5%

    tp1_price = curr_price + (m5_atr * 2.2)
    tp2_price = curr_price + (m5_atr * 4.0)

    tp1_pct = (tp1_price - curr_price) / curr_price * 100
    tp2_pct = (tp2_price - curr_price) / curr_price * 100

    if tp1_pct < MIN_TP1_PERCENT_5M: return None

    fingerprint = f"{symbol}_{last_5m['timestamp']}_M5_{score}"
    if not is_manual_scan:
        if fingerprint in processed_fingerprints: return None
        processed_fingerprints.add(fingerprint)
        symbol_cooldowns[symbol] = now

    return {
        'symbol': symbol,
        'score': score,
        'change_5m': change_5m_pct,
        'vvol_ratio_5m': vvol_ratio_5m,
        'oi_5m_pct': oi_5m_pct,
        'cvd_buy_5m': cvd_buy_5m,
        'entry_low': last_5m['open'],
        'entry_high': curr_price,
        'entry': curr_price,
        'sl': sl_price,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp1_pct': tp1_pct,
        'tp2_pct': tp2_pct
    }

# ==============================================================================
# MANUAL SCANNER PROCESSOR (/scan COMMAND 5M + H1)
# ==============================================================================
def execute_manual_scan():
    send_telegram_direct_message("⚡️ <b>ĐANG SCAN BIẾN ĐỘNG 5 PHÚT & DÒNG TIỀN NỔ ĐÀ...</b>\n⏳ Vui lòng chờ 10-15 giây để lọc ra các mã Crypto xung lực mạnh nhất!")
    
    all_crypto_tickers = get_all_bybit_crypto_symbols()
    matched_coins = []

    for ticker in all_crypto_tickers:
        try:
            sig = evaluate_5m_momentum_setup(ticker, is_manual_scan=True)
            if sig:
                matched_coins.append(sig)
        except Exception:
            pass
        time.sleep(0.02)

    if not matched_coins:
        send_telegram_direct_message("📊 <b>KẾT QUẢ /SCAN REAL-TIME 5M:</b>\n\nThị trường đang nén tĩnh. Chưa có coin Crypto nào bùng nổ Vol/CVD đột biến trong 5 phút qua.")
        return

    matched_coins.sort(key=lambda x: x['score'], reverse=True)
    top_candidates = matched_coins[:5]

    msg = f"🔥 <b>KẾT QUẢ SCAN REAL-TIME — TOP {len(top_candidates)} COIN KHỎE 5M</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for sig in top_candidates:
        msg += f"⚡️ <b>#{sig['symbol']}</b> (Biến động 5M: <b>+{sig['change_5m']:.2f}%</b> | Điểm: <code>{sig['score']}/100</code>)\n"
        msg += f"• Vol 5M: <code>+{sig['vvol_ratio_5m']:.1f}x</code> | OI 5M: <code>{sig['oi_5m_pct']:+.1f}%</code> | CVD Buy: <code>{sig['cvd_buy_5m']:.0f}%</code>\n"
        msg += f"📍 Entry Zone: <code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>\n"
        msg += f"🛑 Stop Loss: <code>{sig['sl']:.4f}</code>\n"
        msg += f"🎯 TP1: <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.1f}%</b>) | TP2: <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.1f}%</b>)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"

    msg += "👉 <i>Nhấn /scan bất kỳ lúc nào để cập nhật sóng 5M mới nhất!</i>"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Crypto 5M Scanner đang chạy 24/7. Nhập <b>/scan</b> để tìm sóng ngay!")
        except Exception as e:
            print(f"Lỗi Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# TELEGRAM ALERT FORMATTER (TỰ ĐỘNG BẮN SIGNAL 5M)
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    signals_history.append(sig)

    msg = f"""⚡️ <b>[LONG 5M MOMENTUM EXPLOSION] #{sig['symbol']}</b>
-------------------------------------
⭐️️ <b>Điểm Xung Lực 5M</b>: <code>{sig['score']}/100</code>

🔥 <b>DẤU HIỆU DÒNG TIỀN ĐỘT BIẾN 5 PHÚT</b>:
• Nến 5M Tăng: <code>+{sig['change_5m']:.2f}%</code> (Nổ đà)
• Volume 5M: <code>+{sig['vvol_ratio_5m']:.1f}x</code> (Bùng nổ)
• OI 5M: <code>{sig['oi_5m_pct']:+.1f}%</code> (Bơm tiền khẩn cấp)
• Lực Mua CVD 5M: <code>{sig['cvd_buy_5m']:.0f}%</code> (Gom áp đảo)
-------------------------------------
🎯 <b>KẾ HOẠCH LƯỚT SÓNG NGÀY (5M)</b>:

📍 <b>Entry Zone</b>:
<code>{sig['entry_low']:.4f} – {sig['entry_high']:.4f}</code>

🛑 <b>Stop Loss (Cắt lỗ)</b>:
<code>{sig['sl']:.4f}</code> (Dưới chân nến 5M)

🎯 <b>Take Profit 1 (Chốt 1)</b>:
<code>{sig['tp1']:.4f}</code> <code>(+{sig['tp1_pct']:.1f}%)</code>

🚀 <b>Take Profit 2 (Chốt 2)</b>:
<code>{sig['tp2']:.4f}</code> <code>(+{sig['tp2_pct']:.1f}%)</code>
-------------------------------------
👉 <b>CHECK CHART 5M & BẮT XUNG LỰC NGAY!</b>"""

    send_telegram_direct_message(msg)
    print(f"✅ [ALERT 5M CRYPTO] Đã bắn tín hiệu #{sig['id']} {sig['symbol']} (Score: {sig['score']})", flush=True)

# ==============================================================================
# MAIN RADAR LOOP
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Crypto 5M Momentum Scanner...", flush=True)
    while True:
        try:
            all_crypto_tickers = get_all_bybit_crypto_symbols()
            print(f"🔍 [5M SCANNER] Đang rà soát {len(all_crypto_tickers)} coin Crypto...", flush=True)
            for ticker in all_crypto_tickers:
                try:
                    sig = evaluate_5m_momentum_setup(ticker, is_manual_scan=False)
                    if sig and sig['score'] >= MIN_ALERT_SCORE:
                        send_telegram_signal(sig)
                except Exception:
                    pass
                time.sleep(0.04)
        except Exception as e:
            print(f"Lỗi luồng Radar Loop: {e}", flush=True)
        
        print(f"✅ Hoàn tất 1 lượt quét 5M. Tạm nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    run_radar_loop()
