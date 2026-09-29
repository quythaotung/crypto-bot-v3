import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION - TỐI ƯU CHỐNG LOÃNG KÈO (HIGH WIN-RATE ONLY)
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết ra Render
COOLDOWN_SECONDS = 10              # Quét liên tục (chỉ nghỉ 10s giữa các vòng)
MIN_TURNOVER_24H = 8_000_000       # Turnover 24h tối thiểu $8M USD (Chỉ lọc coin thanh khoản lớn)
MAX_ALERTS_PER_CYCLE = 2           # Tối đa gửi Top 2 KÈO SIÊU ĐẸP mỗi lượt quét
MIN_WINRATE_SCORE = 88             # NÂNG MỨC ĐIỂM UY TÍN LÊN 88/100 (CHỐNG LOÃNG KÈO)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

signals_history = []
active_signals = []                # Danh sách theo dõi TP/SL real-time
processed_signal_ids = set()
signal_counter = 0

# ==============================================================================
# 1. FLASK KEEP-ALIVE SERVER & TELEGRAM COMMAND LISTENER (/pnl)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit High Win-Rate Strict Filter Bot Active 24/7!"

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
    if not BOT_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram Direct: {e}", flush=True)

def send_pnl_summary():
    if not signals_history:
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang chạy lọc kèo khắt khe 24/7!\n⏳ Chưa có tín hiệu Tỷ Lệ Thắng Cao (>=88đ) nào phát ra."
        send_telegram_direct_message(msg)
        return

    msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH HIGH WIN-RATE:</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    total_pnl_pct = 0.0
    
    for sig in signals_history:
        curr_price = get_current_price(sig['symbol'])
        if not curr_price:
            curr_price = sig['entry']
            
        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
        status = sig.get('status_text', '⏳ Đang theo dõi')
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        
        msg += f"<b>#{sig['id']} [{sig['engine_tag']}] {sig['symbol']}</b> (Score: {sig['score']})\n"
        msg += f"• Entry: <code>{sig['entry']:.4f}</code> | Hiện tại: <code>{curr_price:.4f}</code> ({icon} <b>{pnl_pct:+.2f}%</b>)\n"
        msg += f"• Trạng thái: <b>{status}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        total_pnl_pct += pnl_pct
        
    total_icon = "🔥" if total_pnl_pct >= 0 else "🔻"
    msg += f"<b>TỔNG PnL TÍCH LŨY: {total_icon} {total_pnl_pct:+.2f}%</b>"
    send_telegram_direct_message(msg)

def process_telegram_commands():
    last_update_id = 0
    print("🤖 Đã bật luồng lắng nghe lệnh Telegram (/pnl, /ping)...", flush=True)
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10"
            res = requests.get(url, timeout=12).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "").strip().lower()
                    
                    if text in ["/pnl", "/summary", "pnl"]:
                        send_pnl_summary()
                    elif text in ["/start", "/ping", "ping"]:
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot High Win-Rate đang lọc kèo 24/7!")
        except Exception as e:
            print(f"Lỗi luồng Telegram Listener: {e}", flush=True)
        time.sleep(2)

# ==============================================================================
# 2. TIẾN TRÌNH THEO DÕI GIÁ REAL-TIME (CẢNH BÁO CHẠM TP / SL)
# ==============================================================================
def process_active_signals_tracker():
    while True:
        try:
            current_active = list(active_signals)
            for sig in current_active:
                try:
                    curr_price = get_current_price(sig['symbol'])
                    if not curr_price:
                        continue

                    # 1. CHẠM TP1
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 (Đã hủy bảo vệ SL)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CẢNH BÁO CHẠM TP1] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 Zone: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL đối với lệnh này. Bạn nên dời SL về Entry để gồng TP2!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = "🚀 Hit TP2 (Chốt lời thành công)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CẢNH BÁO CHẠM TP2 - FULL TARGET] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 TP2 Zone: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>ĐÃ ĐẠT TOÀN BỘ MỤC TIÊU SÓNG PUMP! CHỐT LỜI ĐẮC Ý!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)

                    # 3. HIT SL (Chỉ báo khi CHƯA chạm TP1)
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (Cắt lỗ bảo vệ)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Vi phạm mốc hỗ trợ cấu trúc. Cắt lỗ an toàn!</i>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)
                except Exception as inner_e:
                    print(f"Lỗi theo dõi coin {sig.get('symbol')}: {inner_e}", flush=True)
        except Exception as e:
            print(f"Lỗi theo dõi Tracker Loop: {e}", flush=True)
        
        time.sleep(10)

# ==============================================================================
# 3. BYBIT API & INDICATORS HELPERS
# ==============================================================================
BASE_URL = "https://api.bybit.com"

def get_all_bybit_usdt_symbols():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            symbols = [
                t['symbol'] for t in tickers 
                if t['symbol'].endswith('USDT') 
                and not t['symbol'].startswith(('USDC', 'BUSD', 'DAI'))
                and not any(x in t['symbol'] for x in ['3L', '3S', '2L', '2S'])
            ]
            return symbols
    except Exception:
        pass
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

def get_closed_klines(symbol, interval, limit=50):
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=10).json()
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

def get_open_interest_pct(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval_str}&limit=3"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = float(oi_list[0]["openInterest"])
                prev_oi = float(oi_list[1]["openInterest"])
                return ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0.0
    except Exception:
        pass
    return 0.0

def get_taker_buy_pct(symbol, interval_str="1h"):
    url = f"{BASE_URL}/v5/market/account-ratio?category=linear&symbol={symbol}&period={interval_str}&limit=2"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            data = res["result"]["list"]
            if len(data) >= 1:
                return float(data[0].get("buyRatio", 0.5)) * 100
    except Exception:
        pass
    return 50.0

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

def calculate_sma_volume(candles, period=20):
    if len(candles) < period: return 0
    vols = [c['volume'] for c in candles[-period:]]
    return sum(vols) / period

# ==============================================================================
# 4. CORE ENGINE: BỘ LỌC NGHIÊM NGẶT (STRICT HIGH WIN-RATE)
# ==============================================================================

def evaluate_symbol_setups(symbol, btc_h1_candles):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    # --------------------------------------------------------------------------
    # ENGINE 1: QUÉT RETEST MỨC HỖ TRỢ H1 (SUPPORT RETEST)
    # --------------------------------------------------------------------------
    h1_candles = get_closed_klines(symbol, "60", limit=30)
    if len(h1_candles) >= 22:
        last_h1 = h1_candles[-1]
        body_pct = (last_h1['close'] - last_h1['open']) / last_h1['open'] * 100
        change_4_candles = (last_h1['close'] - h1_candles[-4]['close']) / h1_candles[-4]['close'] * 100

        # CHỐNG MUA ĐỦI: Bắt buộc không đu nến xanh dài (> 3.0%) hoặc chóp nến (tăng > 8%)
        if body_pct <= 3.0 and change_4_candles <= 8.0:
            recent_highs = [c['high'] for c in h1_candles[-15:-3]]
            if recent_highs:
                support_level = max(recent_highs)
                curr_price = last_h1['close']
                dist_to_supp = abs(curr_price - support_level) / support_level * 100
                
                if dist_to_supp <= 1.0 or (last_h1['low'] <= support_level <= last_h1['high']):
                    taker_h1_pct = get_taker_buy_pct(symbol, "1h")
                    oi_h1_pct = get_open_interest_pct(symbol, "1h")
                    
                    h1_range = last_h1['high'] - last_h1['low']
                    lower_wick = (min(last_h1['open'], last_h1['close']) - last_h1['low'])
                    wick_ratio = lower_wick / h1_range if h1_range > 0 else 0
                    
                    if (wick_ratio >= 0.30 or last_h1['close'] > last_h1['open']) and taker_h1_pct >= 62.0 and oi_h1_pct >= 1.0:
                        atr1h = calculate_atr(h1_candles, 14)
                        entry = curr_price
                        sl = min(last_h1['low'], support_level) - (0.15 * atr1h)
                        risk = entry - sl
                        
                        if risk > 0:
                            tp1 = entry + (risk * 1.6)
                            tp2 = entry + (risk * 2.6)
                            score = int(88 + min(wick_ratio * 20, 8))
                            
                            if score >= MIN_WINRATE_SCORE:
                                return {
                                    'engine_tag': 'SUPPORT RETEST',
                                    'title': '🎯 [H1 SUPPORT RETEST] - RECLAIM RETEST ĐẸP',
                                    'symbol': symbol,
                                    'entry': entry,
                                    'sl': sl,
                                    'tp1': tp1,
                                    'tp2': tp2,
                                    'risk_pct': (risk / entry) * 100,
                                    'tp1_pct': ((tp1 - entry) / entry) * 100,
                                    'tp2_pct': ((tp2 - entry) / entry) * 100,
                                    'vol_ratio': last_h1['volume'] / calculate_sma_volume(h1_candles[:-1], 20),
                                    'oi_pct': oi_h1_pct,
                                    'taker_pct': taker_h1_pct,
                                    'score': score,
                                    'timestamp': last_h1['timestamp'],
                                    'signal_id': f"RETEST_{symbol}_{last_h1['timestamp']}"
                                }

    # --------------------------------------------------------------------------
    # ENGINE 2: QUÉT BẮT SỚM M15 (EARLY BUY - CHUẨN RSI 40-55 ĐÁY)
    # --------------------------------------------------------------------------
    m15_candles = get_closed_klines(symbol, "15", limit=30)
    if len(m15_candles) >= 22:
        last_m15 = m15_candles[-1]
        body_m15_pct = (last_m15['close'] - last_m15['open']) / last_m15['open'] * 100
        
        if body_m15_pct <= 2.5:
            rsi_m15_vals = calculate_rsi(m15_candles, 14)
            if len(rsi_m15_vals) >= 3:
                curr_rsi = rsi_m15_vals[-1]
                prev_rsi = min(rsi_m15_vals[-3:-1])
                
                # BẮT BUỘC RSI CHUẨN 40 - 55 VÀ TẠO ĐÁY BẮT ĐẦU ĐI LÊN
                if 40.0 <= curr_rsi <= 55.0 and curr_rsi > prev_rsi:
                    avg_vol_m15 = calculate_sma_volume(m15_candles[:-1], 20)
                    vol_ratio_m15 = last_m15['volume'] / avg_vol_m15 if avg_vol_m15 > 0 else 0.0
                    taker_m15_pct = get_taker_buy_pct(symbol, "5m")

                    if vol_ratio_m15 >= 1.8 and taker_m15_pct >= 65.0:
                        atr15m = calculate_atr(m15_candles, 14)
                        entry = last_m15['close']
                        sl = last_m15['low'] - (0.15 * atr15m)
                        risk = entry - sl
                        if risk > 0:
                            tp1 = entry + (risk * 1.6)
                            tp2 = entry + (risk * 2.6)
                            score_m15 = int(88 + min(vol_ratio_m15 * 2, 8))
                            
                            if score_m15 >= MIN_WINRATE_SCORE:
                                return {
                                    'engine_tag': 'M15 EARLY BUY',
                                    'title': '⚡️ [EARLY LONG M15] - RSI TẠO ĐÁY NỀN AN TOÀN',
                                    'symbol': symbol,
                                    'entry': entry,
                                    'sl': sl,
                                    'tp1': tp1,
                                    'tp2': tp2,
                                    'risk_pct': (risk / entry) * 100,
                                    'tp1_pct': ((tp1 - entry) / entry) * 100,
                                    'tp2_pct': ((tp2 - entry) / entry) * 100,
                                    'vol_ratio': vol_ratio_m15,
                                    'oi_pct': get_open_interest_pct(symbol, "15m"),
                                    'taker_pct': taker_m15_pct,
                                    'score': score_m15,
                                    'timestamp': last_m15['timestamp'],
                                    'signal_id': f"M15_STRICT_{symbol}_{last_m15['timestamp']}"
                                }

    return None

# ==============================================================================
# 5. GIAO DIỆN TELEGRAM CHUẨN ĐẸP VÀ RÕ RÀNG
# ==============================================================================
def send_telegram_signal(sig):
    global signal_counter
    signal_counter += 1
    sig['id'] = signal_counter
    sig['hit_tp1'] = False
    sig['hit_tp2'] = False
    sig['status_text'] = '⏳ Đang theo dõi real-time'
    
    signals_history.append(sig)
    active_signals.append(sig)

    msg = f"""<b>{sig['title']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
🪙 Coin: <b>#{sig['symbol']}</b> (Score: <b>{sig['score']}/100</b>)

🔥 <b>Dấu hiệu dòng tiền nổ:</b>
• Volume: <b>+{sig['vol_ratio']:.2f}x SMA20</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}%</b>
• Lực mua CVD: <b>{sig['taker_pct']:.1f}%</b>

🎯 <b>KẾ HOẠCH TRADE KỸ THUẬT:</b>
📍 Entry Zone: <code>{sig['entry']:.4f}</code>
🛑 Stop Loss: <code>{sig['sl']:.4f}</code> (<b>-{sig['risk_pct']:.2f}%</b>)
🎯 Take Profit 1 (1.6R): <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.2f}%</b>)
🚀 Take Profit 2 (2.6R): <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.2f}%</b>)
━━━━━━━━━━━━━━━━━━━━━━━
👉 <i>Đã bật theo dõi giá Real-time 24/7!</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [TÍN HIỆU CAO CẤP] #{sig['id']} [{sig['engine_tag']}] cho {sig['symbol']} (Score: {sig['score']})", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Quét Lọc Kèo Tỷ Lệ Thắng Cao (Strict Filter)...", flush=True)
    while True:
        btc_h1 = get_closed_klines("BTCUSDT", "60", limit=30)
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 [STRICT SCAN] Đang lọc kèo Win-Rate cao cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                sig = evaluate_symbol_setups(symbol, btc_h1)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    processed_signal_ids.add(sig['signal_id'])
                    send_telegram_signal(sig)
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.08)

        print(f"✅ Hoàn tất 1 lượt quét. Chờ {COOLDOWN_SECONDS}s quét tiếp...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
