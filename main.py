import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết ra Render
COOLDOWN_SECONDS = 300             # Giãn cách giữa các vòng quét toàn sàn (5 phút)
MIN_TURNOVER_24H = 5_000_000       # Turnover 24h tối thiểu $5M USD
MAX_ALERTS_PER_CYCLE = 4           # Tối đa gửi Top 4 coin đẹp nhất mỗi vòng quét

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
    return "Bybit H4 Rejection & H1 Spot Squeeze Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang hoạt động 24/7!\n⏳ Chưa có tín hiệu H4/H1 mới phát ra."
        send_telegram_direct_message(msg)
        return

    msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH REAL-TIME:</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    total_pnl_pct = 0.0
    
    for sig in signals_history:
        curr_price = get_current_price(sig['symbol'])
        if not curr_price:
            curr_price = sig['entry']
            
        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
        status = sig.get('status_text', '⏳ Đang theo dõi')
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        
        msg += f"<b>#{sig['id']} [{sig['engine_tag']}] {sig['symbol']}</b>\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot Bybit H4/H1 đang hoạt động 24/7!")
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
                        
                        msg = f"""🎯 <b>[CHẠM MỤC TIÊU TP1] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 Mục Tiêu TP1: <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL. Khuyên dời SL về Breakeven!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = "🚀 Hit TP2 (Chốt lời thành công)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CHẠM MỤC TIÊU TP2 - MEGAPUMP] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 Mục Tiêu TP2: <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH TOÀN BỘ MỤC TIÊU SÓNG PUMP! CHỐT LỜI CỰC ĐẸP!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)

                    # 3. HIT SL (Chỉ báo khi CHƯA chạm TP1)
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL (Phá vỡ cấu trúc)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Cấu Trúc: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Cấu trúc râu nến bị phá vỡ. Cắt lỗ bảo vệ tài khoản!</i>"""
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
# 4. ENGINE DÒ TÍN HIỆU (2 MÔ HÌNH QNT & ONDO)
# ==============================================================================

def evaluate_symbol_setups(symbol):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    # --------------------------------------------------------------------------
    # ENGINE 1: QUÉT H4 REJECTION & PUMP (MÔ HÌNH QNT)
    # --------------------------------------------------------------------------
    h4_candles = get_closed_klines(symbol, "240", limit=30)
    if len(h4_candles) >= 22:
        last_h4 = h4_candles[-1]
        h4_range = last_h4['high'] - last_h4['low']
        
        if h4_range > 0:
            lower_wick_h4 = min(last_h4['open'], last_h4['close']) - last_h4['low']
            wick_ratio_h4 = lower_wick_h4 / h4_range
            
            avg_vol_h4 = calculate_sma_volume(h4_candles[:-1], 20)
            vol_ratio_h4 = last_h4['volume'] / avg_vol_h4 if avg_vol_h4 > 0 else 0.0
            oi_h4_pct = get_open_interest_pct(symbol, "4h")

            # ĐIỀU KIỆN MÔ HÌNH QNT (H4)
            if wick_ratio_h4 >= 0.35 and vol_ratio_h4 >= 1.8 and oi_h4_pct >= 3.0:
                atr4h = calculate_atr(h4_candles, 14)
                entry = last_h4['close']
                sl = last_h4['low'] - (0.15 * atr4h)
                
                # Swing high gần nhất trong 20 nến H4
                prev_highs = [c['high'] for c in h4_candles[-20:-1]]
                swing_high = max(prev_highs) if prev_highs else entry + (1.6 * atr4h)
                
                tp1 = max(swing_high, entry + (1.6 * atr4h))
                tp2 = entry + (2.6 * atr4h)
                
                return {
                    'engine_tag': 'H4 REJECTION',
                    'title': '🚀 [H4 REJECTION & PUMP] - QNT PATTERN',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'tp1': tp1,
                    'tp2': tp2,
                    'wick_pct': wick_ratio_h4 * 100,
                    'vol_ratio': vol_ratio_h4,
                    'oi_pct': oi_h4_pct,
                    'taker_pct': get_taker_buy_pct(symbol, "4h"),
                    'score': 90 + min(vol_ratio_h4 * 2, 10),
                    'timestamp': last_h4['timestamp'],
                    'signal_id': f"QNT_H4_{symbol}_{last_h4['timestamp']}"
                }

    # --------------------------------------------------------------------------
    # ENGINE 2: QUÉT H1 SPOT SQUEEZE (MÔ HÌNH ONDO)
    # --------------------------------------------------------------------------
    h1_candles = get_closed_klines(symbol, "60", limit=30)
    if len(h1_candles) >= 22:
        last_h1 = h1_candles[-1]
        avg_vol_h1 = calculate_sma_volume(h1_candles[:-1], 20)
        vol_ratio_h1 = last_h1['volume'] / avg_vol_h1 if avg_vol_h1 > 0 else 0.0
        
        oi_h1_pct = get_open_interest_pct(symbol, "1h")
        taker_h1_pct = get_taker_buy_pct(symbol, "1h")

        # ĐIỀU KIỆN MÔ HÌNH ONDO (H1)
        if vol_ratio_h1 >= 2.2 and oi_h1_pct >= 4.0 and taker_h1_pct >= 80.0:
            atr1h = calculate_atr(h1_candles, 14)
            entry = last_h1['close']
            sl = last_h1['low'] - (0.15 * atr1h)
            
            prev_highs = [c['high'] for c in h1_candles[-20:-1]]
            swing_high = max(prev_highs) if prev_highs else entry + (1.6 * atr1h)
            
            tp1 = max(swing_high, entry + (1.6 * atr1h))
            tp2 = entry + (2.6 * atr1h)

            return {
                'engine_tag': 'H1 SQUEEZE',
                'title': '⚡️ [H1 SPOT SQUEEZE] - ONDO PATTERN',
                'symbol': symbol,
                'entry': entry,
                'sl': sl,
                'tp1': tp1,
                'tp2': tp2,
                'wick_pct': 0,
                'vol_ratio': vol_ratio_h1,
                'oi_pct': oi_h1_pct,
                'taker_pct': taker_h1_pct,
                'score': 88 + min(vol_ratio_h1 * 2, 10),
                'timestamp': last_h1['timestamp'],
                'signal_id': f"ONDO_H1_{symbol}_{last_h1['timestamp']}"
            }

    return None

# ==============================================================================
# 5. GIAO DIỆN TELEGRAM GỌN GÀNG, ĐẸP MẮT, KHÔNG RỐI
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

    risk_pct = ((sig['entry'] - sig['sl']) / sig['entry']) * 100
    tp1_pct = ((sig['tp1'] - sig['entry']) / sig['entry']) * 100
    tp2_pct = ((sig['tp2'] - sig['entry']) / sig['entry']) * 100

    extra_info = f"• Râu nến rút: <b>{sig['wick_pct']:.1f}%</b>\n" if sig['wick_pct'] > 0 else ""

    msg = f"""<b>{sig['title']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
🪙 Coin: <b>#{sig['symbol']}</b> (Signal #{sig['id']})
🔥 <b>Dấu hiệu dòng tiền:</b>
• Volume: <b>+{sig['vol_ratio']:.2f}x SMA20</b>
• Open Interest: <b>+{sig['oi_pct']:.2f}%</b>
• Taker Buy (CVD): <b>{sig['taker_pct']:.1f}%</b>
{extra_info}
🎯 <b>KẾ HOẠCH LỆNH (LONG):</b>
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Stop Loss: <code>{sig['sl']:.4f}</code> (-{risk_pct:.2f}%)
🎯 Take Profit 1: <code>{sig['tp1']:.4f}</code> (+{tp1_pct:.2f}%)
🚀 Take Profit 2: <code>{sig['tp2']:.4f}</code> (+{tp2_pct:.2f}%)
━━━━━━━━━━━━━━━━━━━━━━━
👉 <i>Đã bật theo dõi giá Real-time 24/7!</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ Đã bắn tín hiệu #{sig['id']} [{sig['engine_tag']}] cho {sig['symbol']}", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar Engine H4 Rejection & H1 Spot Squeeze...", flush=True)
    while True:
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 Đang rà soát 2 mô hình QNT/ONDO cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                sig = evaluate_symbol_setups(symbol)
                if sig and sig['signal_id'] not in processed_signal_ids:
                    candidates.append(sig)
            except Exception:
                pass
            time.sleep(0.1)

        if candidates:
            candidates.sort(key=lambda x: x['score'], reverse=True)
            top_candidates = candidates[:MAX_ALERTS_PER_CYCLE]

            for c in top_candidates:
                processed_signal_ids.add(c['signal_id'])
                send_telegram_signal(c)
            
        print(f"✅ Hoàn thành 1 vòng quét. Nghỉ {COOLDOWN_SECONDS}s...", flush=True)
        time.sleep(COOLDOWN_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=process_telegram_commands, daemon=True).start()
    Thread(target=process_active_signals_tracker, daemon=True).start()
    run_radar_loop()
