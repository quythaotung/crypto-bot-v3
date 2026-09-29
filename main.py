import time
import requests
import os
from threading import Thread
from flask import Flask

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEBUG_MODE = True                  # In log chi tiết ra Render
COOLDOWN_SECONDS = 10              # Quét liên tục không nghỉ 5p để bắn tin nhắn tức thì
MIN_TURNOVER_24H = 8_000_000       # Chỉ lọc các coin có thanh khoản 24h >= $8M USD
MAX_ALERTS_PER_CYCLE = 2           # Tối đa gửi Top 2 tín hiệu chất lượng cao nhất mỗi lượt

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
    return "Bybit H4/H1 Structure & Liquidity Sweep Bot Active 24/7!"

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
        msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL:</b>\n\n🟢 Bot đang chạy lọc H4/H1 24/7!\n⏳ Chưa có tín hiệu H1 Reclaim đạt chuẩn cấu trúc H4 nào phát ra."
        send_telegram_direct_message(msg)
        return

    msg = "📊 <b>BÁO CÁO TỔNG HỢP PnL CÁC LỆNH HIGH-QUALITY STRUCTURE:</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    total_pnl_pct = 0.0
    
    for sig in signals_history:
        curr_price = get_current_price(sig['symbol'])
        if not curr_price:
            curr_price = sig['entry']
            
        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
        status = sig.get('status_text', '⏳ Đang theo dõi')
        icon = "🟢" if pnl_pct >= 0 else "🔴"
        
        msg += f"<b>#{sig['id']} [H1 RECLAIM] {sig['symbol']}</b>\n"
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
                        send_telegram_direct_message("🏓 <b>Pong!</b> Bot H4 Structure / H1 Sweep đang hoạt động 24/7!")
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

                    # 1. CHẠM TP1 (Chỉ tự động HỦY SL khi cản TP1)
                    if not sig['hit_tp1'] and curr_price >= sig['tp1']:
                        sig['hit_tp1'] = True
                        sig['status_text'] = "🎯 Hit TP1 H4 Swing High (Đã hủy bảo vệ SL)"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🎯 <b>[CẢNH BÁO CHẠM TP1 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🚀 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🎯 TP1 (H4 Swing High): <code>{sig['tp1']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
✅ <b>ĐÃ CẢN TP1 STRUCTURE THÀNH CÔNG!</b>
🛡 <i>Tự động HỦY cảnh báo SL đối với lệnh này. Bạn nên dời SL về Breakeven (Hoà vốn) để gồng TP2/TP3!</i>"""
                        send_telegram_direct_message(msg)

                    # 2. CHẠM TP2
                    elif sig['hit_tp1'] and not sig['hit_tp2'] and curr_price >= sig['tp2']:
                        sig['hit_tp2'] = True
                        sig['status_text'] = "🚀 Hit TP2 H4 Resistance"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🚀 <b>[CẢNH BÁO CHẠM TP2 STRUCTURE] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🚀 TP2 (H4 Resistance): <code>{sig['tp2']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>ĐÃ ĐẠT VÙNG KHÁNG CỰ H4 TIẾP THEO! KHUYẾN NGHỊ CHỐT PHẦN LỚN LỢI NHUẬN!</b>"""
                        send_telegram_direct_message(msg)

                    # 3. CHẠM TP3 (NẾU CÓ)
                    elif sig['hit_tp2'] and sig.get('tp3') and curr_price >= sig['tp3']:
                        sig['status_text'] = "🔥 Hit TP3 Major H4 Supply"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🔥 <b>[CẢNH BÁO CHẠM TP3 MAJOR SUPPLY] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🔥 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>+{pnl_pct:.2f}%</b>)
🔥 TP3 (Major H4 Supply): <code>{sig['tp3']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
🏆 <b>HOÀN THÀNH TOÀN BỘ CHUYẾN SÓNG H4! FULL TARGET SUCCESS!</b>"""
                        send_telegram_direct_message(msg)
                        if sig in active_signals:
                            active_signals.remove(sig)

                    # 4. HIT SL (Chỉ báo khi CHƯA chạm TP1)
                    elif not sig['hit_tp1'] and curr_price <= sig['sl']:
                        sig['status_text'] = "🛑 Hit SL Sweep Low"
                        pnl_pct = ((curr_price - sig['entry']) / sig['entry']) * 100
                        
                        msg = f"""🛑 <b>[CẢNH BÁO HIT SL STRUCTURAL] #{sig['id']} - {sig['symbol']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
📍 Entry: <code>{sig['entry']:.4f}</code>
🛑 Giá Hiện Tại: <code>{curr_price:.4f}</code> (<b>{pnl_pct:.2f}%</b>)
🛑 Mốc SL Đáy Sweep: <code>{sig['sl']:.4f}</code>
━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Vi phạm mốc Liquidity Sweep Low. Dừng lệnh bảo vệ vốn!</i>"""
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

def get_closed_klines(symbol, interval, limit=100):
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

def calculate_ema(candles, period):
    if len(candles) < period: return []
    closes = [c['close'] for c in candles]
    multiplier = 2 / (period + 1)
    ema_values = []
    
    # Tính SMA làm giá trị khởi tạo cho EMA
    initial_sma = sum(closes[:period]) / period
    ema_values.append(initial_sma)
    
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

def get_24h_turnover(symbol):
    url = f"{BASE_URL}/v5/market/tickers?category=linear&symbol={symbol}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            return float(res["result"]["list"][0].get("turnover24h", 0))
    except Exception:
        pass
    return 0

# ==============================================================================
# 4. CORE STRATEGY: H4 BULLISH TREND + H1 LIQUIDITY SWEEP & RECLAIM
# ==============================================================================

def evaluate_h4_h1_setup(symbol):
    if get_24h_turnover(symbol) < MIN_TURNOVER_24H: 
        return None

    # --------------------------------------------------------------------------
    # BƯỚC 1: XÁC MINH H4 BULLISH TREND
    # --------------------------------------------------------------------------
    h4_candles = get_closed_klines(symbol, "240", limit=60)
    if len(h4_candles) < 40: return None

    ema34_h4 = calculate_ema(h4_candles, 34)
    ema89_h4 = calculate_ema(h4_candles, 89)
    if not ema34_h4 or not ema89_h4: return None

    last_h4 = h4_candles[-1]
    curr_h4_close = last_h4['close']
    curr_ema34_h4 = ema34_h4[-1]
    curr_ema89_h4 = ema89_h4[-1]

    # 1.1 Giá > EMA34 và EMA34 >= EMA89
    if not (curr_h4_close > curr_ema34_h4 and curr_ema34_h4 >= curr_ema89_h4):
        return None

    # 1.2 Cấu trúc HH (Higher High) + HL (Higher Low) khung H4
    recent_h4_lows = [c['low'] for c in h4_candles[-20:-1]]
    recent_h4_highs = [c['high'] for c in h4_candles[-20:-1]]
    prev_h4_lows = [c['low'] for c in h4_candles[-40:-20]]
    prev_h4_highs = [c['high'] for c in h4_candles[-40:-20]]
    
    if not (recent_h4_lows and prev_h4_lows and recent_h4_highs and prev_h4_highs):
        return None
        
    if not (min(recent_h4_lows) > min(prev_h4_lows) and max(recent_h4_highs) > max(prev_h4_highs)):
        return None

    # --------------------------------------------------------------------------
    # BƯỚC 2: XÁC ĐỊNH VÙNG H4 SUPPORT ZONE
    # --------------------------------------------------------------------------
    h4_swing_low = min(recent_h4_lows)
    h4_support_low = min(h4_swing_low, curr_ema34_h4 * 0.995)
    h4_support_high = max(h4_swing_low * 1.01, curr_ema34_h4 * 1.005)

    # --------------------------------------------------------------------------
    # BƯỚC 3: KIỂM TRA H1 PULLBACK & LIQUIDITY SWEEP
    # --------------------------------------------------------------------------
    h1_candles = get_closed_klines(symbol, "60", limit=50)
    if len(h1_candles) < 30: return None

    last_h1 = h1_candles[-1]
    curr_price = last_h1['close']

    # Entry không được quá xa H4 Support Zone (chống chase pump)
    if curr_price > h4_support_high * 1.025:
        return None

    # Phải có nhịp chạm/nhún sâu vào H4 Support Zone trong 10 nến H1 gần nhất
    recent_h1_lows_10 = [c['low'] for c in h1_candles[-10:]]
    if min(recent_h1_lows_10) > h4_support_high:
        return None

    # TÌM CẤU TRÚC LIQUIDITY SWEEP H1 (Phá Low gần nhất rồi rút chân đóng cửa lại trên Support)
    h1_recent_support = min([c['low'] for c in h1_candles[-15:-3]])
    sweep_candle = None
    sweep_candle_idx = -1

    for idx in range(-5, 0):
        candle = h1_candles[idx]
        if candle['low'] < h1_recent_support and candle['close'] > h1_recent_support:
            sweep_candle = candle
            sweep_candle_idx = idx
            break

    if not sweep_candle:
        return None

    # --------------------------------------------------------------------------
    # BƯỚC 4: CHECK BULLISH RSI DIVERGENCE (PHÂN KỲ ẨN/THƯỜNG KHUNG H1)
    # --------------------------------------------------------------------------
    rsi_h1 = calculate_rsi(h1_candles, 14)
    has_rsi_div = False
    if len(rsi_h1) >= 15:
        curr_rsi = rsi_h1[-1]
        prev_min_rsi = min(rsi_h1[-15:-3])
        # Nếu Giá tạo Low thấp hơn nhưng RSI tạo Low cao hơn -> Divergence
        if last_h1['low'] <= h1_recent_support and curr_rsi > prev_min_rsi:
            has_rsi_div = True

    # --------------------------------------------------------------------------
    # BƯỚC 5: XÁC NHẬN H1 STRUCTURAL RECLAIM
    # --------------------------------------------------------------------------
    minor_resistance = max([c['high'] for c in h1_candles[sweep_candle_idx:-1]])
    if curr_price <= minor_resistance and last_h1['close'] <= minor_resistance:
        return None  # Chưa đóng nến Reclaim phá cản nhỏ H1

    # --------------------------------------------------------------------------
    # BƯỚC 6: TÍNH STOP LOSS VÀ TAKE PROFIT THEO H4 STRUCTURE
    # --------------------------------------------------------------------------
    h1_atr = calculate_atr(h1_candles, 14)
    sweep_low = min([c['low'] for c in h1_candles[-5:]])
    
    # SL nằm dưới đáy Sweep + Buffer đệm 0.2 x ATR (Không % cố định, không theo RR)
    sl_price = sweep_low - (0.20 * h1_atr)
    risk = curr_price - sl_price
    if risk <= 0: return None

    # BẮT CÁC MỐC TP THEO CẤU TRÚC H4
    # TP1 = H4 Swing High gần nhất
    tp1_base = max([c['high'] for c in h4_candles[-15:]])
    tp1_price = tp1_base

    # TP2 = H4 Resistance tiếp theo
    tp2_candidates = [c['high'] for c in h4_candles[-40:-15] if c['high'] > tp1_price * 1.01]
    tp2_price = max(tp2_candidates) if tp2_candidates else tp1_price * 1.05

    # TP3 = H4 Major Swing High / Supply
    tp3_candidates = [c['high'] for c in h4_candles[-60:-40] if c['high'] > tp2_price * 1.01]
    tp3_price = max(tp3_candidates) if tp3_candidates else None

    # --------------------------------------------------------------------------
    # BƯỚC 7: KIỂM TRA ROOM ĐẾN TP1 (CẮT BỎ TÍN HIỆU NẾU TP1 TRÊN ĐẦU QUÁ CẦN)
    # --------------------------------------------------------------------------
    reward_tp1 = tp1_price - curr_price
    if reward_tp1 <= 0: return None

    # Nếu Khoảng cách đến TP1 quá gần (< 1.2x Risk) -> NO SIGNAL (Thỏa mãn yêu cầu Room TP)
    if reward_tp1 < (risk * 1.2):
        return None

    return {
        'engine_tag': 'H1 SWEEP RECLAIM',
        'title': '🎯 [H1 LONG SETUP] - H4 TREND & LIQUIDITY SWEEP',
        'symbol': symbol,
        'entry': curr_price,
        'sl': sl_price,
        'tp1': tp1_price,
        'tp2': tp2_price,
        'tp3': tp3_price,
        'risk_pct': (risk / curr_price) * 100,
        'tp1_pct': ((tp1_price - curr_price) / curr_price) * 100,
        'tp2_pct': ((tp2_price - curr_price) / curr_price) * 100,
        'rr_tp1': reward_tp1 / risk,
        'has_rsi_div': has_rsi_div,
        'timestamp': last_h1['timestamp'],
        'signal_id': f"H1_RECLAIM_{symbol}_{last_h1['timestamp']}"
    }

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

    rsi_str = "✅ Có Bullish Divergence" if sig['has_rsi_div'] else "⚪️ Theo cấu trúc Dòng tiền"
    tp3_str = f"🔥 <b>Take Profit 3 (H4 Supply):</b> <code>{sig['tp3']:.4f}</code>\n" if sig.get('tp3') else ""

    msg = f"""<b>{sig['title']}</b>
━━━━━━━━━━━━━━━━━━━━━━━
🪙 Coin: <b>#{sig['symbol']}</b> (Signal #{sig['id']})
📈 H4 Trend: <b>Bullish (HH/HL + Price > EMA34 >= EMA89)</b>
🎯 H1 Sweep: <b>Đã Liquidity Sweep & Reclaim Cản Nho</b>
📊 RSI Status: <b>{rsi_str}</b>

🎯 <b>KẾ HOẠCH LỆNH STRUCTURE LONG:</b>
📍 <b>Entry:</b> <code>{sig['entry']:.4f}</code>
🛑 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code> (<b>-{sig['risk_pct']:.2f}%</b>)
<i>(Dưới đáy H1 Sweep Low + Buffer ATR)</i>

🎯 <b>Take Profit 1 (H4 Swing High):</b> <code>{sig['tp1']:.4f}</code> (<b>+{sig['tp1_pct']:.2f}%</b>)
🚀 <b>Take Profit 2 (H4 Resistance):</b> <code>{sig['tp2']:.4f}</code> (<b>+{sig['tp2_pct']:.2f}%</b>)
{tp3_str}📐 <b>R:R Tham Khảo TP1:</b> 1:{sig['rr_tp1']:.2f}
━━━━━━━━━━━━━━━━━━━━━━━
👉 <i>Đã bật theo dõi giá Real-time 24/7!</i>"""

    send_telegram_direct_message(msg)
    print(f"✅ [H4/H1 STRUCTURE SIGNAL] #{sig['id']} cho {sig['symbol']} (Entry: {sig['entry']:.4f})", flush=True)

# ==============================================================================
# 6. MAIN LOOP RUNNER
# ==============================================================================
def run_radar_loop():
    print("⚡️ Bắt đầu Radar H4 Bullish & H1 Liquidity Sweep Reclaim Bot...", flush=True)
    while True:
        all_symbols = get_all_bybit_usdt_symbols()
        print(f"🔍 [H4/H1 SCAN] Đang lọc cấu trúc Sweep & Reclaim cho {len(all_symbols)} coin...", flush=True)
        
        candidates = []
        for symbol in all_symbols:
            try:
                sig = evaluate_h4_h1_setup(symbol)
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
