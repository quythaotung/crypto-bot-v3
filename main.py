import os
import time
import requests
import numpy as np
import pandas as pd
import sqlite3
from threading import Thread
from flask import Flask

# ==============================================================================
# 1. KHỞI TẠO FLASK KEEP-ALIVE SERVER (DÙNG CHO RENDER 24/7)
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bybit H4 EMA20 A+ Box Breakout Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BIẾN MÔI TRƯỜNG & ĐIỀU KIỆN
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "2000000"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.20"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "100"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "3600"))

MAX_ALERTS_PER_CYCLE = 3
MAX_ACTIVE_TRADES = 5

DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE
# ==============================================================================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sent_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                score INTEGER,
                call_count INTEGER DEFAULT 1,
                last_sent_at INTEGER NOT NULL,
                UNIQUE(symbol, status)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp3 REAL NOT NULL,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                status TEXT DEFAULT 'OPEN',
                created_at INTEGER NOT NULL,
                updated_at INTEGER DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_gainers_tracker (
                symbol TEXT PRIMARY KEY,
                appear_count INTEGER DEFAULT 1,
                last_seen INTEGER NOT NULL
            )
        """)
        conn.commit()

def update_top_gainers_frequency(symbols_in_top: list):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for sym in symbols_in_top:
            cursor.execute("""
                INSERT INTO top_gainers_tracker (symbol, appear_count, last_seen)
                VALUES (?, 1, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    appear_count = appear_count + 1,
                    last_seen = ?
            """, (sym, now_ts, now_ts))
        conn.commit()

def get_gainer_frequency(symbol: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT appear_count FROM top_gainers_tracker WHERE symbol = ?", (symbol,))
        row = cursor.fetchone()
        return row[0] if row else 1

def is_symbol_currently_active(symbol: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM active_trades WHERE symbol = ? AND status = 'OPEN'", (symbol,))
        return cursor.fetchone() is not None

def get_active_trades_count() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM active_trades WHERE status = 'OPEN'")
        return cursor.fetchone()[0]

def process_signal_dedup(symbol: str, status: str, score: int) -> tuple:
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT call_count, last_sent_at FROM sent_signals WHERE symbol = ? AND status = ?",
            (symbol, status)
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                "INSERT INTO sent_signals (symbol, status, score, call_count, last_sent_at) VALUES (?, ?, ?, 1, ?)",
                (symbol, status, score, now_ts)
            )
            conn.commit()
            return True, 1
        else:
            prev_call_count, last_sent_at = row
            time_passed = now_ts - last_sent_at

            if prev_call_count >= 3:
                return False, prev_call_count

            if time_passed < COOLDOWN_SECONDS:
                return False, prev_call_count

            new_call_count = prev_call_count + 1
            cursor.execute(
                "UPDATE sent_signals SET call_count = ?, last_sent_at = ?, score = ? WHERE symbol = ? AND status = ?",
                (new_call_count, now_ts, score, symbol, status)
            )
            conn.commit()
            return True, new_call_count

def save_active_trade(sig: dict):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO active_trades (symbol, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'OPEN', ?, ?)
        """, (sig['symbol'], sig['close_price'], sig['sl'], sig['tp1'], sig['tp2'], sig['tp3'], now_ts, now_ts))
        conn.commit()

# ==============================================================================
# 4. HÀM TỔNG HỢP BÁO CÁO 24H
# ==============================================================================
def get_24h_summary_report() -> str:
    now_ts = int(time.time())
    cutoff_24h = now_ts - (24 * 3600)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT symbol, entry, sl, tp1, tp2, tp3, status, updated_at 
            FROM active_trades 
            WHERE status IN ('CLOSED_TP1', 'CLOSED_TP2', 'CLOSED_TP3', 'CLOSED_SL') AND updated_at >= ?
            ORDER BY updated_at DESC
        """, (cutoff_24h,))
        closed_trades = cursor.fetchall()

        cursor.execute("""
            SELECT symbol, entry, sl, tp1, tp2, tp3, tp1_hit 
            FROM active_trades 
            WHERE status = 'OPEN' 
            ORDER BY created_at DESC
        """)
        open_trades = cursor.fetchall()

    msg = "📊 <b>BÁO CÁO TỔNG HỢP KẾT QUẢ CRYPTO H4 EMA20 A+ (24H)</b>\n"
    msg += "===================================="

    if not closed_trades and not open_trades:
        return msg + "\n\n🔎 Chưa có dữ liệu giao dịch trong 24h qua."

    if closed_trades:
        msg += "\n\n🎯 <b>KÈO ĐÃ CHỐT KẾT QUẢ (24H QUA):</b>"
        for idx, t in enumerate(closed_trades, 1):
            sym, entry, sl, tp1, tp2, tp3, status, updated_at = t
            if status == 'CLOSED_TP3':
                tag = "🚀🚀 <b>[ĐÃ TP3 (+20%)]</b>"
            elif status == 'CLOSED_TP2':
                tag = "🚀 <b>[ĐÃ TP2 (+10%)]</b>"
            elif status == 'CLOSED_TP1':
                tag = "✅ <b>[ĐÃ TP1 (+5%)]</b>"
            elif status == 'CLOSED_SL':
                tag = "🛑 <b>[ĐÃ SL]</b>"
            else:
                tag = "<b>[ĐÃ ĐÓNG]</b>"

            msg += f"\n{idx}. #{sym} {tag} | Entry: <code>{entry:.4f}</code>"

    if open_trades:
        msg += "\n\n⏳ <b>KÈO ĐANG THEO DÕI (ACTIVE):</b>"
        for idx, t in enumerate(open_trades, 1):
            sym, entry, sl, tp1, tp2, tp3, tp1_hit = t
            st = "✅ (Đã cắn TP1 -> Dời SL về Entry)" if tp1_hit == 1 else "⏳ (Đang chạy)"
            msg += f"\n{idx}. #{sym} {st} | Entry: <code>{entry:.4f}</code> | SL: <code>{sl:.4f}</code>"

    msg += "\n\n<i>💡 Chiến lược H4 Breakout Hộp Sideway + EMA20 Confirm A+.</i>"
    return msg

def listen_telegram_commands():
    if not BOT_TOKEN: return
    last_update_id = 0
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    while True:
        try:
            params = {"offset": last_update_id + 1, "timeout": 10}
            res = requests.get(url, params=params, timeout=12).json()

            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_update_id = update["update_id"]
                    message = update.get("message", {})
                    text = message.get("text", "")
                    chat_id = str(message.get("chat", {}).get("id", ""))

                    if text.strip().lower() in ["/tonghop", "tonghop"]:
                        summary_msg = get_24h_summary_report()
                        send_direct_message(summary_msg, target_chat_id=chat_id if chat_id else CHAT_ID)
        except Exception:
            pass
        time.sleep(2)

# ==============================================================================
# 5. LẤY TOP GAINERS BYBIT (CRYPTO ONLY)
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_bybit_crypto_top_gainers_5_10():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            
            non_crypto = [
                'XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD', 'PLATINUM', 'PALLADIUM',
                'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX', 'US30', 'US100', 'US500', 'UK100', 'GER30',
                'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', 'AMD', 'INTC', 'BABA', 'COIN',
                '3L', '3S', 'BEAR', 'BULL', 'HALF', 'HEDGE'
            ]
            
            valid = []
            symbols_for_freq = []

            for t in tickers:
                sym = t.get("symbol", "")
                turnover = safe_float(t.get("turnover24h", 0))
                last_price = safe_float(t.get("lastPrice", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                price_change_pct = safe_float(t.get("price24hPcnt", 0)) * 100
                funding_rate = safe_float(t.get("fundingRate", 0)) * 100

                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI", "EUR", "GBP")):
                    if not any(k in sym for k in non_crypto):
                        if price_change_pct >= 4.0:
                            symbols_for_freq.append(sym)
                            if turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                                valid.append({
                                    "symbol": sym,
                                    "price24hPcnt": round(price_change_pct, 2),
                                    "turnover24h": turnover,
                                    "lastPrice": last_price,
                                    "fundingRate": funding_rate,
                                    "spread_pct": round(spread_pct, 3)
                                })

            if symbols_for_freq:
                update_top_gainers_frequency(symbols_for_freq)

            return sorted(valid, key=lambda x: x["price24hPcnt"], reverse=True)
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Top Gainers Crypto: {e}", flush=True)
    return []

def get_closed_klines_df_h4(symbol: str, limit: int = 60) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval=240&limit={limit+1}"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            raw_list = res["result"]["list"]
            if len(raw_list) < 2: return pd.DataFrame()
            closed_raw = raw_list[1:]
            df = pd.DataFrame(closed_raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'volume', 'turnover']:
                df[col] = df[col].astype(float)
            df['timestamp'] = df['timestamp'].astype(int)
            return df
    except Exception:
        pass
    return pd.DataFrame()

def get_open_interest_change(symbol: str) -> float:
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=4h&limit=3"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            oi_list = res["result"]["list"]
            if len(oi_list) >= 2:
                curr_oi = safe_float(oi_list[0].get("openInterest", 0))
                prev_oi = safe_float(oi_list[1].get("openInterest", 0))
                if prev_oi > 0:
                    return round(((curr_oi - prev_oi) / prev_oi) * 100, 2)
    except Exception:
        pass
    return 0.0

def add_h4_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 25: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)

    return df

# ==============================================================================
# 6. ENGINE BẮT SETUP A+ (BREAKOUT HỘP SIDEWAY VƯỢT ĐỈNH TÍCH LŨY + EMA20)
# ==============================================================================
def analyze_h4_ema20_setup(df_h4: pd.DataFrame, gainer_freq: int) -> dict:
    if df_h4.empty or len(df_h4) < 25: return None

    last_candle = df_h4.iloc[-1]
    
    # Lấy 6 đến 8 nến H4 trước đó (tương đương 24h - 32h đi ngang/giảm trong hộp đỏ)
    prev_box_candles = df_h4.iloc[-9:-1]

    close_now = last_candle['close']
    open_now = last_candle['open']
    high_now = last_candle['high']
    low_now = last_candle['low']
    ema20_now = last_candle['ema20']
    vol_ratio_now = last_candle['vol_ratio']

    # BẮT BỎ CÁC NẾN TRONG HỘP SIDEWAY ĐỎ (MÔ HÌNH HÌNH ẢNH MINH HỌA A+)
    box_high = prev_box_candles['high'].max()
    box_low = prev_box_candles['low'].min()
    box_range_pct = ((box_high - box_low) / box_low) * 100

    is_green_now = close_now > open_now
    is_close_above_ema20 = close_now > ema20_now
    
    # 🔴 ĐIỀU KIỆN TIÊU CHUẨN A+: NẾN XANH ĐÓNG HẲN TRÊN ĐỈNH HỘP TÍCH LŨY + EMA20
    is_breakout_box = close_now > box_high
    
    candle_body = close_now - open_now
    candle_range = high_now - low_now
    is_strong_body = (candle_body / candle_range >= 0.60) if candle_range > 0 else False
    is_strong_vol = vol_ratio_now >= 1.4

    # SETUP 1 (A+ SUPER BREAKOUT): Phá hoàn toàn hộp tích lũy đi ngang + EMA20
    is_setup_a_plus = is_green_now and is_close_above_ema20 and is_breakout_box and is_strong_body and is_strong_vol and (box_range_pct <= 12.0)

    # SETUP 2: CONTINUATION PULLBACK (Tiếp diễn sóng tăng khi chạm EMA20)
    recent_h4 = df_h4.iloc[-10:-1]
    recent_max_high = recent_h4['high'].max()
    recent_min_low = recent_h4['low'].min()
    
    prev_pump_pct = ((recent_max_high - recent_min_low) / recent_min_low) * 100
    pullback_pct = ((recent_max_high - low_now) / recent_max_high) * 100 if recent_max_high > 0 else 0.0

    touched_ema20 = (low_now <= ema20_now * 1.01) and (close_now >= ema20_now * 0.995)
    is_setup_pullback = (prev_pump_pct >= 8.0) and (4.0 <= pullback_pct <= 9.0) and touched_ema20 and is_green_now and is_close_above_ema20

    setup_type = None
    setup_desc = ""

    if is_setup_a_plus:
        setup_type = "H4 A+ BOX BREAKOUT"
        setup_desc = f"Nến H4 xanh dốc đóng vượt cản hộp tích lũy {box_high:.4f} ({box_range_pct:.1f}%) & EMA20"
    elif is_setup_pullback:
        setup_type = "H4 EMA20 PULLBACK"
        setup_desc = f"Chỉnh {pullback_pct:.1f}% về sát EMA20 rồi bật nến xanh H4"

    if not setup_type:
        return None

    # SL bên dưới đáy nến H4 / Đáy hộp sideway
    sl = min(low_now, box_low) * 0.997
    risk_pct = ((close_now - sl) / close_now) * 100

    if risk_pct < 2.0:
        sl = close_now * 0.98

    tp1 = close_now * 1.05
    tp2 = close_now * 1.10
    tp3 = close_now * 1.20

    score = 85
    if is_setup_a_plus: score += 10
    if vol_ratio_now >= 2.5: score += 5

    return {
        "status": setup_type,
        "setup_desc": setup_desc,
        "score": min(100, score),
        "close_price": close_now,
        "ema20": round(ema20_now, 4),
        "box_high": round(box_high, 4),
        "low_candle": round(low_now, 4),
        "vol_ratio": round(vol_ratio_now, 2),
        "gainer_freq": gainer_freq,
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "tp3": round(tp3, 4),
        "timestamp": last_candle['timestamp']
    }

# ==============================================================================
# 7. HỆ THỐNG GỬI TELEGRAM & CẬP NHẬT TRẠNG THÁI ACTIVE
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    msg = f"""🚨 <b>BYBIT H4 EMA20 SETUP A+ SCANNER</b>

<b>Coin:</b> #{sig['symbol']} (Top Gainers {sig['price24hPcnt']}%)
<b>Trạng thái:</b> <code>{sig['status']}</code>
🔔 <b>Lần thông báo:</b> <b>Call lần {sig['call_count']}/3</b>
⭐ <b>Điểm chuẩn A+:</b> <b>{sig['score']}/100</b>
<b>Khung thời gian:</b> H4 (4 Giờ)

💵 <b>Giá vào (Entry):</b> <code>{sig['close_price']:.4f}</code>
🎯 <b>Cản đỉnh hộp Sideway:</b> <code>{sig['box_high']:.4f}</code>
📉 <b>Đường EMA 20 H4:</b> <code>{sig['ema20']:.4f}</code>
🛡 <b>Stop Loss (Đáy nến H4/Đáy hộp):</b> <code>{sig['sl']:.4f}</code>

🎯 <b>Take Profit Target:</b>
• <b>TP1 (+5%):</b> <code>{sig['tp1']:.4f}</code>
• <b>TP2 (+10%):</b> <code>{sig['tp2']:.4f}</code>
• <b>TP3 (+20%):</b> <code>{sig['tp3']:.4f}</code>

📊 <b>Turnover 24h:</b> ${sig['turnover24h']/1e6:.2f}M USD
🔥 <b>Volume H4 / MA20:</b> {sig['vol_ratio']}x
📈 <b>OI H4 Change:</b> {sig['oi_info']}
💸 <b>Funding Rate:</b> {sig['funding']:.4f}%

💡 <b>Mô tả tín hiệu:</b>
• {sig['setup_desc']}
• Nằm trong Top Gainers Bybit (tăng {sig['price24hPcnt']}%)

🔗 <a href="https://www.bybit.com/trade/usdt/{sig['symbol']}">Biểu đồ Bybit {sig['symbol']}</a>

<i>💡 Gõ <b>/tonghop</b> trên Telegram để xem báo cáo TP/SL tổng hợp trong 24h.</i>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        print(f"[ERROR] Lỗi gửi Telegram: {e}", flush=True)
        return False

def send_direct_message(text: str, target_chat_id: str = None):
    dest = target_chat_id if target_chat_id else CHAT_ID
    if not BOT_TOKEN or not dest: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": dest, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=8)
    except Exception:
        pass

def track_and_update_active_trades(tickers_map: dict):
    now_ts = int(time.time())
    status_changed = False

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, symbol, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit FROM active_trades WHERE status = 'OPEN'")
        trades = cursor.fetchall()

        for t in trades:
            trade_id, sym, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit = t
            if sym not in tickers_map: continue

            curr_price = tickers_map[sym]

            if tp1_hit == 0 and curr_price >= tp1:
                cursor.execute("UPDATE active_trades SET tp1_hit = 1, sl = ?, updated_at = ? WHERE id = ?", (entry, now_ts, trade_id))
                conn.commit()
                status_changed = True
                print(f"🎯 [TP1 +5% HIT] #{sym} -> SL dời về Entry", flush=True)

            elif tp1_hit == 1 and tp2_hit == 0 and curr_price >= tp2:
                cursor.execute("UPDATE active_trades SET tp2_hit = 1, updated_at = ? WHERE id = ?", (now_ts, trade_id))
                conn.commit()
                status_changed = True
                print(f"🚀 [TP2 +10% HIT] #{sym}", flush=True)

            elif curr_price >= tp3:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_TP3', updated_at = ? WHERE id = ?", (now_ts, trade_id))
                cursor.execute("DELETE FROM sent_signals WHERE symbol = ?", (sym,))
                conn.commit()
                status_changed = True
                print(f"🚀🚀 [TP3 +20% HIT] #{sym} CLOSED", flush=True)

            elif curr_price <= sl:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_SL', updated_at = ? WHERE id = ?", (now_ts, trade_id))
                cursor.execute("DELETE FROM sent_signals WHERE symbol = ?", (sym,))
                conn.commit()
                status_changed = True
                print(f"🛑 [SL HIT] #{sym} CLOSED", flush=True)

    if status_changed:
        summary_msg = get_24h_summary_report()
        send_direct_message(summary_msg)

# ==============================================================================
# 8. AUTOMATIC SCANNER LOOP
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=listen_telegram_commands, daemon=True).start()

    print("⚡️ Bắt đầu Bybit H4 EMA20 A+ Box Breakout Engine...", flush=True)
    while True:
        try:
            top_gainers = get_bybit_crypto_top_gainers_5_10()
            tickers_map = {t['symbol']: t['lastPrice'] for t in top_gainers}

            track_and_update_active_trades(tickers_map)

            print(f"\n🔍 [SCANNER] Tìm thấy {len(top_gainers)} Coin Crypto trong Top Gainers...", flush=True)

            scanned_signals = []
            for t in top_gainers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                df_h4 = get_closed_klines_df_h4(sym, limit=40)
                if df_h4.empty: continue

                df_h4 = add_h4_indicators(df_h4)
                gainer_freq = get_gainer_frequency(sym)

                analysis = analyze_h4_ema20_setup(df_h4, gainer_freq)
                if analysis:
                    oi_change = get_open_interest_change(sym)
                    analysis['symbol'] = sym
                    analysis['price24hPcnt'] = t['price24hPcnt']
                    analysis['turnover24h'] = t['turnover24h']
                    analysis['funding'] = t['fundingRate']
                    analysis['oi_info'] = f"{oi_change:+}%/4h"
                    scanned_signals.append(analysis)

            scanned_signals.sort(key=lambda x: (x['gainer_freq'], x['score']), reverse=True)
            top_signals = scanned_signals[:MAX_ALERTS_PER_CYCLE]

            sent_count = 0
            if top_signals:
                for sig in top_signals:
                    if get_active_trades_count() >= MAX_ACTIVE_TRADES and not is_symbol_currently_active(sig['symbol']):
                        print(f"⚠️ [SLOT FULL] Đã đủ {MAX_ACTIVE_TRADES} kèo đang chạy, tạm dừng báo kèo mới.", flush=True)
                        break

                    should_send, call_count = process_signal_dedup(sig['symbol'], sig['status'], sig['score'])
                    if should_send:
                        sig['call_count'] = call_count
                        if send_telegram_alert(sig):
                            save_active_trade(sig)
                            sent_count += 1
                            print(f"✅ [H4 SETUP A+ TOP {sent_count}] #{sig['symbol']} Entry: {sig['close_price']} | SL: {sig['sl']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ H4. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_scanner_loop()
