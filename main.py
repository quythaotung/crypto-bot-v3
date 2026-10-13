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
    return "Bybit MA20 Breakout & Retest Rebound Scanner Active 24/7!"

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
        """, (sig['symbol'], sig['retest_entry'], sig['sl'], sig['tp1'], sig['tp2'], sig['tp3'], now_ts, now_ts))
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

    msg = "📊 <b>BÁO CÁO TỔNG HỢP KẾT QUẢ CRYPTO (24H)</b>\n"
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

            msg += f"\n{idx}. #{sym} {tag} | Entry Retest: <code>{entry:.4f}</code>"

    if open_trades:
        msg += "\n\n⏳ <b>KÈO ĐANG THEO DÕI (ACTIVE):</b>"
        for idx, t in enumerate(open_trades, 1):
            sym, entry, sl, tp1, tp2, tp3, tp1_hit = t
            st = "✅ (Đã cắn TP1 -> Dời SL về Entry)" if tp1_hit == 1 else "⏳ (Đang chạy)"
            msg += f"\n{idx}. #{sym} {st} | Entry Retest: <code>{entry:.4f}</code> | SL: <code>{sl:.4f}</code>"

    msg += "\n\n<i>💡 Hệ thống quét H4 MA20 Breakout & Retest Rebound.</i>"
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
# 5. LẤY TICKERS BYBIT (CHẶN ETF / STOCKS / TRADFI)
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_bybit_crypto_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            
            non_crypto_exact = [
                'SOXS', 'SOXL', 'TQQQ', 'SQQQ', 'UVXY', 'SPY', 'QQQ', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                'US30', 'US100', 'US500', 'UK100', 'GER30', 'JPN225', 'SPCX', 'SXXP',
                'XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD', 'PLATINUM', 'PALLADIUM',
                'JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BLK', 'AXP', 'V', 'MA', 'PYPL', 'SCHW',
                'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', 'AMD', 'INTC', 'BABA', 'COIN',
                'PLTR', 'MSTR', 'CRWD', 'HOOD', 'DIS', 'NKE', 'PFE', 'JNJ', 'PG', 'XOM', 'CVX',
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
                    base_coin = sym.replace("USDT", "")
                    
                    is_non_crypto = (
                        base_coin in non_crypto_exact or 
                        any(k in sym for k in non_crypto_exact) or
                        base_coin.startswith(("SOX", "TQQ", "SQQ", "USD", "EUR"))
                    )

                    if not is_non_crypto:
                        if turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                            if price_change_pct >= 3.0:
                                symbols_for_freq.append(sym)
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

            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Tickers Crypto: {e}", flush=True)
    return []

def get_closed_klines_df(symbol: str, interval: str, limit: int = 60) -> pd.DataFrame:
    url = f"{BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&limit={limit+1}"
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

def get_open_interest_change(symbol: str, interval: str = "60") -> float:
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime={interval}&limit=3"
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

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 25: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['ema89'] = closes.ewm(span=89, adjust=False).mean()
    df['ema200'] = closes.ewm(span=200, adjust=False).mean()

    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)

    return df

# ==============================================================================
# 6. ENGINE PHÁT HIỆN SETUP: ĐÓNG NẾN QUA MA20 -> RETEST SÁT MA20 RÚT CHÂN / ĐÓNG XANH H4
# ==============================================================================
def analyze_h4_ma20_breakout_retest(df_h4: pd.DataFrame, realtime_price: float) -> dict:
    if df_h4.empty or len(df_h4) < 25: return None

    last_candle = df_h4.iloc[-1]
    prev_candle = df_h4.iloc[-2]

    close_now = realtime_price if realtime_price > 0 else last_candle['close']
    open_now = last_candle['open']
    high_now = max(last_candle['high'], close_now)
    low_now = min(last_candle['low'], close_now)
    ema20_now = last_candle['ema20']
    vol_ratio_now = last_candle['vol_ratio']

    # 1. ĐÃ TỪNG ĐÓNG NẾN VƯỢT QUA MA20 Ở CÂY NẾN TRƯỚC HỌẶC GẦN ĐÂY
    is_breakout_above_ma20 = (prev_candle['close'] > prev_candle['ema20']) or (df_h4.iloc[-3]['close'] > df_h4.iloc[-3]['ema20'])

    # 2. CÂY NẾN H4 HIỆN TẠI TEST LẠI SÁT/CHẠM ĐƯỜNG MA20 (RÂU LOW CHỌC SÁT MA20)
    is_testing_ma20 = (low_now <= ema20_now * 1.015) and (low_now >= ema20_now * 0.985)

    # 3. ĐÓNG NẾN TRÊN MA20 VÀ CÓ LỰC RÚT CHÂN / ĐÓNG NẾN XANH KẾT THÚC CÂY RETEST
    is_close_above_ma20 = close_now > ema20_now
    lower_shadow = min(open_now, close_now) - low_now
    candle_range = high_now - low_now
    is_strong_rebound = (close_now > open_now) or (candle_range > 0 and lower_shadow / candle_range >= 0.35)

    is_setup_retest = is_breakout_above_ma20 and is_testing_ma20 and is_close_above_ma20 and is_strong_rebound

    if not is_setup_retest:
        return None

    # TÍNH ENTRY LIMIT RETEST AN TOÀN TẠI VÙNG MA20
    retest_entry = ema20_now * 1.002
    is_fomo = close_now > retest_entry * 1.02

    sl = low_now * 0.997
    risk_pct = ((retest_entry - sl) / retest_entry) * 100

    if risk_pct < 2.0:
        sl = retest_entry * 0.98

    tp1 = retest_entry * 1.05
    tp2 = retest_entry * 1.10
    tp3 = retest_entry * 1.20

    return {
        "status": "H4 MA20 BREAKOUT & RETEST BOUNCE",
        "setup_desc": "Đóng nến qua MA20 -> Nến sau nhúng test sát MA20 rút chân / đóng xanh đủ lực đẩy giá tiếp tục xu hướng",
        "score": 95,
        "close_price": close_now,
        "retest_entry": round(retest_entry, 4),
        "is_fomo": is_fomo,
        "ema20": round(ema20_now, 4),
        "box_high": round(prev_candle['high'], 4),
        "low_candle": round(low_now, 4),
        "vol_ratio": round(vol_ratio_now, 2),
        "gainer_freq": 1,
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

    fomo_warning = ""
    if sig.get('is_fomo', False):
        fomo_warning = f"\n⚠️ <b>CẢNH BÁO MUA ĐUỔI (FOMO):</b> Giá hiện tại ({sig['close_price']:.4f}) đã đẩy cao. <b>NÊN ĐẶT LỆNH LIMIT</b> ở điểm retest sát MA20 bên dưới!\n"

    msg = f"""🚨 <b>BYBIT CRYPTO CANDLE SCANNER</b>

<b>Coin:</b> #{sig['symbol']} (24h Change: {sig['price24hPcnt']}%)
<b>Trạng thái:</b> <code>{sig['status']}</code>
🔔 <b>Lần thông báo:</b> <b>Call lần {sig['call_count']}/3</b>
⭐ <b>Điểm tín hiệu:</b> <b>{sig['score']}/100</b>
{fomo_warning}
🎯 <b>ĐIỂM VÀO LIMIT RETEST MA20:</b> <code>{sig['retest_entry']:.4f}</code>
💵 <b>Giá Realtime sàn hiện tại:</b> <code>{sig['close_price']:.4f}</code>
📉 <b>Đường MA 20 H4:</b> <code>{sig['ema20']:.4f}</code>
🛡 <b>Stop Loss (Đáy râu nến retest):</b> <code>{sig['sl']:.4f}</code>

🎯 <b>Take Profit Target (Tính từ Entry Retest):</b>
• <b>TP1 (+5%):</b> <code>{sig['tp1']:.4f}</code>
• <b>TP2 (+10%):</b> <code>{sig['tp2']:.4f}</code>
• <b>TP3 (+20%):</b> <code>{sig['tp3']:.4f}</code>

📊 <b>Turnover 24h:</b> ${sig['turnover24h']/1e6:.2f}M USD
🔥 <b>Volume Ratio:</b> {sig['vol_ratio']}x
📈 <b>OI Change:</b> {sig['oi_info']}
💸 <b>Funding Rate:</b> {sig['funding']:.4f}%

💡 <b>Mô tả tín hiệu:</b>
• {sig['setup_desc']}

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
                print(f"🎯 [TP1 +5% HIT] #{sym} -> SL dời về Entry Retest", flush=True)

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

    print("⚡️ Bắt đầu Bybit MA20 Retest Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            tickers_map = {t['symbol']: t['lastPrice'] for t in tickers}

            track_and_update_active_trades(tickers_map)

            print(f"\n🔍 [SCANNER] Đang rà soát {len(tickers)} Altcoins thanh khoản cao...", flush=True)

            scanned_signals = []
            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                realtime_price = t['lastPrice']

                # Quét Setup H4 Breakout & Retest MA20 chuẩn
                df_h4 = get_closed_klines_df(sym, "240", limit=40)
                if not df_h4.empty:
                    df_h4 = add_indicators(df_h4)
                    
                    analysis_h4_retest = analyze_h4_ma20_breakout_retest(df_h4, realtime_price)
                    if analysis_h4_retest:
                        oi_change = get_open_interest_change(sym, "240")
                        analysis_h4_retest['symbol'] = sym
                        analysis_h4_retest['price24hPcnt'] = t['price24hPcnt']
                        analysis_h4_retest['turnover24h'] = t['turnover24h']
                        analysis_h4_retest['funding'] = t['fundingRate']
                        analysis_h4_retest['oi_info'] = f"{oi_change:+}%/4h"
                        scanned_signals.append(analysis_h4_retest)

            scanned_signals.sort(key=lambda x: x['score'], reverse=True)
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
                            print(f"✅ [SETUP TOP {sent_count}] #{sig['symbol']} Status: {sig['status']} | Retest Entry: {sig['retest_entry']} | SL: {sig['sl']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_scanner_loop()
