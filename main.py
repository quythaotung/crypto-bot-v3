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
    return "Bybit High-Winrate Signal Engine Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BÁO KÈO LỌC CHẮC (CHỈ BÁO SETUP AA+ VÀ A+)
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "2000000")) # Chỉ lấy coin thanh khoản cao ($2M+)
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.20"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "120"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300")) # Quét 5 phút/lần
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "7200"))     # Cooldown 2 tiếng/coin

MAX_ALERTS_PER_CYCLE = 2                                               # Tối đa 2 kèo chất nhất/chu kỳ
MAX_ACTIVE_TRADES = 3                                                  # Tối đa 3 kèo active cùng lúc

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
                setup_name TEXT DEFAULT 'UNKNOWN',
                created_at INTEGER NOT NULL,
                updated_at INTEGER DEFAULT 0
            )
        """)
        
        cursor.execute("PRAGMA table_info(active_trades)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'setup_name' not in columns:
            cursor.execute("ALTER TABLE active_trades ADD COLUMN setup_name TEXT DEFAULT 'UNKNOWN'")

        conn.commit()

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

            if prev_call_count >= 2 or time_passed < COOLDOWN_SECONDS:
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
    setup_name_to_save = sig.get('internal_setup_name', sig['status'])
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO active_trades (symbol, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, status, setup_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'OPEN', ?, ?, ?)
        """, (sig['symbol'], sig['market_entry'], sig['sl'], sig['tp1'], sig['tp2'], sig['tp3'], setup_name_to_save, now_ts, now_ts))
        conn.commit()

# ==============================================================================
# 4. HÀM THỐNG KÊ LÃI LỖ THEO SETUP
# ==============================================================================
def get_setup_statistics_report() -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT setup_name, status, COUNT(*) 
            FROM active_trades 
            WHERE status IN ('CLOSED_TP1', 'CLOSED_TP2', 'CLOSED_TP3', 'CLOSED_SL')
            GROUP BY setup_name, status
        """)
        rows = cursor.fetchall()

    if not rows:
        return "📊 <b>THỐNG KÊ HIỆU SUẤT TRADING</b>\n\n🔎 Chưa có dữ liệu lịch sử kèo đã đóng."

    stats = {}
    for setup_name, status, count in rows:
        if setup_name not in stats:
            stats[setup_name] = {'win': 0, 'loss': 0, 'total': 0}
        
        if status in ['CLOSED_TP1', 'CLOSED_TP2', 'CLOSED_TP3']:
            stats[setup_name]['win'] += count
        elif status == 'CLOSED_SL':
            stats[setup_name]['loss'] += count
        
        stats[setup_name]['total'] += count

    msg = "📊 <b>THỐNG KÊ HIỆU SUẤT LÃI/LỖ TỔNG HỢP</b>\n"
    msg += "===================================="

    for setup, data in stats.items():
        total = data['total']
        win = data['win']
        loss = data['loss']
        winrate = (win / total * 100) if total > 0 else 0.0

        msg += f"\n\n⚙️ <b>Tín hiệu {setup}:</b>"
        msg += f"\n• Tổng số lệnh: <b>{total}</b>"
        msg += f"\n• Thắng (TP): ✅ <b>{win}</b> | Thua (SL): 🛑 <b>{loss}</b>"
        msg += f"\n• Tỷ lệ thắng (Winrate): 🔥 <b>{winrate:.1f}%</b>"

    msg += "\n\n<i>💡 Nhắn /tonghop để xem chi tiết danh sách kèo 24h.</i>"
    return msg

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
            tag = "🚀🚀 <b>[ĐÃ TP3 (+20%)]</b>" if status == 'CLOSED_TP3' else "🚀 <b>[ĐÃ TP2 (+10%)]</b>" if status == 'CLOSED_TP2' else "✅ <b>[ĐÃ TP1 (+5%)]</b>" if status == 'CLOSED_TP1' else "🛑 <b>[ĐÃ SL]</b>"
            msg += f"\n{idx}. #{sym} {tag} | Entry: <code>{entry:.4f}</code>"

    if open_trades:
        msg += "\n\n⏳ <b>KÈO ĐANG THEO DÕI (ACTIVE):</b>"
        for idx, t in enumerate(open_trades, 1):
            sym, entry, sl, tp1, tp2, tp3, tp1_hit = t
            st = "✅ (Đã cắn TP1 -> Dời SL về Entry)" if tp1_hit == 1 else "⏳ (Đang chạy)"
            msg += f"\n{idx}. #{sym} {st} | Entry: <code>{entry:.4f}</code> | SL: <code>{sl:.4f}</code>"

    msg += "\n\n<i>💡 Nhắn /thongke để xem tỷ lệ Winrate tổng hợp.</i>"
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
                    text = message.get("text", "").strip().lower()
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    target_chat = chat_id if chat_id else CHAT_ID

                    if text in ["/tonghop", "tonghop"]:
                        send_direct_message(get_24h_summary_report(), target_chat_id=target_chat)
                    elif text in ["/thongke", "thongke", "/setup_stats"]:
                        send_direct_message(get_setup_statistics_report(), target_chat_id=target_chat)
        except Exception:
            pass
        time.sleep(2)

# ==============================================================================
# 5. LẤY TICKERS BYBIT & KLINE DATA
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

                    if not is_non_crypto and turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT:
                        valid.append({
                            "symbol": sym,
                            "price24hPcnt": round(price_change_pct, 2),
                            "turnover24h": turnover,
                            "lastPrice": last_price,
                            "fundingRate": funding_rate,
                            "spread_pct": round(spread_pct, 3)
                        })

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

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 25: return df
    closes, vols = df['close'], df['volume']

    df['ema20'] = closes.ewm(span=20, adjust=False).mean()
    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)

    return df

# ==============================================================================
# 6. ENGINE BẮT ĐÚNG 2 SETUP TỈ LỆ WIN NỔI BẬT (AA+ VÀ A+)
# ==============================================================================

# 🔥 1. SETUP ĐIỂM VÀO AA+ GOLDEN CONVERGENCE (D1 TEST + H4 RECOVERY)
def analyze_golden_convergence_aa(symbol: str, realtime_price: float) -> dict:
    df_d1 = get_closed_klines_df(symbol, "D", limit=30)
    df_h4 = get_closed_klines_df(symbol, "240", limit=30)
    
    if df_d1.empty or df_h4.empty or len(df_d1) < 22 or len(df_h4) < 22:
        return None
        
    df_d1 = add_indicators(df_d1)
    df_h4 = add_indicators(df_h4)
    
    last_d1 = df_d1.iloc[-1]
    last_h4 = df_h4.iloc[-1]
    prev_h4 = df_h4.iloc[-2]
    
    close_now = realtime_price if realtime_price > 0 else last_h4['close']
    ema20_d1 = last_d1['ema20']
    ema20_h4 = last_h4['ema20']
    
    # Giá D1 nhúng râu test sát EMA20 D1 (<= 2.0%) VÀ H4 vừa bứt phá đóng vượt lại lên EMA20 H4
    is_d1_testing = (last_d1['low'] <= ema20_d1 * 1.02) and (close_now >= ema20_d1 * 0.985)
    is_h4_recovery = (prev_h4['close'] < prev_h4['ema20']) and (close_now > ema20_h4)
    is_strong_vol = last_h4['vol_ratio'] >= 1.5

    if not (is_d1_testing and is_h4_recovery and is_strong_vol):
        return None

    market_entry = close_now
    sl = min(last_h4['low'], last_d1['low']) * 0.997
    if ((market_entry - sl) / market_entry) * 100 < 2.0:
        sl = market_entry * 0.98

    return {
        "timeframe": "D1+H4",
        "status": "BUY SIGNAL AA+",
        "internal_setup_name": "GOLDEN CONVERGENCE (D1 TEST + H4 RECOVERY)",
        "score": 100,
        "close_price": close_now,
        "market_entry": round(market_entry, 4),
        "ema20": round(ema20_h4, 4),
        "sl": round(sl, 4),
        "tp1": round(market_entry * 1.05, 4),
        "tp2": round(market_entry * 1.10, 4),
        "tp3": round(market_entry * 1.20, 4),
        "timestamp": last_h4['timestamp']
    }

# 🔥 2. SETUP A+ H1 EARLY ACCUMULATION (TÍCH LŨY CHẶT SÁT EMA20 H1)
def analyze_h1_early_accumulation_a_plus(df_h1: pd.DataFrame, realtime_price: float) -> dict:
    if df_h1.empty or len(df_h1) < 25: return None

    last_candle = df_h1.iloc[-1]
    close_now = realtime_price if realtime_price > 0 else last_candle['close']
    open_now = last_candle['open']
    ema20_now = last_candle['ema20']

    # Chuỗi nến trước nằm dưới MA20
    prev_under_candles = df_h1.iloc[-12:-4]
    under_ma20_count = sum(1 for _, row in prev_under_candles.iterrows() if row['close'] < row['ema20'])
    is_prev_under_ma20 = under_ma20_count >= 5

    # 3-4 nến gần nhất đứng vững trên EMA20
    recent_3_candles = df_h1.iloc[-3:]
    is_hold_above_ma20 = all(r['close'] >= r['ema20'] for _, r in recent_3_candles.iterrows()) and (close_now >= ema20_now)

    # Khống chế nằm sát nền (cách MA20 <= 1.2%) và chưa tăng dốc (thân nến <= 1.8%)
    dist_from_ma20_pct = ((close_now - ema20_now) / ema20_now) * 100
    is_still_at_base = (0.0 <= dist_from_ma20_pct <= 1.2)
    candle_body_pct = ((close_now - open_now) / open_now) * 100 if open_now > 0 else 0.0
    
    # Volume có sự gia tăng nhẹ chuẩn bị nổ
    is_vol_confirm = last_candle['vol_ratio'] >= 1.3

    if not (is_prev_under_ma20 and is_hold_above_ma20 and is_still_at_base and candle_body_pct <= 1.8 and is_vol_confirm):
        return None

    market_entry = close_now
    sl = min(r['low'] for _, r in recent_3_candles.iterrows()) * 0.997
    if ((market_entry - sl) / market_entry) * 100 < 2.0:
        sl = market_entry * 0.98

    return {
        "timeframe": "H1",
        "status": "BUY SIGNAL A+",
        "internal_setup_name": "H1 EARLY ACCUMULATION (SETUP A+)",
        "score": 98,
        "close_price": close_now,
        "market_entry": round(market_entry, 4),
        "ema20": round(ema20_now, 4),
        "sl": round(sl, 4),
        "tp1": round(market_entry * 1.05, 4),
        "tp2": round(market_entry * 1.10, 4),
        "tp3": round(market_entry * 1.20, 4),
        "timestamp": last_candle['timestamp']
    }

# ==============================================================================
# 7. HỆ THỐNG GỬI TELEGRAM BẢO BẬT BẢN QUYỀN
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    msg = f"""🚨 <b>BYBIT VIP SCANNER ({sig['timeframe']})</b>

<b>Coin:</b> #{sig['symbol']} (24h Change: {sig['price24hPcnt']}%)

⚡️ <b>GIÁ VÀO MARKET NGAY:</b> <code>{sig['market_entry']:.4f}</code>
📉 <b>Đường MA 20 ({sig['timeframe']}):</b> <code>{sig['ema20']:.4f}</code>
🛡 <b>Stop Loss:</b> <code>{sig['sl']:.4f}</code>

🎯 <b>Take Profit Target:</b>
• <b>TP1 (+5%):</b> <code>{sig['tp1']:.4f}</code>
• <b>TP2 (+10%):</b> <code>{sig['tp2']:.4f}</code>
• <b>TP3 (+20%):</b> <code>{sig['tp3']:.4f}</code>

🔗 <a href="https://www.bybit.com/trade/usdt/{sig['symbol']}">Biểu đồ Bybit {sig['symbol']}</a>"""

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
    try:
        requests.post(url, json={"chat_id": dest, "text": text, "parse_mode": "HTML"}, timeout=8)
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
            elif curr_price >= tp3:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_TP3', updated_at = ? WHERE id = ?", (now_ts, trade_id))
                cursor.execute("DELETE FROM sent_signals WHERE symbol = ?", (sym,))
                conn.commit()
                status_changed = True
            elif curr_price <= sl:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_SL', updated_at = ? WHERE id = ?", (now_ts, trade_id))
                cursor.execute("DELETE FROM sent_signals WHERE symbol = ?", (sym,))
                conn.commit()
                status_changed = True

    if status_changed:
        send_direct_message(get_24h_summary_report())

# ==============================================================================
# 8. AUTOMATIC SCANNER LOOP
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=listen_telegram_commands, daemon=True).start()

    print("⚡️ Bắt đầu VIP High Winrate Scanner Engine...", flush=True)
    while True:
        try:
            tickers = get_bybit_crypto_tickers()
            tickers_map = {t['symbol']: t['lastPrice'] for t in tickers}
            track_and_update_active_trades(tickers_map)

            print(f"\n🔍 [VIP SCANNER] Đang lọc {len(tickers)} Altcoins thanh khoản cao...", flush=True)

            scanned_signals = []
            for t in tickers[:MAX_SYMBOLS_SCAN]:
                sym = t['symbol']
                realtime_price = t['lastPrice']

                # 1. Bắt Setup AA+ Golden Convergence (D1 + H4)
                aa_sig = analyze_golden_convergence_aa(sym, realtime_price)
                if aa_sig:
                    aa_sig['symbol'] = sym
                    aa_sig['price24hPcnt'] = t['price24hPcnt']
                    scanned_signals.append(aa_sig)

                # 2. Bắt Setup A+ Early Accumulation (H1)
                df_h1 = get_closed_klines_df(sym, "60", limit=40)
                if not df_h1.empty:
                    df_h1 = add_indicators(df_h1)
                    a_plus_sig = analyze_h1_early_accumulation_a_plus(df_h1, realtime_price)
                    if a_plus_sig:
                        a_plus_sig['symbol'] = sym
                        a_plus_sig['price24hPcnt'] = t['price24hPcnt']
                        scanned_signals.append(a_plus_sig)

            scanned_signals.sort(key=lambda x: x['score'], reverse=True)
            top_signals = scanned_signals[:MAX_ALERTS_PER_CYCLE]

            sent_count = 0
            if top_signals:
                for sig in top_signals:
                    if get_active_trades_count() >= MAX_ACTIVE_TRADES and not is_symbol_currently_active(sig['symbol']):
                        print(f"⚠️ [SLOT FULL] Đã đủ {MAX_ACTIVE_TRADES} kèo chuẩn active, tạm dừng nhận kèo mới.", flush=True)
                        break

                    should_send, call_count = process_signal_dedup(sig['symbol'], sig['status'], sig['score'])
                    if should_send:
                        sig['call_count'] = call_count
                        if send_telegram_alert(sig):
                            save_active_trade(sig)
                            sent_count += 1
                            print(f"✅ [VIP ALERT TOP {sent_count}] #{sig['symbol']} Status: {sig['status']} | Entry: {sig['market_entry']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_scanner_loop()
