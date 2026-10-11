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
    return "Bybit Early Pump Engine (88+ Points & Upgrade Criteria) Active 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# ==============================================================================
# 2. CẤU HÌNH BIẾN MÔI TRƯỜNG & NGƯỠNG LỌC ĐIỂM
# ==============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

HARD_MIN_SCORE = 88                                                  # 🔴 Chỉ báo ngay nếu >= 88 điểm
POTENTIAL_MIN_SCORE = 83                                             # 🟡 Lưu theo dõi ngầm nếu từ 83 đến 87 điểm

MIN_TURNOVER_24H = float(os.environ.get("MIN_TURNOVER_24H", "2000000"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.20"))
MAX_SYMBOLS_SCAN = int(os.environ.get("MAX_SYMBOLS_SCAN", "80"))
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "1800")) 

MAX_ALERTS_PER_CYCLE = 5  # Top 5 kèo có điểm số cao nhất mỗi chu kỳ

DB_PATH = "signals_history.db"
BASE_URL = "https://api.bybit.com"

# ==============================================================================
# 3. SQLITE DATABASE (XỬ LÝ ĐIỂM TIỀM NĂNG 83-87 & NÂNG ĐIỂM LẦN 2)
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
                has_been_alerted INTEGER DEFAULT 0,
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
                created_at INTEGER NOT NULL
            )
        """)
        conn.commit()

def process_signal_scoring_logic(symbol: str, status: str, score: int) -> tuple:
    """
    Xử lý logic lọc điểm:
    - Nếu score >= 88 ngay từ đầu: Cho phép báo ngay (has_been_alerted = 1).
    - Nếu 83 <= score < 88 lần đầu: Lưu ngầm theo dõi (has_been_alerted = 0, không báo Telegram).
    - Nếu coin 83-87 điểm ở các chu kỳ sau NÂNG LÊN >= 88 điểm: Cho phép báo Telegram (Call lần 2).
    """
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT call_count, last_sent_at, score, has_been_alerted FROM sent_signals WHERE symbol = ? AND status = ?",
            (symbol, status)
        )
        row = cursor.fetchone()

        if row is None:
            # Lần đầu tiên phát hiện coin
            if score >= HARD_MIN_SCORE:
                # Đạt hẳn >= 88 điểm -> Báo Telegram ngay
                cursor.execute(
                    "INSERT INTO sent_signals (symbol, status, score, call_count, last_sent_at, has_been_alerted) VALUES (?, ?, ?, 1, ?, 1)",
                    (symbol, status, score, now_ts)
                )
                conn.commit()
                return True, 1
            elif score >= POTENTIAL_MIN_SCORE:
                # Đạt từ 83 - 87 điểm -> Lưu ngầm theo dõi tiềm năng, KHÔNG gửi báo ngay
                cursor.execute(
                    "INSERT INTO sent_signals (symbol, status, score, call_count, last_sent_at, has_been_alerted) VALUES (?, ?, ?, 1, ?, 0)",
                    (symbol, status, score, now_ts)
                )
                conn.commit()
                return False, 1
            else:
                return False, 0
        else:
            prev_call_count, last_sent_at, prev_score, has_been_alerted = row
            time_passed = now_ts - last_sent_at

            # Coin cũ trước đó nằm ở vùng tiềm năng (83-87đ) chưa từng được báo
            if has_been_alerted == 0:
                if score >= HARD_MIN_SCORE:
                    # ĐÃ NÂNG ĐIỂM THỰC SỰ TIỀM NĂNG (Nâng lên >= 88đ) -> Cho phép phát báo Telegram ở LẦN 2!
                    new_call_count = prev_call_count + 1
                    cursor.execute(
                        "UPDATE sent_signals SET call_count = ?, last_sent_at = ?, score = ?, has_been_alerted = 1 WHERE symbol = ? AND status = ?",
                        (new_call_count, now_ts, score, symbol, status)
                    )
                    conn.commit()
                    return True, new_call_count
                else:
                    # Vẫn giậm chân ở điểm thấp < 88đ -> Cập nhật lại điểm, tiếp tục theo dõi ngầm
                    cursor.execute(
                        "UPDATE sent_signals SET score = ?, last_sent_at = ? WHERE symbol = ? AND status = ?",
                        (score, now_ts, symbol, status)
                    )
                    conn.commit()
                    return False, prev_call_count

            # Coin đã từng được báo Telegram trước đây (has_been_alerted == 1)
            else:
                if time_passed < COOLDOWN_SECONDS and (score - prev_score) < 5:
                    return False, prev_call_count

                if score >= HARD_MIN_SCORE:
                    new_call_count = prev_call_count + 1
                    cursor.execute(
                        "UPDATE sent_signals SET call_count = ?, last_sent_at = ?, score = ? WHERE symbol = ? AND status = ?",
                        (new_call_count, now_ts, score, symbol, status)
                    )
                    conn.commit()
                    return True, new_call_count
                return False, prev_call_count

def save_active_trade(sig: dict):
    now_ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO active_trades (symbol, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'OPEN', ?)
        """, (sig['symbol'], sig['close_price'], sig['invalidation_sl'], sig['tp1'], sig['tp2'], sig['tp3'], now_ts))
        conn.commit()

def get_summary_report() -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT symbol, entry, sl, tp1, tp2, tp3, tp1_hit FROM active_trades WHERE status = 'OPEN' ORDER BY id DESC")
        trades = cursor.fetchall()

    if not trades:
        return "📋 <b>BÁO CÁO TỔNG HỢP VỊ THẾ ACTIVE</b>\n\n🔎 Hiện tại chưa có kèo nào đang trong danh sách theo dõi."

    msg = f"📊 <b>TỔNG HỢP CÁC KÈO ĐANG ACTIVE ({len(trades)} KÈO)</b>\n"
    msg += "===================================="

    for idx, t in enumerate(trades, 1):
        sym, entry, sl, tp1, tp2, tp3, tp1_hit = t
        if tp1_hit == 1:
            sl_status = f"<code>{entry:.4f}</code> (🛡 Đã cắn TP1 -> Dời SL về Breakeven)"
            status_tag = "✅ <b>[ĐÃ HÍT TP1]</b>"
        else:
            sl_status = f"<code>{sl:.4f}</code>"
            status_tag = "⏳ <b>[ĐANG THEO DÕI]</b>"

        msg += f"""

{idx}. #<b>{sym}</b> {status_tag}
• 🎯 <b>Entry:</b> <code>{entry:.4f}</code>
• 🛡 <b>Stop Loss:</b> {sl_status}
• 🚀 <b>TP1 (+5%):</b> <code>{tp1:.4f}</code>
• 🚀 <b>TP2:</b> <code>{tp2:.4f}</code>
• 🚀 <b>TP3:</b> <code>{tp3:.4f}</code>"""

    msg += "\n\n<i>⚠️ Lưu ý: Khi kèo cắn TP1, SL tự động được dời về Entry (Hòa vốn) để bảo vệ tài khoản.</i>"
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
                        summary_msg = get_summary_report()
                        send_direct_message(summary_msg, target_chat_id=chat_id if chat_id else CHAT_ID)
        except Exception:
            pass
        time.sleep(2)

# ==============================================================================
# 4. BYBIT API V5 HELPERS & CÁC CHỈ BÁO KỸ THUẬT
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != '' else default
    except (ValueError, TypeError):
        return default

def get_usdt_perpetual_tickers():
    url = f"{BASE_URL}/v5/market/tickers?category=linear"
    try:
        res = requests.get(url, timeout=6).json()
        if res.get("retCode") == 0 and res.get("result", {}).get("list"):
            tickers = res["result"]["list"]
            non_crypto = ['XAU', 'XAG', 'WTI', 'USOIL', 'BRENT', 'COPPER', 'SILVER', 'GOLD',
                          'QQQ', 'SPCX', 'SPY', 'DIA', 'IWM', 'TLT', 'NDX', 'SPX',
                          'NVDA', 'TSLA', 'AAPL', 'AMZN', 'META', 'MSFT', 'GOOGL', 'NFLX', '3L', '3S']
            valid = []
            for t in tickers:
                sym = t.get("symbol", "")
                turnover = safe_float(t.get("turnover24h", 0))
                last_price = safe_float(t.get("lastPrice", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                funding_rate = safe_float(t.get("fundingRate", 0)) * 100

                spread_pct = ((ask - bid) / bid * 100) if (bid > 0 and ask > 0) else 0.0

                if sym.endswith("USDT") and not sym.startswith(("USDC", "BUSD", "DAI")):
                    if turnover >= MIN_TURNOVER_24H and spread_pct <= MAX_SPREAD_PCT and not any(k in sym for k in non_crypto):
                        valid.append({
                            "symbol": sym,
                            "turnover24h": turnover,
                            "lastPrice": last_price,
                            "fundingRate": funding_rate,
                            "spread_pct": round(spread_pct, 3)
                        })
            return sorted(valid, key=lambda x: x["turnover24h"], reverse=True)
    except Exception as e:
        print(f"[ERROR] Lỗi lấy Tickers: {e}", flush=True)
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

def get_open_interest_change(symbol: str) -> float:
    url = f"{BASE_URL}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=1h&limit=3"
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
    if df.empty or len(df) < 30: return df
    closes, highs, lows, vols = df['close'], df['high'], df['low'], df['volume']

    df['ema34'] = closes.ewm(span=34, adjust=False).mean()
    df['ema89'] = closes.ewm(span=89, adjust=False).mean()
    df['ema200'] = closes.ewm(span=200, adjust=False).mean()

    tr1 = highs - lows
    tr2 = (highs - closes.shift(1)).abs()
    tr3 = (lows - closes.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr14'] = tr.rolling(14).mean()

    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi14'] = 100 - (100 / (1 + rs))

    bb_mid = closes.rolling(20).mean()
    bb_std = closes.rolling(20).std()
    df['bb_width'] = ((bb_mid + (bb_std * 2)) - (bb_mid - (bb_std * 2))) / bb_mid

    df['vol_ma20'] = vols.rolling(20).mean()
    df['vol_ratio'] = vols / df['vol_ma20'].replace(0, np.nan)

    return df

# ==============================================================================
# 5. ENGINE PHÁT HIỆN TRẠNG THÁI & CHẤM ĐIỂM
# ==============================================================================
def analyze_early_pump(df_m15: pd.DataFrame, df_h1: pd.DataFrame, btc_perf: float) -> dict:
    if df_m15.empty or len(df_m15) < 35: return None

    last_candle = df_m15.iloc[-1]
    prev_candles = df_m15.iloc[-25:-1]

    close_now = last_candle['close']
    vol_ratio_now = last_candle['vol_ratio']
    rsi_now = last_candle['rsi14']
    bb_width_now = last_candle['bb_width']
    atr_now = last_candle['atr14']

    base_high = prev_candles['high'].max()
    base_low = prev_candles['low'].min()
    base_range_pct = ((base_high - base_low) / base_low) * 100

    is_tight_base = base_range_pct <= 12.0
    is_bb_squeezed = bb_width_now <= 0.10

    status = None
    reasons = []

    if close_now > base_high and vol_ratio_now >= 1.2:
        status = "BREAKOUT CONFIRMED"
        reasons.append(f"Đóng nến M15 ({close_now:.4f}) vượt đỉnh nền ({base_high:.4f})")
        reasons.append(f"Volume bắt đầu bùng nổ {vol_ratio_now:.1f}x so với MA20")

    elif is_tight_base and is_bb_squeezed and (close_now >= base_high * 0.980):
        status = "PRE-BREAKOUT"
        reasons.append(f"Nền tích lũy nén chặt {base_range_pct:.1f}% trong 24 nến M15")
        reasons.append("Bollinger Bands siết hẹp, giá chạm sát vùng kháng cự")

    elif close_now <= base_high * 1.01 and close_now >= base_high * 0.985 and vol_ratio_now < 1.3:
        status = "RETEST WATCH"
        reasons.append(f"Giá test lại vùng phá nền ({base_high:.4f}) với Volume cạn")

    if not status: return None

    pump_from_base = ((close_now - base_low) / base_low) * 100
    no_chase = pump_from_base > 12.0

    invalidation_sl = base_low - (0.3 * atr_now)
    risk = close_now - invalidation_sl
    if risk <= 0: return None

    tp1 = max(close_now + (risk * 2.0), close_now * 1.05)
    tp2 = close_now + (risk * 3.5)
    tp3 = close_now + (risk * 5.0)

    score = 0
    if base_range_pct <= 5.0: score += 20
    elif base_range_pct <= 8.0: score += 15
    else: score += 10

    if status == "BREAKOUT CONFIRMED": score += 20
    elif status == "PRE-BREAKOUT": score += 18
    elif status == "RETEST WATCH": score += 15

    if vol_ratio_now >= 2.0: score += 20
    elif vol_ratio_now >= 1.2: score += 15
    else: score += 10

    if 50 <= rsi_now <= 75: score += 15
    else: score += 8

    if not df_h1.empty and len(df_h1) >= 20:
        last_h1 = df_h1.iloc[-1]
        if last_h1['close'] > last_h1['ema34']: score += 10

    coin_perf = ((close_now - df_m15.iloc[0]['close']) / df_m15.iloc[0]['close']) * 100
    if coin_perf > btc_perf + 1.5: score += 10
    else: score += 5

    score += 5

    return {
        "status": status,
        "score": min(100, score),
        "close_price": close_now,
        "base_high": base_high,
        "base_low": base_low,
        "base_range_pct": round(base_range_pct, 2),
        "vol_ratio": round(vol_ratio_now, 2),
        "rsi14": round(rsi_now, 1),
        "retest_zone": f"{base_high * 0.998:.4f} - {base_high * 1.005:.4f}",
        "invalidation_sl": round(invalidation_sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "tp3": round(tp3, 4),
        "rr_ratio": "1:2 / 1:3.5",
        "no_chase": no_chase,
        "reasons": reasons,
        "timestamp": last_candle['timestamp']
    }

# ==============================================================================
# 6. HỆ THỐNG GỬI TELEGRAM & THEO DÕI TP/SL
# ==============================================================================
def send_telegram_alert(sig: dict) -> bool:
    if not BOT_TOKEN or not CHAT_ID: return False

    chase_tag = "\n⚠️ <b>CẢNH BÁO: KHÔNG ĐUỔI GIÁ (Giá đã tăng xa nền)</b>" if sig['no_chase'] else ""

    msg = f"""🚨 <b>BYBIT EARLY PUMP SCANNER</b>

<b>Coin:</b> #{sig['symbol']}
<b>Trạng thái:</b> <code>{sig['status']}</code>
🔔 <b>Lần thông báo:</b> <b>Call lần {sig['call_count']}</b>
⭐ <b>Điểm xếp hạng:</b> <b>{sig['score']}/100</b>
<b>Khung thời gian:</b> M15 + H1

💵 <b>Giá hiện tại:</b> {sig['close_price']:.4f}
📦 <b>Vùng tích lũy:</b> {sig['base_low']:.4f} - {sig['base_high']:.4f} ({sig['base_range_pct']}%)
🎯 <b>Kháng cự Breakout:</b> {sig['base_high']:.4f}
🔄 <b>Vùng Retest tham khảo:</b> {sig['retest_zone']}
🛡 <b>Hỗ trợ / Stop Loss:</b> {sig['invalidation_sl']:.4f}
⛳️ <b>TP1 (+5%):</b> {sig['tp1']:.4f} | <b>TP2:</b> {sig['tp2']:.4f}

📊 <b>Turnover 24h:</b> ${sig['turnover24h']/1e6:.2f}M USD
🔥 <b>Volume / MA20:</b> {sig['vol_ratio']}x
📈 <b>RSI14:</b> {sig['rsi14']} | <b>OI 1h:</b> {sig['oi_info']}
💸 <b>Funding Rate:</b> {sig['funding']:.4f}%

💡 <b>Lý do phát hiện:</b>
• {chr(10).join(['• ' + r for r in sig['reasons']])}{chase_tag}

🔗 <a href="https://www.bybit.com/trade/usdt/{sig['symbol']}">Biểu đồ Bybit {sig['symbol']}</a>

<i>💡 Gõ <b>/tonghop</b> trên Telegram để xem danh sách TP/SL các kèo đang Active.</i>"""

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
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, symbol, entry, sl, tp1, tp2, tp3, tp1_hit FROM active_trades WHERE status = 'OPEN'")
        trades = cursor.fetchall()

        for t in trades:
            trade_id, sym, entry, sl, tp1, tp2, tp3, tp1_hit = t
            if sym not in tickers_map: continue

            curr_price = tickers_map[sym]

            # 1. KIỂM TRA CHẠM TP1
            if tp1_hit == 0 and curr_price >= tp1:
                cursor.execute("UPDATE active_trades SET tp1_hit = 1, sl = ? WHERE id = ?", (entry, trade_id))
                conn.commit()
                msg = f"""🎉 <b>TAKE PROFIT ALERT — TP1 HIT!</b>

#<b>{sym}</b>
✅ Giá đã chạm <b>TP1: {tp1:.4f}</b> (+5.0%+)
🛡 <b>CẬP NHẬT RỦI RO:</b> Đã dời SL về Breakeven (Hòa vốn) tại <b>{entry:.4f}</b>.
👉 <b>Mức Stop Loss ban đầu ({sl:.4f}) KHÔNG CÒN HIỆU LỰC!</b> Vị thế hoàn toàn an toàn."""
                send_direct_message(msg)
                print(f"🎯 [TP1 HIT] #{sym} -> SL updated to Entry", flush=True)

            # 2. KIỂM TRA CHẠM TP2 HOẶC TP3
            elif tp1_hit == 1 and curr_price >= tp2:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_TP2' WHERE id = ?", (trade_id,))
                conn.commit()
                msg = f"""🚀 <b>TAKE PROFIT ALERT — TP2 HIT!</b>

#<b>{sym}</b>
✅ Giá đã chốt lời thành công chặng 2 tại <b>TP2: {tp2:.4f}</b>!
🎉 Chúc mừng bạn đã gặt hái lợi nhuận tối đa!"""
                send_direct_message(msg)

            # 3. KIỂM TRA DÍNH SL (Chỉ xảy ra nếu CHƯA chạm TP1)
            elif tp1_hit == 0 and curr_price <= sl:
                cursor.execute("UPDATE active_trades SET status = 'CLOSED_SL' WHERE id = ?", (trade_id,))
                conn.commit()
                msg = f"""🛑 <b>STOP LOSS ALERT</b>

#<b>{sym}</b>
❌ Giá đã chạm Stop Loss ban đầu tại <b>{sl:.4f}</b>.
Kịch bản bị vô hiệu hóa."""
                send_direct_message(msg)

# ==============================================================================
# 8. AUTOMATIC SCANNER LOOP
# ==============================================================================
def run_scanner_loop():
    init_db()
    Thread(target=listen_telegram_commands, daemon=True).start()

    print("⚡️ Bắt đầu Bybit Early Pump & Dynamic TP/SL Engine (88+ Points Filter)...", flush=True)
    while True:
        try:
            tickers = get_usdt_perpetual_tickers()
            tickers_map = {t['symbol']: t['lastPrice'] for t in tickers}

            # Cập nhật và kiểm tra TP/SL vị thế đang mở
            track_and_update_active_trades(tickers_map)

            target_tickers = tickers[:MAX_SYMBOLS_SCAN]
            print(f"\n🔍 [SCANNER] Đang rà soát {len(target_tickers)} Altcoins thanh khoản cao...", flush=True)

            btc_df = get_closed_klines_df("BTCUSDT", "15", 20)
            btc_perf = ((btc_df.iloc[-1]['close'] - btc_df.iloc[0]['close']) / btc_df.iloc[0]['close']) * 100 if not btc_df.empty else 0.0

            scanned_signals = []
            for t in target_tickers:
                sym = t['symbol']
                df_m15 = get_closed_klines_df(sym, "15", limit=50)
                if df_m15.empty: continue

                df_h1 = get_closed_klines_df(sym, "60", limit=30)
                
                df_m15 = add_indicators(df_m15)
                if not df_h1.empty:
                    df_h1 = add_indicators(df_h1)

                analysis = analyze_early_pump(df_m15, df_h1, btc_perf)
                # Chỉ xử lý nếu điểm đạt từ 83 trở lên (để đưa vào luồng kiểm tra / nâng điểm)
                if analysis and analysis['score'] >= POTENTIAL_MIN_SCORE:
                    oi_change = get_open_interest_change(sym)
                    analysis['symbol'] = sym
                    analysis['turnover24h'] = t['turnover24h']
                    analysis['funding'] = t['fundingRate']
                    analysis['oi_info'] = f"{oi_change:+}%/1h"
                    scanned_signals.append(analysis)

            scanned_signals.sort(key=lambda x: x['score'], reverse=True)
            top_5_signals = scanned_signals[:MAX_ALERTS_PER_CYCLE]

            sent_count = 0
            if top_5_signals:
                for sig in top_5_signals:
                    should_send, call_count = process_signal_scoring_logic(sig['symbol'], sig['status'], sig['score'])
                    if should_send:
                        sig['call_count'] = call_count
                        if send_telegram_alert(sig):
                            save_active_trade(sig)
                            sent_count += 1
                            print(f"✅ [SNIPER TOP {sent_count}] #{sig['symbol']} Status: {sig['status']} | Call lần {call_count} | Score: {sig['score']}", flush=True)

        except Exception as e:
            print(f"[ERROR] Scanner Loop Error: {e}", flush=True)

        print(f"⏳ Hoàn tất chu kỳ. Tạm nghỉ {SCAN_INTERVAL_SECONDS} giây...", flush=True)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    run_scanner_loop()
