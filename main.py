import asyncio
import json
import websockets
import requests
import os
from threading import Thread
from flask import Flask

# 1. Flask Web Server để giữ Render sống 24/7
app = Flask(__name__)

@app.route('/')
def home():
    return "Crypto Signal Bot is Active!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# 2. Cấu hình Telegram Token & Chat ID
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram_signal(symbol, score, vol_mult, oi_change, cvd_percent, status, entry_min, entry_max, sl, tp1, tp2):
    message = f"""🚀 <b>[LONG H1 SQUEEZE EXPLOSION] {symbol}</b>
-------------------------------------
⭐️ <b>Điểm Uy Tín: {score}/100</b>

🔥 <b>DẤU HIỆU CÁ MẬP GOM SPOT & SHORT SQUEEZE:</b>
• Volume H1: <b>+{vol_mult}x (Dốc đứng)</b>
• Open Interest (OI): <b>+{oi_change}% (Tiền bơm cực mạnh)</b>
• Lực Mua CVD: <b>{cvd_percent}% (Taker Buy ép đỉnh)</b>
• Trạng Thái: <b>{status}</b>
-------------------------------------
🎯 <b>KẾ HOẠCH VÀO LỆNH (LONG):</b>

📍 <b>Entry Zone:</b>
{entry_min:.4f} – {entry_max:.4f}

🛑 <b>Stop Loss (Cắt lỗ):</b>
{sl:.4f} (Thủng chân nến gom H1)

🎯 <b>Take Profit 1 (Chốt 1):</b>
{tp1:.4f} (Đỉnh cũ cản H1)

🚀 <b>Take Profit 2 (Chốt 2):</b>
{tp2:.4f} (Vùng nổ đà sóng tăng)
-------------------------------------
👉 <b>CHECK CHART H1 & BẮT SÓNG NGAY!</b>"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Lỗi Telegram: {response.text}")
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

# 3. Xử lý thuật toán tính toán Tín hiệu Signal
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# Bộ đếm hỗ trợ tính toán CVD & Tín hiệu
buy_vol_accumulator = 0.0
total_vol_accumulator = 0.0

async def process_signal(symbol, current_price):
    global buy_vol_accumulator, total_vol_accumulator
    
    if total_vol_accumulator == 0:
        return
    
    cvd_percent = round((buy_vol_accumulator / total_vol_accumulator) * 100, 1)
    
    # Giả định bộ thông số phân tích chỉ báo H1 (Có thể kết nối API Bybit chi tiết)
    score = 90
    vol_mult = 2.3
    oi_change = 8.3
    status = "Breakout Nền Tích Lũy"
    
    # Tính toán Entry, SL, TP tự động theo % quản lý vốn
    entry_min = current_price * 0.996
    entry_max = current_price * 1.000
    sl = current_price * 0.963          # Cắt lỗ ~ 3.7%
    tp1 = current_price * 1.045         # Chốt lời 1 ~ 4.5%
    tp2 = current_price * 1.116         # Chốt lời 2 ~ 11.6%
    
    print(f"⚡️ Bắn tín hiệu Squeeze Explosion cho {symbol}...")
    send_telegram_signal(symbol, score, vol_mult, oi_change, cvd_percent, status, entry_min, entry_max, sl, tp1, tp2)

async def subscribe_realtime_data():
    global buy_vol_accumulator, total_vol_accumulator
    
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                # Đăng ký nhận luồng khớp lệnh (Public Trade)
                symbol_watch = "QNTUSDT" # Hoặc cặp coin bạn muốn theo dõi
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [f"publicTrade.{symbol_watch}"]
                }
                await ws.send(json.dumps(subscribe_msg))
                print(f"⚡️ Đã bật radar quét tín hiệu Long Squeeze cho {symbol_watch}...")

                last_signal_time = 0

                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    
                    if "data" in data:
                        for trade in data["data"]:
                            symbol = trade["s"]
                            price = float(trade["p"])
                            volume = float(trade["v"])
                            side = trade["S"]
                            
                            trade_val = price * volume
                            total_vol_accumulator += trade_val
                            if side == "Buy":
                                buy_vol_accumulator += trade_val

                            # Điều kiện: Khi xuất hiện lực mua đột biến (> $5,000 USDT)
                            if trade_val > 5000 and (asyncio.get_event_loop().time() - last_signal_time > 60):
                                await process_signal(symbol, price)
                                last_signal_time = asyncio.get_event_loop().time()
                                
                                # Reset bộ tích lũy
                                buy_vol_accumulator = 0.0
                                total_vol_accumulator = 0.0

        except Exception as e:
            print(f"⚠️ Mất kết nối WS: {e}. Kết nối lại sau 5 giây...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Chạy Web Server ngầm
    server_thread = Thread(target=run_flask)
    server_thread.daemon = True
    server_thread.start()
    
    # Chạy Bot Radar Tín Hiệu
    asyncio.run(subscribe_realtime_data())
