import asyncio
import json
import websockets
import requests
import os
from threading import Thread
from flask import Flask

# 1. Tạo Web Server giả lập để Render không báo lỗi "No open ports detected"
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot Realtime is running 24/7!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# 2. Cấu hình Telegram
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram_signal(symbol, price, volume, side):
    message = f"""🚀 <b>[REAL-TIME ALERT] {symbol}</b>
-------------------------------------
🔥 <b>DẤU HIỆU BIẾN ĐỘNG MẠNH:</b>
• Phe thực hiện: <b>{side}</b>
• Giá hiện tại: <b>{price}</b>
• Kích thước lệnh (Volume): <b>{volume}</b>
-------------------------------------
👉 <b>CHECK CHART NGAY!</b>"""
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Lỗi gửi tin nhắn: {response.text}")
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

# 3. Kết nối WebSocket Real-time Bybit
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

async def subscribe_realtime_data():
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                subscribe_msg = {
                    "op": "subscribe",
                    "args": ["publicTrade.GRASSUSDT"]
                }
                await ws.send(json.dumps(subscribe_msg))
                print("⚡️ Đã kết nối WebSocket Bybit Real-time thành công...")

                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    
                    if "data" in data:
                        for trade in data["data"]:
                            symbol = trade["s"]
                            price = float(trade["p"])
                            volume = float(trade["v"])
                            side = trade["S"]
                            
                            # Lọc đơn hàng > 10,000 USDT
                            trade_value = price * volume
                            if trade_value > 10000:
                                print(f"🔥 Lệnh lớn: {side} {symbol} - ${trade_value:,.2f}")
                                send_telegram_signal(symbol, price, volume, side)

        except Exception as e:
            print(f"⚠️ Mất kết nối: {e}. Đang thử kết nối lại sau 5 giây...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Chạy Web server ở luồng riêng (Background Thread)
    server_thread = Thread(target=run_flask)
    server_thread.daemon = True
    server_thread.start()
    
    # Chạy WebSocket lắng nghe Real-time
    asyncio.run(subscribe_realtime_data())
