import asyncio
import json
import websockets
import requests
import os

# Lấy cấu hình bảo mật từ biến môi trường của Render
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
            print(f"Lỗi gửi tin nhắn Telegram: {response.text}")
    except Exception as e:
        print(f"Lỗi kết nối gửi Telegram: {e}")

# URL WebSocket Bybit Futures (Linear)
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

async def subscribe_realtime_data():
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                # Đăng ký nhận dữ liệu khớp lệnh real-time
                subscribe_msg = {
                    "op": "subscribe",
                    "args": ["publicTrade.GRASSUSDT"]  # Bạn có thể đổi hoặc thêm cặp coin khác tại đây
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
                            side = trade["S"]  # Buy hoặc Sell
                            
                            # Lọc điều kiện: Đơn hàng lớn hơn 10,000 USDT
                            trade_value = price * volume
                            if trade_value > 10000:
                                print(f"🔥 Lệnh lớn: {side} {symbol} - ${trade_value:,.2f}")
                                send_telegram_signal(symbol, price, volume, side)

        except Exception as e:
            print(f"⚠️ Mất kết nối: {e}. Đang thử kết nối lại sau 5 giây...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(subscribe_realtime_data())
