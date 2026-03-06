import websocket
import json
from kafka import KafkaProducer

#Initializing kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'crypto_prices'
SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"

def on_message(ws, message):
    data = json.loads(message)
    payload = {
        'event_time': data['E'],
        'symbol': data['s'],
        'price': float(data['p']),
        'quantity': float(data['q'])
    }
    producer.send(TOPIC_NAME, value=payload)
    print(f"Published to Kafka Topic '{TOPIC_NAME}': {payload}")

def on_error(ws, error):
    print(f"Error encountered: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket Connection Closed")

def on_open(ws):
    print("WebSocket Connection Opened")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        SOCKET, 
        on_open=on_open,
        on_message=on_message, 
        on_error=on_error, 
        on_close=on_close
    )
    
    print(f"Starting producer... listening to {SOCKET}")
    ws.run_forever()