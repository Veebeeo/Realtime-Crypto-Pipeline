import json
import time
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

DB_URL = "postgresql://crypto_user:crypto_password@127.0.0.1:5454/crypto_db"
engine = create_engine(DB_URL)

BATCH_SIZE = 50
RETRY_DELAY = 5

def consume_and_load():
    consumer = KafkaConsumer(
        'crypto_prices',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting Consumer, Listening to Kafka topic 'crypto_prices'")
    print("Connected to PostgreSQL database. Waiting for data")

    batch = []

    for message in consumer:
        trade_data = message.value
        batch.append(trade_data)

        if len(batch) >= BATCH_SIZE:

            try:

                df = pd.DataFrame(batch)
                df['event_time'] = pd.to_datetime(df['event_time'], unit='ms')

                df['trade_value'] = df['price'] * df['quantity']
                df.to_sql('btc_trades', engine, if_exists='append', index=False)

                latest = df.iloc[-1]

                timestamp = latest['event_time'].strftime('%Y-%m-%d %H:%M:%S')
                price = latest['price']
                print(f"[{timestamp}] Loaded {len(batch)} trades | Latest BTC: ${price:,.2f}")

                batch = []
            
            except Exception as e:
                print(f"Error loading batch: {e}")
                print(f"Retrying in {RETRY_DELAY}s... ({len(batch)} trades buffered)")
                time.sleep(RETRY_DELAY)



if __name__ == "__main__":
    consume_and_load()