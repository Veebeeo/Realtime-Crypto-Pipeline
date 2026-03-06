import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

DB_URL = "postgresql://crypto_user:crypto_password@127.0.0.1:5454/crypto_db"
engine = create_engine(DB_URL)

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

    for message in consumer:
        trade_data = message.value

        df = pd.DataFrame([trade_data])
        df['event_time'] = pd.to_datetime(df['event_time'], unit='ms')

        df['trade_value'] = df['price'] * df['quantity']

        timestamp = df['event_time'][0].strftime('%Y-%m-%d %H:%M:%S')
        price = df['price'][0]
        value = df['trade_value'][0]
        print(f"[{timestamp}] BTC Price: ${price:,.2f} | Trade Value: ${value:,.2f} -> Loading to DB")

        df.to_sql('btc_trades', engine, if_exists='append', index=False)

if __name__ == "__main__":
    consume_and_load()