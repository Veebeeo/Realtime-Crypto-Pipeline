# Real-Time Cryptocurrency Data Pipeline & Trading Dashboard

An end-to-end, distributed streaming data pipeline that ingests, processes, and visualizes high-frequency cryptocurrency trades in real-time. 

This project captures live BTC/USDT market data via WebSocket, streams it through an enterprise message broker, stores it in a time-series optimized database, and feeds a live-updating analytics dashboard.

## Architecture

![Architecture Diagram](Architecture.svg)

1. **Extraction (Producer):** A Python script connects to the Binance WebSocket API, capturing sub-second trade execution data.
2. **Message Broker (Kafka):** Apache Kafka handles the high-throughput stream, decoupling ingestion from processing to ensure fault tolerance.
3. **Transformation & Load (Consumer):** A Python consumer batches incoming messages, calculates total trade values using Pandas, and loads the data into the database.
4. **Storage (PostgreSQL):** A Dockerized database stores the raw trades with indexing optimized for time-series querying.
5. **Analytics (Power BI):** A DirectQuery dashboard provides a live, rolling 15-minute window of market movements, VWAP, and volatility.

## Tech Stack
* **Language:** Python (Pandas, WebSocket-client)
* **Message Broker:** Apache Kafka & Zookeeper
* **Database:** PostgreSQL
* **Infrastructure:** Docker & Docker Compose
* **Visualization:** Power BI (DirectQuery & DAX)

## Live Dashboard Features

https://github.com/user-attachments/assets/da922faa-2db0-42ac-95b6-206e3be79797


* **Sub-Second Latency:** Visuals update continuously using DirectQuery and custom DAX time-filtering.
* **Volume-Weighted Average Price (VWAP):** Real-time calculation of true average market price.
* **Volatility Tracking:** Live gauge showing the percentage spread between the session high and low.
* **Whale Tracking:** Minute-binned logarithmic volume charts to easily spot massive block trades.

## How to Run Locally

**1. Clone the repository**
```bash
git clone [https://github.com/Veebeeo/realtime-crypto-pipeline.git](https://github.com/Veebeeo/realtime-crypto-pipeline.git)
cd realtime-crypto-pipeline
```

**2. Spin up the infrastructure**
Start Zookeeper, Kafka, and PostgreSQL using Docker Compose:
```bash
docker-compose up -d
```

**3. Setup the Python Environment**
```bash
pip install -r requirements.txt
```

Note: Make sure to create a .env file in the root directory with your PostgreSQL connection string:
DATABASE_URL=postgresql://user_name:password@127.0.0.1:5454/db_name

**4. Start the Data Pipeline**
Open two separate terminals. In the first, start the Kafka Producer:
```bash
python producer.py
```
In the second, start the Kafka Consumer:
```bash
python consumer.py
```

**5. View the Dashboard**
Open Crypto_Dashboard.pbix in Power BI Desktop. The dashboard is configured to DirectQuery the local PostgreSQL instance and will begin animating as soon as the Python scripts inject data.

## Key Learnings
* Managing time-zone offsets (UTC vs. IST) between cloud APIs, databases, and visualization tools.
* Overcoming DirectQuery limitations by substituting DAX columns with native Power BI Data Bins.
* Tuning Kafka consumer batch sizes to balance database write-performance with live dashboard fluidity.

