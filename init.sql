-- Active: 1773245824117@@localhost@5454@crypto_db
CREATE TABLE IF NOT EXISTS btc_trades (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    quantity NUMERIC(18, 8) NOT NULL,
    trade_value NUMERIC(24, 8) NOT NULL
);

CREATE INDEX idx_btc_trades_event_time ON btc_trades(event_time DESC);
CREATE INDEX idx_btc_trades_symbol ON btc_trades(symbol);