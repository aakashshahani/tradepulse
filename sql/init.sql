-- TradePulse schema (Phase 1)
-- Loaded by the postgres container on first startup via
-- /docker-entrypoint-initdb.d. Phase 3+ writes into these tables.

-- OHLCV candles aggregated per symbol per time window.
CREATE TABLE IF NOT EXISTS candles (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol       TEXT           NOT NULL,
    window_start TIMESTAMPTZ    NOT NULL,
    window_end   TIMESTAMPTZ    NOT NULL,
    open         NUMERIC(20, 8) NOT NULL,
    high         NUMERIC(20, 8) NOT NULL,
    low          NUMERIC(20, 8) NOT NULL,
    close        NUMERIC(20, 8) NOT NULL,
    volume       NUMERIC(30, 8) NOT NULL,
    -- One candle per symbol per window. window_end is derived from
    -- window_start, so this is the natural key and the ON CONFLICT target.
    UNIQUE (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_window
    ON candles (symbol, window_start DESC);

-- Price-movement alerts derived from candle/trade activity.
CREATE TABLE IF NOT EXISTS alerts (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol     TEXT           NOT NULL,
    ts         TIMESTAMPTZ    NOT NULL,
    price      NUMERIC(20, 8) NOT NULL,
    pct_change NUMERIC(10, 4) NOT NULL,
    message    TEXT           NOT NULL,
    -- ts is the candle's window_end, so one alert per symbol per window.
    -- This is the ON CONFLICT target for idempotent replay.
    UNIQUE (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_alerts_symbol_ts
    ON alerts (symbol, ts DESC);

-- Optional short-term raw trade buffer.
CREATE TABLE IF NOT EXISTS raw_trades (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT           NOT NULL,
    ts     TIMESTAMPTZ    NOT NULL,
    price  NUMERIC(20, 8) NOT NULL,
    size   NUMERIC(30, 8) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_ts
    ON raw_trades (symbol, ts DESC);
