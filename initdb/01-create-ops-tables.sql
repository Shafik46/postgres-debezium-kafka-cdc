CREATE SCHEMA IF NOT EXISTS ops_sys;

CREATE TABLE IF NOT EXISTS ops_sys.order_lifecycle (
    order_id             UUID            PRIMARY KEY,
    customer_id          VARCHAR(32)     NOT NULL,
    merchant_id          VARCHAR(32)     NOT NULL,
    order_amount         NUMERIC(10, 2)  NOT NULL,
    currency_code        CHAR(3)         NOT NULL DEFAULT 'USD',
    payment_status       VARCHAR(32)     NOT NULL,
    fulfillment_status   VARCHAR(32)     NOT NULL,
    delivery_status      VARCHAR(32)     NOT NULL,
    current_stage        VARCHAR(64)     NOT NULL,
    city                 VARCHAR(64)     NOT NULL,
    created_at           TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops_sys.order_events (
    event_id             BIGSERIAL       PRIMARY KEY,
    order_id             UUID            NOT NULL REFERENCES ops_sys.order_lifecycle(order_id),
    event_type           VARCHAR(64)     NOT NULL,
    event_status         VARCHAR(64)     NOT NULL,
    event_notes          TEXT,
    event_time           TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_id
    ON ops_sys.order_events(order_id);

CREATE INDEX IF NOT EXISTS idx_order_events_event_time
    ON ops_sys.order_events(event_time);
