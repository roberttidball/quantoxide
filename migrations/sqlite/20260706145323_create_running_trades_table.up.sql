CREATE TABLE running_trades (
    account_id BLOB NOT NULL CHECK (length(account_id) = 16),
    trade_id BLOB NOT NULL CHECK (length(trade_id) = 16),
    trailing_stoploss REAL,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (account_id, trade_id),
    CONSTRAINT trailing_stoploss_bounded CHECK (
        trailing_stoploss IS NULL OR (trailing_stoploss >= 0.1 AND trailing_stoploss <= 99.9)
    )
);
