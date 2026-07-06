CREATE TABLE price_ticks (
    time DATETIME NOT NULL PRIMARY KEY,
    last_price REAL NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
