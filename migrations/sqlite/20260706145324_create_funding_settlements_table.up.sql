CREATE TABLE funding_settlements (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id) = 16),
    time DATETIME NOT NULL UNIQUE,
    fixing_price REAL NOT NULL,
    funding_rate REAL NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
