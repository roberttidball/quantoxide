CREATE TABLE ohlc_candles (
    time DATETIME NOT NULL PRIMARY KEY,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    gap INTEGER NOT NULL DEFAULT 0 CHECK (gap IN (0, 1)),
    stable INTEGER NOT NULL DEFAULT 1 CHECK (stable IN (0, 1))
);

CREATE INDEX idx_ohlc_candles_gaps ON ohlc_candles (gap)
WHERE gap = 1;

CREATE TRIGGER trigger_ohlc_candles_updated_at
AFTER UPDATE ON ohlc_candles
FOR EACH ROW
WHEN
    OLD.open IS NOT NEW.open OR
    OLD.high IS NOT NEW.high OR
    OLD.low IS NOT NEW.low OR
    OLD.close IS NOT NEW.close OR
    OLD.volume IS NOT NEW.volume OR
    OLD.gap IS NOT NEW.gap OR
    OLD.stable IS NOT NEW.stable
BEGIN
    UPDATE ohlc_candles
    SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE time = NEW.time;
END;
