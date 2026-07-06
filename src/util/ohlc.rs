use chrono::{DateTime, Utc};

use crate::db::models::OhlcCandleRow;

/// Accumulates one-minute OHLC rows into a single resolution bucket.
pub(crate) struct OhlcBucketAccumulator {
    bucket_time: DateTime<Utc>,
    first_candle_time: DateTime<Utc>,
    last_candle_time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    min_created_at: DateTime<Utc>,
    max_updated_at: DateTime<Utc>,
    all_stable: bool,
}

impl OhlcBucketAccumulator {
    pub fn new(bucket_time: DateTime<Utc>) -> Self {
        Self {
            bucket_time,
            first_candle_time: DateTime::<Utc>::MAX_UTC,
            last_candle_time: DateTime::<Utc>::MIN_UTC,
            open: 0.0,
            high: f64::MIN,
            low: f64::MAX,
            close: 0.0,
            volume: 0,
            min_created_at: DateTime::<Utc>::MAX_UTC,
            max_updated_at: DateTime::<Utc>::MIN_UTC,
            all_stable: true,
        }
    }

    pub fn bucket_time(&self) -> DateTime<Utc> {
        self.bucket_time
    }

    pub fn add_candle(&mut self, candle: &OhlcCandleRow) {
        if candle.time < self.first_candle_time {
            self.first_candle_time = candle.time;
            self.open = candle.open;
        }

        if candle.time > self.last_candle_time {
            self.last_candle_time = candle.time;
            self.close = candle.close;
        }

        self.high = self.high.max(candle.high);
        self.low = self.low.min(candle.low);
        self.volume += candle.volume;

        self.min_created_at = self.min_created_at.min(candle.created_at);
        self.max_updated_at = self.max_updated_at.max(candle.updated_at);

        self.all_stable = self.all_stable && candle.stable;
    }

    pub fn to_candle_row(&self, is_complete: bool) -> OhlcCandleRow {
        OhlcCandleRow {
            time: self.bucket_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            created_at: self.min_created_at,
            updated_at: self.max_updated_at,
            stable: self.all_stable && is_complete,
        }
    }
}
