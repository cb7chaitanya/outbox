//! Low-cardinality process metrics shared by service consumers.

use std::sync::atomic::{AtomicU64, Ordering};

static APPLIED: AtomicU64 = AtomicU64::new(0);
static DUPLICATE: AtomicU64 = AtomicU64::new(0);
static STALE: AtomicU64 = AtomicU64::new(0);
static POISON: AtomicU64 = AtomicU64::new(0);
static GAPS: AtomicU64 = AtomicU64::new(0);
static DLQ: AtomicU64 = AtomicU64::new(0);
static RETRIES: AtomicU64 = AtomicU64::new(0);
static CONSUMER_LAG: AtomicU64 = AtomicU64::new(0);

pub fn record_result(result: &str) {
    match result {
        "applied" => &APPLIED,
        "duplicate" => &DUPLICATE,
        "stale" => &STALE,
        "poison" => &POISON,
        _ => return,
    }
    .fetch_add(1, Ordering::Relaxed);
}

pub fn record_gap() {
    GAPS.fetch_add(1, Ordering::Relaxed);
}
pub fn record_dlq() {
    DLQ.fetch_add(1, Ordering::Relaxed);
}
pub fn record_retry() {
    RETRIES.fetch_add(1, Ordering::Relaxed);
}
pub fn set_consumer_lag(value: i64) {
    CONSUMER_LAG.store(value.max(0) as u64, Ordering::Relaxed);
}

pub fn prometheus() -> String {
    let mut body = String::new();
    body.push_str("# TYPE consumer_records_total counter\n");
    for (result, value) in [
        ("applied", APPLIED.load(Ordering::Relaxed)),
        ("duplicate", DUPLICATE.load(Ordering::Relaxed)),
        ("stale", STALE.load(Ordering::Relaxed)),
        ("poison", POISON.load(Ordering::Relaxed)),
    ] {
        body.push_str(&format!(
            "consumer_records_total{{result=\"{result}\"}} {value}\n"
        ));
    }
    body.push_str("# TYPE consumer_version_gaps_total counter\n");
    body.push_str(&format!(
        "consumer_version_gaps_total {}\n",
        GAPS.load(Ordering::Relaxed)
    ));
    body.push_str("# TYPE dlq_published_total counter\n");
    body.push_str(&format!(
        "dlq_published_total {}\n",
        DLQ.load(Ordering::Relaxed)
    ));
    body.push_str("# TYPE dependency_retries_total counter\n");
    body.push_str(&format!(
        "dependency_retries_total {}\n",
        RETRIES.load(Ordering::Relaxed)
    ));
    body.push_str("# TYPE consumer_lag_records gauge\n");
    body.push_str(&format!(
        "consumer_lag_records {}\n",
        CONSUMER_LAG.load(Ordering::Relaxed)
    ));
    body
}
