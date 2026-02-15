use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PerfCounters {
    pub ws_msg_count: AtomicU64,
    pub ws_msg_bytes: AtomicU64,
    pub ws_parse_ns_sum: AtomicU64,
    pub ws_parse_ns_max: AtomicU64,
    pub ws_apply_ns_sum: AtomicU64,
    pub ws_apply_ns_max: AtomicU64,
}

impl PerfCounters {
    pub fn record_ws_message(&self, bytes_len: usize, parse_ns: u64, apply_ns: u64) {
        self.ws_msg_count.fetch_add(1, Ordering::Relaxed);
        self.ws_msg_bytes
            .fetch_add(bytes_len as u64, Ordering::Relaxed);

        self.ws_parse_ns_sum.fetch_add(parse_ns, Ordering::Relaxed);
        update_max(&self.ws_parse_ns_max, parse_ns);

        self.ws_apply_ns_sum.fetch_add(apply_ns, Ordering::Relaxed);
        update_max(&self.ws_apply_ns_max, apply_ns);
    }

    pub fn ws_avg_parse_ns(&self) -> u64 {
        let c = self.ws_msg_count.load(Ordering::Relaxed);
        if c == 0 {
            return 0;
        }
        self.ws_parse_ns_sum.load(Ordering::Relaxed) / c
    }

    pub fn ws_avg_apply_ns(&self) -> u64 {
        let c = self.ws_msg_count.load(Ordering::Relaxed);
        if c == 0 {
            return 0;
        }
        self.ws_apply_ns_sum.load(Ordering::Relaxed) / c
    }

    pub fn reset_ws(&self) {
        self.ws_msg_count.store(0, Ordering::Relaxed);
        self.ws_msg_bytes.store(0, Ordering::Relaxed);
        self.ws_parse_ns_sum.store(0, Ordering::Relaxed);
        self.ws_parse_ns_max.store(0, Ordering::Relaxed);
        self.ws_apply_ns_sum.store(0, Ordering::Relaxed);
        self.ws_apply_ns_max.store(0, Ordering::Relaxed);
    }
}

fn update_max(cell: &AtomicU64, v: u64) {
    let mut cur = cell.load(Ordering::Relaxed);
    while v > cur {
        match cell.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => cur = next,
        }
    }
}
