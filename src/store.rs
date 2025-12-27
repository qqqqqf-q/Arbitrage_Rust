use std::collections::HashMap;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::domain::{OrderBook, Ticker};

pub struct TickerStore {
    id_to_pair: Vec<String>,
    pair_to_id: HashMap<String, usize>,
    bid_bits: Vec<AtomicU64>,
    ask_bits: Vec<AtomicU64>,
    valid: Vec<AtomicBool>,
    valid_count: AtomicU64,
    last_update_ms: AtomicU64,
    update_seq: AtomicU64,
}

impl TickerStore {
    pub fn new(pairs: Vec<String>) -> Self {
        let pair_count = pairs.len();
        let mut pair_to_id = HashMap::with_capacity(pairs.len());
        for (i, p) in pairs.iter().enumerate() {
            pair_to_id.insert(p.clone(), i);
        }

        Self {
            id_to_pair: pairs,
            pair_to_id,
            bid_bits: (0..pair_count).map(|_| AtomicU64::new(0)).collect(),
            ask_bits: (0..pair_count).map(|_| AtomicU64::new(0)).collect(),
            valid: (0..pair_count).map(|_| AtomicBool::new(false)).collect(),
            valid_count: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
            update_seq: AtomicU64::new(0),
        }
    }

    pub fn pair_count(&self) -> usize {
        self.id_to_pair.len()
    }

    pub fn valid_count(&self) -> usize {
        self.valid_count.load(Ordering::Relaxed) as usize
    }

    pub fn last_update_ms(&self) -> u64 {
        self.last_update_ms.load(Ordering::Relaxed)
    }

    pub fn update_seq(&self) -> u64 {
        self.update_seq.load(Ordering::Relaxed)
    }

    pub fn pair_name(&self, pair_id: usize) -> Option<&str> {
        self.id_to_pair.get(pair_id).map(|s| s.as_str())
    }

    pub fn pair_id(&self, pair: &str) -> Option<usize> {
        self.pair_to_id.get(pair).copied()
    }

    pub fn update_by_id(&self, pair_id: usize, bid: f64, ask: f64, now_ms: u64) {
        if pair_id >= self.id_to_pair.len() {
            return;
        }

        self.bid_bits[pair_id].store(bid.to_bits(), Ordering::Relaxed);
        self.ask_bits[pair_id].store(ask.to_bits(), Ordering::Relaxed);
        self.last_update_ms.store(now_ms, Ordering::Relaxed);
        self.update_seq.fetch_add(1, Ordering::Relaxed);

        let new_valid = is_valid_ticker(bid, ask);
        let old_valid = self.valid[pair_id].swap(new_valid, Ordering::Relaxed);
        if old_valid != new_valid {
            if new_valid {
                self.valid_count.fetch_add(1, Ordering::Relaxed);
            } else {
                self.valid_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    pub fn get_by_id(&self, pair_id: usize) -> Option<Ticker> {
        if pair_id >= self.id_to_pair.len() {
            return None;
        }
        let bid = f64::from_bits(self.bid_bits[pair_id].load(Ordering::Relaxed));
        let ask = f64::from_bits(self.ask_bits[pair_id].load(Ordering::Relaxed));
        if is_valid_ticker(bid, ask) {
            Some(Ticker { bid, ask })
        } else {
            None
        }
    }

    pub fn get_by_pair(&self, pair: &str) -> Option<Ticker> {
        let id = self.pair_id(pair)?;
        self.get_by_id(id)
    }
}

fn is_valid_ticker(bid: f64, ask: f64) -> bool {
    bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask >= bid
}

pub struct OrderBookStore {
    id_to_pair: Vec<String>,
    pair_to_id: HashMap<String, usize>,
    books: Vec<StdRwLock<Option<OrderBook>>>,
    last_update_ms: Vec<AtomicU64>,
    valid: Vec<AtomicBool>,
    valid_count: AtomicU64,
}

impl OrderBookStore {
    pub fn new(pairs: Vec<String>) -> Self {
        let pair_count = pairs.len();
        let mut pair_to_id = HashMap::with_capacity(pairs.len());
        for (i, p) in pairs.iter().enumerate() {
            pair_to_id.insert(p.clone(), i);
        }

        Self {
            id_to_pair: pairs,
            pair_to_id,
            books: (0..pair_count).map(|_| StdRwLock::new(None)).collect(),
            last_update_ms: (0..pair_count).map(|_| AtomicU64::new(0)).collect(),
            valid: (0..pair_count).map(|_| AtomicBool::new(false)).collect(),
            valid_count: AtomicU64::new(0),
        }
    }

    pub fn pair_count(&self) -> usize {
        self.id_to_pair.len()
    }

    pub fn valid_count(&self) -> usize {
        self.valid_count.load(Ordering::Relaxed) as usize
    }

    pub fn pair_id(&self, pair: &str) -> Option<usize> {
        self.pair_to_id.get(pair).copied()
    }

    pub fn pair_name(&self, pair_id: usize) -> Option<&str> {
        self.id_to_pair.get(pair_id).map(|s| s.as_str())
    }

    pub fn last_update_ms_by_id(&self, pair_id: usize) -> u64 {
        if pair_id >= self.id_to_pair.len() {
            return 0;
        }
        self.last_update_ms[pair_id].load(Ordering::Relaxed)
    }

    pub fn get_by_id(&self, pair_id: usize) -> Option<OrderBook> {
        if pair_id >= self.id_to_pair.len() {
            return None;
        }
        let guard = self.books[pair_id].read().ok()?;
        guard.clone()
    }

    pub fn get_by_pair(&self, pair: &str) -> Option<OrderBook> {
        let id = self.pair_id(pair)?;
        self.get_by_id(id)
    }

    pub fn update_by_id(&self, pair_id: usize, book: OrderBook, now_ms: u64) {
        if pair_id >= self.id_to_pair.len() {
            return;
        }

        if let Ok(mut guard) = self.books[pair_id].write() {
            *guard = Some(book);
        }
        self.last_update_ms[pair_id].store(now_ms, Ordering::Relaxed);

        let old_valid = self.valid[pair_id].swap(true, Ordering::Relaxed);
        if !old_valid {
            self.valid_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}
