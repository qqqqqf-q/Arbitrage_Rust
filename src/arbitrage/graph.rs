use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rust_decimal::Decimal;

use crate::domain::Market;
use crate::store::TickerStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Buy,
    Sell,
}

#[derive(Debug)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    pub pair_id: usize,
    pub kind: EdgeKind,
    weight_bits: AtomicU64,
}

#[derive(Debug)]
pub struct Graph {
    pub nodes: Vec<String>,
    pub edges: Vec<Edge>,
    pub adjacency: Vec<Vec<usize>>,
    pub currency_to_index: HashMap<String, usize>,
    pair_edges: Vec<(usize, usize)>, // (buy_edge_idx, sell_edge_idx)
    fee_multiplier_bits: AtomicU64,
    rebuild_all: AtomicBool,
}

pub fn build_static_graph(
    markets: &HashMap<String, Market>,
    pairs: &[String],
    taker_fee_rate: Decimal,
) -> anyhow::Result<Graph> {
    let fee_multiplier = fee_multiplier_from_fee(taker_fee_rate)?;
    let mut currencies = BTreeSet::new();

    for pair in pairs {
        let m = markets
            .get(pair)
            .ok_or_else(|| anyhow::anyhow!("缺少市场数据: {}", pair))?;
        if !m.active || !m.spot {
            continue;
        }

        currencies.insert(m.base.clone());
        currencies.insert(m.quote.clone());
    }

    if currencies.len() < 2 || pairs.is_empty() {
        return Ok(Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            adjacency: Vec::new(),
            currency_to_index: HashMap::new(),
            pair_edges: Vec::new(),
            fee_multiplier_bits: AtomicU64::new(fee_multiplier.to_bits()),
            rebuild_all: AtomicBool::new(false),
        });
    }

    let nodes: Vec<String> = currencies.into_iter().collect();
    let mut currency_to_index = HashMap::with_capacity(nodes.len());
    for (i, c) in nodes.iter().enumerate() {
        currency_to_index.insert(c.clone(), i);
    }

    let mut edges = Vec::with_capacity(pairs.len() * 2);
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    let mut pair_edges = Vec::with_capacity(pairs.len());

    for (pair_id, pair) in pairs.iter().enumerate() {
        let m = markets
            .get(pair)
            .ok_or_else(|| anyhow::anyhow!("缺少市场数据: {}", pair))?;
        let base_idx = currency_to_index[&m.base];
        let quote_idx = currency_to_index[&m.quote];

        let buy_edge_idx = edges.len();
        edges.push(Edge {
            from: quote_idx,
            to: base_idx,
            pair_id,
            kind: EdgeKind::Buy,
            weight_bits: AtomicU64::new(f64::NAN.to_bits()),
        });
        adjacency[quote_idx].push(buy_edge_idx);

        let sell_edge_idx = edges.len();
        edges.push(Edge {
            from: base_idx,
            to: quote_idx,
            pair_id,
            kind: EdgeKind::Sell,
            weight_bits: AtomicU64::new(f64::NAN.to_bits()),
        });
        adjacency[base_idx].push(sell_edge_idx);

        pair_edges.push((buy_edge_idx, sell_edge_idx));
    }

    Ok(Graph {
        nodes,
        edges,
        adjacency,
        currency_to_index,
        pair_edges,
        fee_multiplier_bits: AtomicU64::new(fee_multiplier.to_bits()),
        rebuild_all: AtomicBool::new(true),
    })
}

impl Graph {
    pub fn set_fee_rate(&self, taker_fee_rate: Decimal) -> anyhow::Result<()> {
        let fee_multiplier = fee_multiplier_from_fee(taker_fee_rate)?;
        self.fee_multiplier_bits
            .store(fee_multiplier.to_bits(), Ordering::Relaxed);
        self.rebuild_all.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn maybe_rebuild_all_weights(&self, store: &TickerStore) {
        if !self.rebuild_all.swap(false, Ordering::Relaxed) {
            return;
        }
        for (pair_id, &(buy_idx, sell_idx)) in self.pair_edges.iter().enumerate() {
            let ticker = store.get_by_id(pair_id);
            if let Some(t) = ticker {
                self.update_pair_weights(pair_id, t.bid, t.ask);
            } else {
                self.invalidate_pair(pair_id);
                let _ = (buy_idx, sell_idx);
            }
        }
    }

    pub fn update_pair_weights(&self, pair_id: usize, bid: f64, ask: f64) {
        if pair_id >= self.pair_edges.len() {
            return;
        }

        if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask >= bid) {
            self.invalidate_pair(pair_id);
            return;
        }

        let fee_multiplier = f64::from_bits(self.fee_multiplier_bits.load(Ordering::Relaxed));
        let (buy_idx, sell_idx) = self.pair_edges[pair_id];

        let buy_w = compute_buy_weight(ask, fee_multiplier);
        self.edges[buy_idx]
            .weight_bits
            .store(buy_w.to_bits(), Ordering::Relaxed);

        let sell_w = compute_sell_weight(bid, fee_multiplier);
        self.edges[sell_idx]
            .weight_bits
            .store(sell_w.to_bits(), Ordering::Relaxed);
    }

    pub fn edge_weight(&self, edge_idx: usize) -> f64 {
        f64::from_bits(self.edges[edge_idx].weight_bits.load(Ordering::Relaxed))
    }

    fn invalidate_pair(&self, pair_id: usize) {
        if pair_id >= self.pair_edges.len() {
            return;
        }
        let (buy_idx, sell_idx) = self.pair_edges[pair_id];
        self.edges[buy_idx]
            .weight_bits
            .store(f64::NAN.to_bits(), Ordering::Relaxed);
        self.edges[sell_idx]
            .weight_bits
            .store(f64::NAN.to_bits(), Ordering::Relaxed);
    }
}

fn fee_multiplier_from_fee(taker_fee_rate: Decimal) -> anyhow::Result<f64> {
    let fee = rust_decimal::prelude::ToPrimitive::to_f64(&taker_fee_rate)
        .ok_or_else(|| anyhow::anyhow!("taker_fee_rate 无法转换为 f64"))?;
    Ok(1.0 - fee)
}

fn compute_buy_weight(ask: f64, fee_multiplier: f64) -> f64 {
    const LOG_EPS: f64 = 1e-15;
    if !(ask.is_finite() && ask > 0.0 && fee_multiplier.is_finite()) {
        return f64::NAN;
    }
    let net_rate = (1.0 / ask) * fee_multiplier;
    if net_rate <= LOG_EPS {
        return f64::NAN;
    }
    let w = -net_rate.ln();
    if w.is_finite() { w } else { f64::NAN }
}

fn compute_sell_weight(bid: f64, fee_multiplier: f64) -> f64 {
    const LOG_EPS: f64 = 1e-15;
    if !(bid.is_finite() && bid > 0.0 && fee_multiplier.is_finite()) {
        return f64::NAN;
    }
    let net_rate = bid * fee_multiplier;
    if net_rate <= LOG_EPS {
        return f64::NAN;
    }
    let w = -net_rate.ln();
    if w.is_finite() { w } else { f64::NAN }
}
