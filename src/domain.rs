use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Market {
    pub symbol: String,         // 形如 "BTC/USDT"
    pub binance_symbol: String, // 形如 "BTCUSDT"
    pub base: String,
    pub quote: String,
    pub min_qty: Decimal,
    pub min_notional: Decimal,
    pub active: bool,
    pub spot: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Ticker {
    pub bid: f64,
    pub ask: f64,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: Vec<(f64, f64)>, // (price, qty)
    pub asks: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub from: String,
    pub to: String,
    pub pair: String,
    #[serde(rename = "type")]
    pub kind: String, // "BUY" / "SELL"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CycleInfo {
    pub nodes: Vec<String>, // 形如 ["USDT","BTC","ETH","USDT"]
    pub trades: Vec<Trade>,
    pub depth: usize,
}
