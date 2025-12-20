use std::env;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use zeroize::Zeroizing;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub api_key: Zeroizing<String>,
    pub api_secret: Zeroizing<String>,
    pub telegram_bot_token: Zeroizing<String>,
    pub authorized_user_id: i64,
}

impl Credentials {
    pub fn from_env() -> anyhow::Result<Self> {
        let api_key = env::var("API_KEY").unwrap_or_default();
        let api_secret = env::var("API_SECRET").unwrap_or_default();
        let telegram_bot_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let authorized_user_id = env::var("AUTHORIZED_USER_ID")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);

        if api_key.is_empty() || api_secret.is_empty() || telegram_bot_token.is_empty() {
            anyhow::bail!("错误：必须通过环境变量设置 API_KEY / API_SECRET / TELEGRAM_BOT_TOKEN");
        }

        Ok(Self {
            api_key: Zeroizing::new(api_key),
            api_secret: Zeroizing::new(api_secret),
            telegram_bot_token: Zeroizing::new(telegram_bot_token),
            authorized_user_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub running: bool,
    pub auto_trade_enabled: bool,

    pub simulation_start_amount: Decimal,
    pub taker_fee_rate: Decimal,
    pub min_trade_amount_usd_equivalent: Decimal,
    pub use_quote_order_qty_for_buy: bool,
    pub max_trade_retries: u32,
    pub trade_retry_delay_sec: f64,

    pub min_profit_full_sim_percent: Decimal,
    pub max_arbitrage_depth: usize,
    pub min_24h_quote_volume: Decimal,

    pub risk_assessment_enabled: bool,
    pub min_profit_after_slippage_percent: Decimal,
    pub max_allowed_slippage_percent_total: Decimal,
    pub max_bid_ask_spread_percent_per_step: Decimal,
    pub min_depth_required_usd: Decimal,
    pub order_book_depth: usize,

    pub websocket_chunk_size: usize,
    pub balance_update_interval_seconds: u64,
    pub ticker_batch_size: usize,
    pub orderbook_fetch_max_workers: usize,
    pub use_threaded_orderbook_fetch: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            running: true,
            auto_trade_enabled: false,

            simulation_start_amount: dec!(100.0),
            taker_fee_rate: dec!(0.00075),
            min_trade_amount_usd_equivalent: dec!(6.0),
            use_quote_order_qty_for_buy: true,
            max_trade_retries: 2,
            trade_retry_delay_sec: 1.5,

            min_profit_full_sim_percent: dec!(0.05),
            max_arbitrage_depth: 5,
            min_24h_quote_volume: dec!(100000),

            risk_assessment_enabled: true,
            min_profit_after_slippage_percent: dec!(0.05),
            max_allowed_slippage_percent_total: dec!(0.15),
            max_bid_ask_spread_percent_per_step: dec!(0.50),
            min_depth_required_usd: dec!(100.0),
            order_book_depth: 10,

            websocket_chunk_size: 180,
            balance_update_interval_seconds: 60,
            ticker_batch_size: 200,
            orderbook_fetch_max_workers: 4,
            use_threaded_orderbook_fetch: true,
        }
    }
}

impl Config {
    pub fn set_fee_rate(&mut self, fee: Decimal) {
        self.taker_fee_rate = fee;
    }

    pub fn set_min_profit(&mut self, min_profit_percent: Decimal) {
        self.min_profit_full_sim_percent = min_profit_percent;
    }

    pub fn set_max_depth(&mut self, depth: usize) {
        self.max_arbitrage_depth = depth;
    }
}

