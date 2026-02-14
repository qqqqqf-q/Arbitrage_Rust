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

    pub base_assets: Vec<String>,
    pub maker_only: bool,

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

    pub ticker_warmup_ratio: f64,
    pub ticker_warmup_timeout_seconds: u64,
    pub ticker_warmup_min_valid: usize,

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

            base_assets: vec!["USDT".to_string()],
            maker_only: false,

            simulation_start_amount: dec!(100.0),
            taker_fee_rate: dec!(0.00075),
            min_trade_amount_usd_equivalent: dec!(6.0),
            use_quote_order_qty_for_buy: true,
            max_trade_retries: 2,
            trade_retry_delay_sec: 1.5,

            min_profit_full_sim_percent: dec!(0.05),
            max_arbitrage_depth: 6,
            min_24h_quote_volume: dec!(100000),

            risk_assessment_enabled: true,
            min_profit_after_slippage_percent: dec!(0.05),
            max_allowed_slippage_percent_total: dec!(0.15),
            max_bid_ask_spread_percent_per_step: dec!(0.50),
            min_depth_required_usd: dec!(100.0),
            order_book_depth: 10,

            ticker_warmup_ratio: 0.8,
            ticker_warmup_timeout_seconds: 20,
            ticker_warmup_min_valid: 0,

            websocket_chunk_size: 180,
            balance_update_interval_seconds: 60,
            ticker_batch_size: 200,
            orderbook_fetch_max_workers: 4,
            use_threaded_orderbook_fetch: true,
        }
    }
}

impl Config {
    pub fn from_env_or_default() -> Self {
        let mut cfg = Self::default();

        if let Ok(v) = env::var("BASE_ASSETS") {
            let mut list: Vec<String> = v
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_uppercase())
                .collect();
            list.sort();
            list.dedup();
            if !list.is_empty() {
                cfg.base_assets = list;
            }
        }

        if let Ok(v) = env::var("MAKER_ONLY") {
            let v = v.trim().to_lowercase();
            cfg.maker_only = matches!(v.as_str(), "1" | "true" | "yes" | "on");
        }

        if let Ok(v) = env::var("MAX_ARBITRAGE_DEPTH") {
            if let Ok(d) = v.trim().parse::<usize>() {
                if d > 0 {
                    cfg.max_arbitrage_depth = d;
                }
            }
        }

        if let Ok(v) = env::var("WEBSOCKET_CHUNK_SIZE") {
            if let Ok(n) = v.trim().parse::<usize>() {
                if n > 0 {
                    cfg.websocket_chunk_size = n;
                }
            }
        }

        if let Ok(v) = env::var("TICKER_WARMUP_RATIO") {
            if let Ok(r) = v.trim().parse::<f64>() {
                if r.is_finite() && r > 0.0 && r <= 1.0 {
                    cfg.ticker_warmup_ratio = r;
                }
            }
        }

        if let Ok(v) = env::var("TICKER_WARMUP_TIMEOUT_SEC") {
            if let Ok(n) = v.trim().parse::<u64>() {
                cfg.ticker_warmup_timeout_seconds = n;
            }
        }

        if let Ok(v) = env::var("TICKER_WARMUP_MIN_VALID") {
            if let Ok(n) = v.trim().parse::<usize>() {
                cfg.ticker_warmup_min_valid = n;
            }
        }

        cfg
    }

    pub fn set_fee_rate(&mut self, fee: Decimal) {
        self.taker_fee_rate = fee;
    }

    pub fn set_min_profit(&mut self, min_profit_percent: Decimal) {
        self.min_profit_full_sim_percent = min_profit_percent;
    }

    pub fn set_max_depth(&mut self, depth: usize) {
        self.max_arbitrage_depth = depth;
    }

    pub fn primary_base_asset(&self) -> &str {
        self.base_assets
            .first()
            .map(|s| s.as_str())
            .unwrap_or("USDT")
    }
}
