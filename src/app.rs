use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use rust_decimal::Decimal;
use teloxide::payloads::SendMessageSetters;
use teloxide::prelude::{Bot, ChatId, Requester};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

use crate::arbitrage::graph::{Graph, build_static_graph};
use crate::arbitrage::{
    risk::assess_risk,
    simulate::simulate_full,
    spfa::{SpfaWorkspace, find_negative_cycles_spfa_with_workspace},
};
use crate::binance::rest::BinanceRestClient;
use crate::binance::trade_ws::BinanceTradeWsClient;
use crate::binance::ws::{spawn_book_ticker_streams, spawn_partial_depth_streams};
use crate::config::{Config, Credentials};
use crate::domain::{CycleInfo, Market};
use crate::perf::PerfCounters;
use crate::store::{OrderBookStore, TickerStore};
use crate::telegram::bot::TelegramController;

#[derive(Debug, Default, Clone)]
pub struct PerfStatsSnapshot {
    pub cycle_count_total: u64,
    pub last_cycle_duration_sec: f64,
    pub snap_copy_duration_sec: f64,
    pub graph_build_duration_sec: f64,
    pub bf_call_duration_sec: f64,
    pub verification_duration_sec: f64,
    pub sim_total_duration_sec: f64,
    pub risk_total_duration_sec: f64,
    pub sim_calls: u64,
    pub risk_calls: u64,
}

#[derive(Debug, Default)]
pub struct PerfStats {
    pub start_epoch_ms: u64,
    pub cycle_count_total: u64,
    pub last_cycle_duration_sec: f64,
    pub snap_copy_duration_sec: f64,
    pub graph_build_duration_sec: f64,
    pub bf_call_duration_sec: f64,
    pub verification_duration_sec: f64,
    pub sim_total_duration_sec: f64,
    pub risk_total_duration_sec: f64,
    pub sim_calls: u64,
    pub risk_calls: u64,
}

impl PerfStats {
    pub fn snapshot(&self) -> PerfStatsSnapshot {
        PerfStatsSnapshot {
            cycle_count_total: self.cycle_count_total,
            last_cycle_duration_sec: self.last_cycle_duration_sec,
            snap_copy_duration_sec: self.snap_copy_duration_sec,
            graph_build_duration_sec: self.graph_build_duration_sec,
            bf_call_duration_sec: self.bf_call_duration_sec,
            verification_duration_sec: self.verification_duration_sec,
            sim_total_duration_sec: self.sim_total_duration_sec,
            risk_total_duration_sec: self.risk_total_duration_sec,
            sim_calls: self.sim_calls,
            risk_calls: self.risk_calls,
        }
    }
}

pub struct BalanceStore {
    map: DashMap<String, Decimal>,
}

impl BalanceStore {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    pub fn set_all(&self, balances: HashMap<String, Decimal>) {
        self.map.clear();
        for (k, v) in balances {
            self.map.insert(k, v);
        }
    }

    pub fn snapshot_sorted(&self) -> BTreeMap<String, Decimal> {
        let mut out = BTreeMap::new();
        for entry in self.map.iter() {
            out.insert(entry.key().clone(), *entry.value());
        }
        out
    }

    pub fn get(&self, currency: &str) -> Decimal {
        self.map
            .get(currency)
            .map(|v| *v.value())
            .unwrap_or(Decimal::ZERO)
    }
}

pub struct AppContext {
    pub cfg: Arc<RwLock<Config>>,
    pub tickers: Arc<TickerStore>,
    pub graph: Arc<Graph>,
    pub order_books: Arc<OrderBookStore>,
    pub balances: Arc<BalanceStore>,
    pub perf: Arc<Mutex<PerfStats>>,
    pub perf_counters: Arc<PerfCounters>,
    pub markets: Arc<HashMap<String, Market>>,
    pub websocket_symbols: Arc<Vec<String>>, // 形如 "BTC/USDT"
    pub ws_conn_ok: Arc<Vec<std::sync::atomic::AtomicBool>>,
    pub trade_ws: BinanceTradeWsClient,
    pub bot: Bot,
    pub user_chat_id: Arc<Mutex<Option<ChatId>>>,
    pub trade_semaphore: Arc<tokio::sync::Semaphore>,
    pub ticker_notify: Arc<tokio::sync::Notify>,
    pub ticker_notify_armed: Arc<AtomicBool>,
}

pub async fn run() -> anyhow::Result<()> {
    let creds = Credentials::from_env()?;
    let cfg = Arc::new(RwLock::new(Config::from_env_or_default()));

    let rest = BinanceRestClient::new(&creds.api_key, &creds.api_secret)?;
    let trade_ws = BinanceTradeWsClient::connect(&creds.api_key, &creds.api_secret).await?;

    info!("加载币安现货市场中...");
    let markets = crate::binance::ws::load_spot_markets(&rest).await?;
    info!("已加载 {} 个现货市场。", markets.len());

    info!("分批获取 24h Ticker，进行流动性过滤...");
    let websocket_symbols =
        crate::binance::ws::filter_symbols_by_quote_volume(&rest, &markets, cfg.clone()).await?;
    if websocket_symbols.is_empty() {
        anyhow::bail!("流动性过滤后无可用交易对，程序退出。");
    }
    info!(
        "流动性过滤完成，将监听 {} 个交易对。",
        websocket_symbols.len()
    );

    let tickers = Arc::new(TickerStore::new(websocket_symbols.clone()));
    let order_books = Arc::new(OrderBookStore::new(websocket_symbols.clone()));
    let graph = Arc::new(build_static_graph(
        &markets,
        &websocket_symbols,
        cfg.read().await.taker_fee_rate,
    )?);
    let balances = Arc::new(BalanceStore::new());
    let perf_counters = Arc::new(PerfCounters::default());
    let perf = Arc::new(Mutex::new(PerfStats {
        start_epoch_ms: now_ms(),
        ..PerfStats::default()
    }));

    let ws_chunk_size = cfg.read().await.websocket_chunk_size;
    let ticker_notify = Arc::new(tokio::sync::Notify::new());
    let ticker_notify_armed = Arc::new(AtomicBool::new(false));
    let (ws_tasks, ws_conn_ok) = spawn_book_ticker_streams(
        websocket_symbols.clone(),
        markets.clone(),
        tickers.clone(),
        graph.clone(),
        ticker_notify.clone(),
        ticker_notify_armed.clone(),
        perf_counters.clone(),
        ws_chunk_size,
    );

    let (depth_ws_tasks, _depth_ws_conn_ok) = spawn_partial_depth_streams(
        websocket_symbols.clone(),
        markets.clone(),
        order_books.clone(),
        ws_chunk_size,
        cfg.read().await.order_book_depth,
    );

    let bot = Bot::new(creds.telegram_bot_token.as_str());
    let user_chat_id = Arc::new(Mutex::new(None));

    let ctx = Arc::new(AppContext {
        cfg: cfg.clone(),
        tickers: tickers.clone(),
        graph: graph.clone(),
        order_books: order_books.clone(),
        balances: balances.clone(),
        perf: perf.clone(),
        perf_counters: perf_counters.clone(),
        markets: Arc::new(markets),
        websocket_symbols: Arc::new(websocket_symbols),
        ws_conn_ok,
        trade_ws: trade_ws.clone(),
        bot: bot.clone(),
        user_chat_id: user_chat_id.clone(),
        trade_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
        ticker_notify,
        ticker_notify_armed,
    });

    let tg = TelegramController::new(bot, creds.authorized_user_id, ctx.clone())?;
    let tg_task = tokio::spawn(async move { tg.run().await });

    let balance_task = spawn_balance_task(rest.clone(), ctx.clone());
    let arbitrage_loop_task = spawn_arbitrage_loop(rest.clone(), ctx.clone());

    tokio::select! {
        res = tg_task => {
            if let Err(e) = res {
                warn!("Telegram 任务异常退出: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("收到 Ctrl+C，开始关闭...");
        }
    }

    for t in ws_tasks {
        t.abort();
    }
    for t in depth_ws_tasks {
        t.abort();
    }
    balance_task.abort();
    arbitrage_loop_task.abort();

    Ok(())
}

fn spawn_balance_task(
    rest: BinanceRestClient,
    ctx: Arc<AppContext>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let interval = {
                let c = ctx.cfg.read().await;
                c.balance_update_interval_seconds
            };
            if let Err(e) = update_balance_once(&rest, &ctx).await {
                warn!("获取余额失败: {e}");
            }
            tokio::time::sleep(Duration::from_secs(interval)).await;
        }
    })
}

async fn update_balance_once(rest: &BinanceRestClient, ctx: &AppContext) -> anyhow::Result<()> {
    let account = rest.account().await?;
    let mut out = HashMap::new();
    for b in account.balances {
        let free = b.free.parse::<Decimal>().unwrap_or(Decimal::ZERO);
        if free > Decimal::ZERO {
            out.insert(b.asset, free);
        }
    }
    ctx.balances.set_all(out);
    Ok(())
}

fn spawn_arbitrage_loop(
    rest: BinanceRestClient,
    ctx: Arc<AppContext>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = main_arbitrage_loop(&rest, &ctx).await {
            error!("主套利循环异常退出: {e:?}");
        }
    })
}

async fn main_arbitrage_loop(_rest: &BinanceRestClient, ctx: &AppContext) -> anyhow::Result<()> {
    info!("主循环预热中，等待 WebSocket Ticker 数据稳定...");
    let required = ((ctx.websocket_symbols.len() as f64) * 0.8) as usize;
    while ctx.tickers.valid_count() < required {
        info!(
            "  ...等待 Ticker 数据 ({}/{})",
            ctx.tickers.valid_count(),
            required
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    info!("Ticker 数据已稳定，主套利计算循环正式开始。");

    let mut last_processed_seq = 0u64;
    let mut spfa_ws = SpfaWorkspace::new();

    loop {
        {
            let c = ctx.cfg.read().await;
            if !c.running {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }

        if !ctx.ws_conn_ok.iter().any(|b| b.load(Ordering::Relaxed)) {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        let seq = ctx.tickers.update_seq();
        if seq == 0 || seq == last_processed_seq {
            ctx.ticker_notify_armed.store(false, Ordering::Relaxed);
            let seq2 = ctx.tickers.update_seq();
            if seq2 != 0 && seq2 != last_processed_seq {
                continue;
            }
            let _ = tokio::time::timeout(Duration::from_millis(250), ctx.ticker_notify.notified()).await;
            continue;
        }
        last_processed_seq = seq;

        let cycle_start = std::time::Instant::now();
        let cfg_snapshot = ctx.cfg.read().await.clone();
        ctx.graph.maybe_rebuild_all_weights(&ctx.tickers);

        let bf_start = std::time::Instant::now();
        let cycles = find_negative_cycles_spfa_with_workspace(
            &ctx.graph,
            ctx.websocket_symbols.as_ref(),
            cfg_snapshot.max_arbitrage_depth,
            &mut spfa_ws,
        )?;
        let bf_sec = bf_start.elapsed().as_secs_f64();

        let verify_start = std::time::Instant::now();
        let mut sim_sec_total = 0.0f64;
        let mut risk_sec_total = 0.0f64;
        let mut sim_calls = 0u64;
        let mut risk_calls = 0u64;
        if !cycles.is_empty() {
            for cycle in cycles {
                let path_str = cycle.nodes.join(" -> ");
                for base_asset in &cfg_snapshot.base_assets {
                    let sim_start = std::time::Instant::now();
                    let sim = simulate_full(
                        &cycle,
                        base_asset,
                        cfg_snapshot.simulation_start_amount,
                        Some(base_asset),
                        &ctx.tickers,
                        &ctx.markets,
                        &cfg_snapshot,
                    )?;
                    sim_sec_total += sim_start.elapsed().as_secs_f64();
                    sim_calls += 1;

                    if !sim.verified {
                        continue;
                    }

                    info!(
                        "模拟验证成功: base={} path={} (模拟利润: {:.4}%)",
                        base_asset, path_str, sim.profit_percent
                    );

                    if !cfg_snapshot.auto_trade_enabled {
                        break;
                    }

                    let permit = match ctx.trade_semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            info!("已有交易任务在运行，跳过机会: {}", path_str);
                            break;
                        }
                    };

                    let risk_start = std::time::Instant::now();
                    let risk = assess_risk(
                        &cycle,
                        cfg_snapshot.simulation_start_amount,
                        &ctx.order_books,
                        &ctx.markets,
                        &ctx.tickers,
                        &cfg_snapshot,
                    )
                    .await?;
                    risk_sec_total += risk_start.elapsed().as_secs_f64();
                    risk_calls += 1;
                    if risk.is_viable {
                        info!(
                            "风险评估通过: base={} path={}，准备执行...",
                            base_asset, path_str
                        );
                        notify_text(
                            ctx,
                            format!(
                                "检测到机会 (base={} 模拟利润 {:.4}%)，风险评估通过，开始执行...\n路径: <code>{}</code>",
                                base_asset, sim.profit_percent, path_str
                            ),
                        )
                        .await;

                        let ctx_clone = Arc::new(ctx_clone_shallow(ctx));
                            let cycle_clone = cycle.clone();
                            let base_asset = base_asset.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) =
                                    execute_arbitrage_path(&ctx_clone, &cycle_clone, &base_asset)
                                        .await
                                {
                                    notify_text(&*ctx_clone, format!("套利执行失败: {}", e)).await;
                                }
                        });
                        break;
                    } else {
                        warn!(
                            "风险评估未通过: {}。原因: {}",
                            path_str,
                            risk.reasons.join("; ")
                        );
                    }

                    break;
                }
            }
        }
        let verify_sec = verify_start.elapsed().as_secs_f64();

        let total_sec = cycle_start.elapsed().as_secs_f64();
        {
            let mut p = ctx.perf.lock().await;
            p.cycle_count_total += 1;
            p.last_cycle_duration_sec = total_sec;
            p.snap_copy_duration_sec = 0.0;
            p.graph_build_duration_sec = 0.0;
            p.bf_call_duration_sec = bf_sec;
            p.verification_duration_sec = verify_sec;
            p.sim_total_duration_sec = sim_sec_total;
            p.risk_total_duration_sec = risk_sec_total;
            p.sim_calls = sim_calls;
            p.risk_calls = risk_calls;
        }
    }
}

#[derive(Clone)]
struct ShallowCtx {
    cfg: Arc<RwLock<Config>>,
    tickers: Arc<TickerStore>,
    balances: Arc<BalanceStore>,
    markets: Arc<HashMap<String, Market>>,
    trade_ws: BinanceTradeWsClient,
    bot: Bot,
    user_chat_id: Arc<Mutex<Option<ChatId>>>,
}

fn ctx_clone_shallow(ctx: &AppContext) -> ShallowCtx {
    ShallowCtx {
        cfg: ctx.cfg.clone(),
        tickers: ctx.tickers.clone(),
        balances: ctx.balances.clone(),
        markets: ctx.markets.clone(),
        trade_ws: ctx.trade_ws.clone(),
        bot: ctx.bot.clone(),
        user_chat_id: ctx.user_chat_id.clone(),
    }
}

async fn notify_text(ctx: &impl Notifier, text: String) {
    let _ = ctx.notify(text).await;
}

#[async_trait::async_trait]
trait Notifier {
    async fn notify(&self, text: String) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl Notifier for AppContext {
    async fn notify(&self, text: String) -> anyhow::Result<()> {
        if let Some(chat_id) = *self.user_chat_id.lock().await {
            self.bot
                .send_message(chat_id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Notifier for ShallowCtx {
    async fn notify(&self, text: String) -> anyhow::Result<()> {
        if let Some(chat_id) = *self.user_chat_id.lock().await {
            self.bot
                .send_message(chat_id, text)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;
        }
        Ok(())
    }
}

async fn execute_arbitrage_path(
    ctx: &ShallowCtx,
    cycle: &CycleInfo,
    base_asset: &str,
) -> anyhow::Result<()> {
    let cfg = ctx.cfg.read().await.clone();

    let path_str = cycle.nodes.join(" -> ");
    info!("--- [执行开始] 开始执行路径: {} ---", path_str);

    let min_start_usd = cfg.min_trade_amount_usd_equivalent;
    let start_fund_currency = base_asset;
    let start_balance = ctx.balances.get(start_fund_currency);
    if start_balance < min_start_usd {
        let msg = format!(
            "起始资金 {} 余额 ({}) 不足最低要求 (${})。",
            start_fund_currency, start_balance, min_start_usd
        );
        notify_text(ctx, format!("套利中止: {}", msg)).await;
        anyhow::bail!(msg);
    }

    let mut current_amount = std::cmp::min(start_balance, cfg.simulation_start_amount);
    let mut current_currency = start_fund_currency.to_string();

    let cycle_start = cycle
        .nodes
        .first()
        .cloned()
        .unwrap_or_else(|| "USDT".to_string());

    if current_currency != cycle_start {
        let res =
            execute_real_swap(&ctx.trade_ws, ctx, &current_currency, &cycle_start, current_amount)
                .await?;
        current_amount = res.received_amount;
        current_currency = res.received_currency;
    }

    for trade in &cycle.trades {
        if current_currency != trade.from {
            anyhow::bail!("逻辑错误: 需要 {}, 但持有 {}", trade.from, current_currency);
        }

        let market = ctx
            .markets
            .get(&trade.pair)
            .ok_or_else(|| anyhow::anyhow!("市场 {} 未找到", trade.pair))?;

        let expected_price = ctx
            .tickers
            .get_by_pair(&trade.pair)
            .map(|t| if trade.kind == "BUY" { t.ask } else { t.bid });

        let order = if trade.kind == "BUY" && cfg.use_quote_order_qty_for_buy {
            place_market_order_with_retry(
                &ctx.trade_ws,
                market,
                "BUY",
                None,
                Some(current_amount),
                &cfg,
            )
                .await?
        } else if trade.kind == "BUY" {
            let qty = match expected_price {
                Some(p) if p > 0.0 => {
                    current_amount / Decimal::from_f64_retain(p).unwrap_or(Decimal::ZERO)
                }
                _ => anyhow::bail!("缺少预期价格，无法计算 BUY 数量"),
            };
            place_market_order_with_retry(&ctx.trade_ws, market, "BUY", Some(qty), None, &cfg)
                .await?
        } else {
            place_market_order_with_retry(
                &ctx.trade_ws,
                market,
                "SELL",
                Some(current_amount),
                None,
                &cfg,
            )
                .await?
        };

        current_amount = order.received_amount;
        current_currency = order.received_currency.clone();

        notify_text(
            ctx,
            format!(
                "成交: {} 花费 {} {} on <code>{}</code> -> 收到 {} {}",
                order.side,
                order.spent_amount,
                order.spent_currency,
                market.symbol,
                order.received_amount,
                order.received_currency
            ),
        )
        .await;
    }

    if current_currency != start_fund_currency {
        let res = execute_real_swap(
            &ctx.trade_ws,
            ctx,
            &current_currency,
            start_fund_currency,
            current_amount,
        )
        .await?;
        current_amount = res.received_amount;
        current_currency = res.received_currency;
    }

    notify_text(
        ctx,
        format!(
            "套利执行完成\n路径: <code>{}</code>\n最终持有: {} {}",
            path_str, current_amount, current_currency
        ),
    )
    .await;

    info!("--- [执行结束] 路径 {} 执行完成 ---", path_str);
    Ok(())
}

#[derive(Debug, Clone)]
struct OrderExecResult {
    side: String,
    spent_amount: Decimal,
    spent_currency: String,
    received_amount: Decimal,
    received_currency: String,
}

async fn execute_real_swap(
    trade: &BinanceTradeWsClient,
    ctx: &ShallowCtx,
    from_currency: &str,
    to_currency: &str,
    from_amount: Decimal,
) -> anyhow::Result<OrderExecResult> {
    if from_currency == to_currency {
        return Ok(OrderExecResult {
            side: "NOP".to_string(),
            spent_amount: Decimal::ZERO,
            spent_currency: from_currency.to_string(),
            received_amount: from_amount,
            received_currency: to_currency.to_string(),
        });
    }

    let cfg = ctx.cfg.read().await.clone();
    let symbol_buy = format!("{}/{}", to_currency, from_currency);
    if let Some(market) = ctx.markets.get(&symbol_buy) {
        let expected_price = ctx.tickers.get_by_pair(&symbol_buy).map(|t| t.ask);
        if cfg.use_quote_order_qty_for_buy {
            return place_market_order_with_retry(
                trade,
                market,
                "BUY",
                None,
                Some(from_amount),
                &cfg,
            )
            .await;
        }
        let qty = match expected_price {
            Some(p) if p > 0.0 => {
                from_amount / Decimal::from_f64_retain(p).unwrap_or(Decimal::ZERO)
            }
            _ => anyhow::bail!("缺少预期价格，无法计算 BUY 数量"),
        };
        return place_market_order_with_retry(trade, market, "BUY", Some(qty), None, &cfg).await;
    }

    let symbol_sell = format!("{}/{}", from_currency, to_currency);
    if let Some(market) = ctx.markets.get(&symbol_sell) {
        return place_market_order_with_retry(trade, market, "SELL", Some(from_amount), None, &cfg)
            .await;
    }

    anyhow::bail!(
        "无法找到合适的交易对转换 {} -> {}",
        from_currency,
        to_currency
    )
}

async fn place_market_order_with_retry(
    trade: &BinanceTradeWsClient,
    market: &Market,
    side: &str,
    quantity: Option<Decimal>,
    quote_order_qty: Option<Decimal>,
    cfg: &Config,
) -> anyhow::Result<OrderExecResult> {
    let max_retries = cfg.max_trade_retries;
    let retry_delay = cfg.trade_retry_delay_sec;

    for attempt in 0..=max_retries {
        let order_start = Instant::now();
        match trade
            .create_market_order(&market.binance_symbol, side, quantity, quote_order_qty)
            .await
        {
            Ok(order) => {
                let latency_ms = order_start.elapsed().as_secs_f64() * 1000.0;
                let executed_qty = order
                    .executed_qty
                    .parse::<Decimal>()
                    .unwrap_or(Decimal::ZERO);
                let quote_qty = order
                    .cummulative_quote_qty
                    .parse::<Decimal>()
                    .unwrap_or(Decimal::ZERO);
                let received_fee = sum_commission_in_asset(
                    &order,
                    if side == "BUY" {
                        &market.base
                    } else {
                        &market.quote
                    },
                );
                let res = if side == "BUY" {
                    OrderExecResult {
                        side: side.to_string(),
                        spent_amount: quote_qty,
                        spent_currency: market.quote.clone(),
                        received_amount: (executed_qty - received_fee).max(Decimal::ZERO),
                        received_currency: market.base.clone(),
                    }
                } else {
                    OrderExecResult {
                        side: side.to_string(),
                        spent_amount: executed_qty,
                        spent_currency: market.base.clone(),
                        received_amount: (quote_qty - received_fee).max(Decimal::ZERO),
                        received_currency: market.quote.clone(),
                    }
                };
                info!(
                    "下单成功: symbol={} side={} order_id={} status={} latency_ms={:.2}",
                    market.binance_symbol,
                    side,
                    order.order_id,
                    order.status,
                    latency_ms
                );
                return Ok(res);
            }
            Err(e) => {
                let latency_ms = order_start.elapsed().as_secs_f64() * 1000.0;
                warn!(
                    "交易尝试 #{} 失败: symbol={} side={} latency_ms={:.2} err={}",
                    attempt + 1,
                    market.binance_symbol,
                    side,
                    latency_ms,
                    e
                );
                if attempt >= max_retries {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_secs_f64(retry_delay)).await;
            }
        }
    }

    anyhow::bail!("达到最大重试次数")
}

fn sum_commission_in_asset(order: &crate::binance::models::OrderResponse, asset: &str) -> Decimal {
    let Some(fills) = &order.fills else {
        return Decimal::ZERO;
    };

    let mut sum = Decimal::ZERO;
    for f in fills {
        if f.commission_asset == asset {
            if let Ok(v) = f.commission.parse::<Decimal>() {
                sum += v;
            }
        }
    }
    sum
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}
