use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context as _;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use simd_json::prelude::{ValueAsArray, ValueAsObject, ValueAsScalar, ValueObjectAccess};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::arbitrage::graph::Graph;
use crate::binance::models::{ExchangeInfo, SymbolFilter};
use crate::binance::rest::BinanceRestClient;
use crate::config::Config;
use crate::domain::Market;
use crate::perf::PerfCounters;
use crate::store::{OrderBookStore, TickerStore};

pub async fn load_spot_markets(
    rest: &BinanceRestClient,
) -> anyhow::Result<HashMap<String, Market>> {
    let ExchangeInfo { symbols } = rest.exchange_info().await?;
    let mut out = HashMap::with_capacity(symbols.len());

    for s in symbols {
        let active = s.status == "TRADING";
        let spot = s.is_spot_trading_allowed;
        if !active || !spot {
            continue;
        }

        let mut min_qty = Decimal::ZERO;
        let mut min_notional = Decimal::ZERO;
        for f in s.filters {
            match f {
                SymbolFilter::LotSize { min_qty: q, .. } => {
                    min_qty = q.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                }
                SymbolFilter::MinNotional { min_notional: n } => {
                    min_notional = n.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                }
                SymbolFilter::Other => {}
            }
        }

        let pair = format!("{}/{}", s.base_asset, s.quote_asset);
        out.insert(
            pair.clone(),
            Market {
                symbol: pair,
                binance_symbol: s.symbol,
                base: s.base_asset,
                quote: s.quote_asset,
                min_qty,
                min_notional,
                active,
                spot,
            },
        );
    }

    Ok(out)
}

pub async fn filter_symbols_by_quote_volume(
    rest: &BinanceRestClient,
    markets: &HashMap<String, Market>,
    cfg: Arc<RwLock<Config>>,
) -> anyhow::Result<Vec<String>> {
    let all_pairs: Vec<String> = markets.keys().cloned().collect();
    let batch_size = cfg.read().await.ticker_batch_size;

    let mut selected = Vec::new();
    let min_volume = cfg.read().await.min_24h_quote_volume;

    for chunk in all_pairs.chunks(batch_size) {
        let mut binance_symbols = Vec::with_capacity(chunk.len());
        let mut rev = HashMap::with_capacity(chunk.len());
        for pair in chunk {
            if let Some(m) = markets.get(pair) {
                binance_symbols.push(m.binance_symbol.clone());
                rev.insert(m.binance_symbol.clone(), pair.clone());
            }
        }

        let tickers = rest.tickers_24h(&binance_symbols).await?;
        for t in tickers {
            let qv = t.quote_volume.parse::<Decimal>().unwrap_or(Decimal::ZERO);
            if qv >= min_volume {
                if let Some(pair) = rev.get(&t.symbol) {
                    selected.push(pair.clone());
                }
            }
        }
    }

    Ok(selected)
}

pub fn spawn_book_ticker_streams(
    websocket_symbols: Vec<String>,
    markets: HashMap<String, Market>,
    store: Arc<TickerStore>,
    graph: Arc<Graph>,
    ticker_notify: Arc<tokio::sync::Notify>,
    ticker_notify_armed: Arc<AtomicBool>,
    perf: Arc<PerfCounters>,
    chunk_size: usize,
) -> (
    Vec<tokio::task::JoinHandle<()>>,
    Arc<Vec<AtomicBool>>,
    Arc<Vec<usize>>,
) {
    let mut chunks: Vec<Vec<String>> = Vec::new();
    for c in websocket_symbols.chunks(chunk_size) {
        chunks.push(c.to_vec());
    }

    let conn_ok: Arc<Vec<AtomicBool>> =
        Arc::new((0..chunks.len()).map(|_| AtomicBool::new(false)).collect());

    let mut pair_to_stream = HashMap::with_capacity(websocket_symbols.len());
    let mut binance_to_id: HashMap<String, usize> = HashMap::with_capacity(websocket_symbols.len());
    for (pair_id, pair) in websocket_symbols.iter().enumerate() {
        if let Some(m) = markets.get(pair) {
            let stream = format!("{}@bookTicker", m.binance_symbol.to_lowercase());
            pair_to_stream.insert(pair.clone(), stream);
            binance_to_id.insert(m.binance_symbol.clone(), pair_id);
        }
    }
    let binance_to_id = Arc::new(binance_to_id);

    let stream_chunks: Vec<Vec<String>> = chunks
        .into_iter()
        .map(|pairs| {
            pairs
                .into_iter()
                .filter_map(|p| pair_to_stream.get(&p).cloned())
                .collect()
        })
        .collect();

    let chunk_pair_counts: Arc<Vec<usize>> =
        Arc::new(stream_chunks.iter().map(|s| s.len()).collect());

    let tasks = stream_chunks
        .into_iter()
        .enumerate()
        .map(|(idx, streams)| {
            let store = store.clone();
            let graph = graph.clone();
            let ticker_notify = ticker_notify.clone();
            let ticker_notify_armed = ticker_notify_armed.clone();
            let perf = perf.clone();
            let conn_ok = conn_ok.clone();
            let binance_to_id = binance_to_id.clone();

            tokio::spawn(async move {
                run_ws_chunk(
                    idx,
                    streams,
                    store,
                    graph,
                    ticker_notify,
                    ticker_notify_armed,
                    perf,
                    conn_ok,
                    binance_to_id,
                )
                .await;
            })
        })
        .collect();

    (tasks, conn_ok, chunk_pair_counts)
}

async fn run_ws_chunk(
    chunk_index: usize,
    streams: Vec<String>,
    store: Arc<TickerStore>,
    graph: Arc<Graph>,
    ticker_notify: Arc<tokio::sync::Notify>,
    ticker_notify_armed: Arc<AtomicBool>,
    perf: Arc<PerfCounters>,
    conn_ok: Arc<Vec<AtomicBool>>,
    binance_to_id: Arc<HashMap<String, usize>>,
) {
    info!(
        "启动 WebSocket 块 {} (监听 {} 个交易对)...",
        chunk_index + 1,
        streams.len()
    );
    conn_ok[chunk_index].store(false, Ordering::Relaxed);

    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    );

    let mut backoff = std::time::Duration::from_secs(1);
    let backoff_max = std::time::Duration::from_secs(30);

    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                info!("块 {}: WebSocket 连接成功。", chunk_index + 1);
                conn_ok[chunk_index].store(true, Ordering::Relaxed);
                backoff = std::time::Duration::from_secs(1);

                let (_, mut reader) = ws.split();
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = handle_book_ticker_ws_message(
                                text.into_bytes(),
                                &store,
                                &graph,
                                &ticker_notify,
                                &ticker_notify_armed,
                                &perf,
                                &binance_to_id,
                            ) {
                                warn!("块 {}: 解析消息失败: {}", chunk_index + 1, e);
                            }
                        }
                        Ok(Message::Binary(bin)) => {
                            if let Err(e) = handle_book_ticker_ws_message(
                                bin,
                                &store,
                                &graph,
                                &ticker_notify,
                                &ticker_notify_armed,
                                &perf,
                                &binance_to_id,
                            ) {
                                warn!("块 {}: 解析消息失败: {}", chunk_index + 1, e);
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            warn!("块 {}: WebSocket 错误: {}", chunk_index + 1, e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                warn!("块 {}: WebSocket 连接失败: {}", chunk_index + 1, e);
            }
        }

        conn_ok[chunk_index].store(false, Ordering::Relaxed);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}

pub(crate) fn handle_book_ticker_ws_message(
    mut bytes: Vec<u8>,
    store: &TickerStore,
    graph: &Graph,
    ticker_notify: &tokio::sync::Notify,
    ticker_notify_armed: &AtomicBool,
    perf: &PerfCounters,
    binance_to_id: &HashMap<String, usize>,
) -> anyhow::Result<()> {
    let bytes_len = bytes.len();
    let parse_start = std::time::Instant::now();
    let v = simd_json::to_borrowed_value(&mut bytes).context("JSON 解析失败")?;

    let data = v
        .get("data")
        .and_then(|d| d.as_object())
        .context("缺少 data")?;

    let sym = data.get("s").and_then(|s| s.as_str()).context("缺少 s")?;
    let bid_s = data.get("b").and_then(|s| s.as_str()).unwrap_or("0");
    let ask_s = data.get("a").and_then(|s| s.as_str()).unwrap_or("0");
    let bid: f64 = fast_float::parse(bid_s).unwrap_or(0.0);
    let ask: f64 = fast_float::parse(ask_s).unwrap_or(0.0);
    let parse_ns = parse_start.elapsed().as_nanos() as u64;

    if let Some(&pair_id) = binance_to_id.get(sym) {
        let apply_start = std::time::Instant::now();
        let now_ms = crate::app::now_ms();
        store.update_by_id(pair_id, bid, ask, now_ms);
        graph.update_pair_weights(pair_id, bid, ask);
        if !ticker_notify_armed.swap(true, Ordering::Relaxed) {
            ticker_notify.notify_one();
        }
        let apply_ns = apply_start.elapsed().as_nanos() as u64;
        perf.record_ws_message(bytes_len, parse_ns, apply_ns);
    }
    Ok(())
}

pub fn spawn_partial_depth_streams(
    websocket_symbols: Vec<String>,
    markets: HashMap<String, Market>,
    books: Arc<OrderBookStore>,
    chunk_size: usize,
    levels: usize,
) -> (Vec<tokio::task::JoinHandle<()>>, Arc<Vec<AtomicBool>>) {
    let mut chunks: Vec<Vec<String>> = Vec::new();
    for c in websocket_symbols.chunks(chunk_size) {
        chunks.push(c.to_vec());
    }

    let conn_ok: Arc<Vec<AtomicBool>> =
        Arc::new((0..chunks.len()).map(|_| AtomicBool::new(false)).collect());

    let depth_levels = match levels {
        0..=5 => 5,
        6..=10 => 10,
        _ => 20,
    };
    let mut stream_to_id: HashMap<String, usize> = HashMap::with_capacity(websocket_symbols.len());
    let mut stream_chunks: Vec<Vec<String>> = Vec::with_capacity(chunks.len());

    for pairs in chunks {
        let mut streams = Vec::with_capacity(pairs.len());
        for pair in &pairs {
            if let Some(m) = markets.get(pair) {
                let pair_id = books.pair_id(pair).unwrap_or(usize::MAX);
                if pair_id == usize::MAX {
                    continue;
                }
                let stream = format!(
                    "{}@depth{}@100ms",
                    m.binance_symbol.to_lowercase(),
                    depth_levels
                );
                stream_to_id.insert(stream.clone(), pair_id);
                streams.push(stream);
            }
        }
        stream_chunks.push(streams);
    }

    let stream_to_id = Arc::new(stream_to_id);

    let tasks = stream_chunks
        .into_iter()
        .enumerate()
        .map(|(idx, streams)| {
            let books = books.clone();
            let conn_ok = conn_ok.clone();
            let stream_to_id = stream_to_id.clone();
            tokio::spawn(async move {
                run_depth_ws_chunk(idx, streams, books, conn_ok, stream_to_id).await;
            })
        })
        .collect();

    (tasks, conn_ok)
}

async fn run_depth_ws_chunk(
    chunk_index: usize,
    streams: Vec<String>,
    books: Arc<OrderBookStore>,
    conn_ok: Arc<Vec<AtomicBool>>,
    stream_to_id: Arc<HashMap<String, usize>>,
) {
    info!(
        "启动 Depth WebSocket 块 {} (监听 {} 个交易对)...",
        chunk_index + 1,
        streams.len()
    );
    conn_ok[chunk_index].store(false, Ordering::Relaxed);

    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    );

    let mut backoff = std::time::Duration::from_secs(1);
    let backoff_max = std::time::Duration::from_secs(30);

    loop {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                info!("Depth 块 {}: WebSocket 连接成功。", chunk_index + 1);
                conn_ok[chunk_index].store(true, Ordering::Relaxed);
                backoff = std::time::Duration::from_secs(1);

                let (_, mut reader) = ws.split();
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Err(e) =
                                handle_depth_ws_message(text.into_bytes(), &books, &stream_to_id)
                            {
                                warn!("Depth 块 {}: 解析消息失败: {}", chunk_index + 1, e);
                            }
                        }
                        Ok(Message::Binary(bin)) => {
                            if let Err(e) = handle_depth_ws_message(bin, &books, &stream_to_id) {
                                warn!("Depth 块 {}: 解析消息失败: {}", chunk_index + 1, e);
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            warn!("Depth 块 {}: WebSocket 错误: {}", chunk_index + 1, e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                warn!("Depth 块 {}: WebSocket 连接失败: {}", chunk_index + 1, e);
            }
        }

        conn_ok[chunk_index].store(false, Ordering::Relaxed);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}

fn handle_depth_ws_message(
    mut bytes: Vec<u8>,
    books: &OrderBookStore,
    stream_to_id: &HashMap<String, usize>,
) -> anyhow::Result<()> {
    let v = simd_json::to_borrowed_value(&mut bytes).context("JSON 解析失败")?;

    let stream = v
        .get("stream")
        .and_then(|s| s.as_str())
        .context("缺少 stream")?;
    let Some(&pair_id) = stream_to_id.get(stream) else {
        return Ok(());
    };

    let data = v
        .get("data")
        .and_then(|d| d.as_object())
        .context("缺少 data")?;

    let bids_v = data.get("bids").context("缺少 bids")?;
    let asks_v = data.get("asks").context("缺少 asks")?;

    let bids = parse_depth_side(bids_v).unwrap_or_default();
    let asks = parse_depth_side(asks_v).unwrap_or_default();

    if bids.is_empty() || asks.is_empty() {
        return Ok(());
    }

    let now_ms = crate::app::now_ms();
    books.update_by_id(pair_id, crate::domain::OrderBook { bids, asks }, now_ms);
    Ok(())
}

fn parse_depth_side(v: &simd_json::BorrowedValue) -> Option<Vec<(f64, f64)>> {
    let arr = v.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for lv in arr {
        let lv = lv.as_array()?;
        if lv.len() < 2 {
            continue;
        }
        let p_s = lv[0].as_str().unwrap_or("0");
        let q_s = lv[1].as_str().unwrap_or("0");
        let p: f64 = fast_float::parse(p_s).unwrap_or(0.0);
        let q: f64 = fast_float::parse(q_s).unwrap_or(0.0);
        if p.is_finite() && q.is_finite() && p > 0.0 && q > 0.0 {
            out.push((p, q));
        }
    }
    Some(out)
}
