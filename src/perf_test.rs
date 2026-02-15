use std::collections::HashMap;
use std::env;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::arbitrage::graph::build_static_graph;
use crate::arbitrage::simulate::simulate_full;
use crate::arbitrage::spfa::{SpfaWorkspace, find_negative_cycles_spfa_with_workspace};
use crate::binance::rest::BinanceRestClient;
use crate::binance::ws::{
    filter_symbols_by_quote_volume, handle_book_ticker_ws_message, load_spot_markets,
    spawn_book_ticker_streams,
};
use crate::config::Config;
use crate::domain::Market;
use crate::perf::PerfCounters;
use crate::store::TickerStore;

const TRACE_VERSION: u32 = 1;
const TRACE_KIND_BOOK_TICKER: &str = "binance_book_ticker_v1";

#[derive(Debug, Serialize)]
struct PerfTestLiveReport {
    duration_sec: f64,

    pair_count: usize,
    ws_chunk_size: usize,
    ws_chunks_total: usize,
    ws_chunks_connected: usize,
    pairs_connected_estimate: usize,

    ws_msg_count: u64,
    ws_msg_bytes: u64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    ws_avg_parse_ns: u64,
    ws_max_parse_ns: u64,
    ws_avg_apply_ns: u64,
    ws_max_apply_ns: u64,
}

#[derive(Debug, Serialize)]
struct PerfTestRecordReport {
    trace_path: String,
    duration_sec: u64,
    trace_duration_sec: f64,

    pair_count: usize,
    ws_chunk_size: usize,
    ws_chunks_total: usize,

    frames: u64,
    bytes: u64,
    mb: f64,
    frames_per_sec: f64,
    mb_per_sec: f64,
    trace_last_t_us: u64,
}

#[derive(Debug, Serialize)]
struct PerfTestReplayReport {
    trace_path: String,
    pace_enabled: bool,

    frames: u64,
    trace_last_t_us: u64,
    trace_duration_sec: f64,
    duration_sec: f64,
    replay_speedup_x: f64,

    recorded_at_epoch_ms: u64,
    ws_chunk_size: usize,
    pair_count: usize,

    ws_msg_count: u64,
    ws_msg_bytes: u64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    ws_avg_parse_ns: u64,
    ws_max_parse_ns: u64,
    ws_avg_apply_ns: u64,
    ws_max_apply_ns: u64,
}

#[derive(Debug, Serialize)]
struct PerfTestReplayMultiReport {
    trace_path: String,
    pace_enabled: bool,
    replay_limit: Option<u64>,
    repeat: usize,
    runs: Vec<PerfTestReplayReport>,
    avg: PerfTestReplayAgg,
    min: PerfTestReplayAgg,
    max: PerfTestReplayAgg,
}

#[derive(Debug, Serialize, Clone)]
struct PerfTestReplayAgg {
    frames: u64,
    trace_duration_sec: f64,

    duration_sec: f64,
    replay_speedup_x: f64,

    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    ws_avg_parse_ns: f64,
    ws_max_parse_ns: f64,
    ws_avg_apply_ns: f64,
    ws_max_apply_ns: f64,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchComputeStats {
    bench_cycles: u64,
    max_depth: usize,
    base_assets: Vec<String>,
    sim_cycle_limit: usize,

    compute_duration_sec: f64,

    spfa_calls: u64,
    spfa_ns_sum: u64,
    spfa_avg_ns: u64,
    spfa_max_ns: u64,

    cycles_found_total: u64,
    cycles_found_avg: f64,
    cycles_found_max: u64,

    sim_calls: u64,
    sim_verified: u64,
    sim_errors: u64,
    sim_ns_sum: u64,
    sim_avg_ns: u64,
    sim_max_ns: u64,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchReplayReport {
    trace_path: String,
    pace_enabled: bool,
    replay_limit: Option<u64>,

    frames: u64,
    bytes: u64,
    trace_last_t_us: u64,
    trace_duration_sec: f64,

    ingest_duration_sec: f64,
    ingest_wall_speedup_x: f64,

    ws_msg_count: u64,
    ws_msg_bytes: u64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    ws_avg_parse_ns: u64,
    ws_max_parse_ns: u64,
    ws_avg_apply_ns: u64,
    ws_max_apply_ns: u64,

    compute: PerfTestBenchComputeStats,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchReplayMultiReport {
    trace_path: String,
    pace_enabled: bool,
    replay_limit: Option<u64>,
    repeat: usize,
    runs: Vec<PerfTestBenchReplayReport>,
    avg: PerfTestBenchReplayAgg,
    min: PerfTestBenchReplayAgg,
    max: PerfTestBenchReplayAgg,
}

#[derive(Debug, Serialize, Clone)]
struct PerfTestBenchReplayAgg {
    ingest_duration_sec: f64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    compute_duration_sec: f64,
    spfa_avg_ns: f64,
    sim_avg_ns: f64,

    cycles_found_avg: f64,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchLoopComputeStats {
    max_depth: usize,
    base_assets: Vec<String>,
    sim_cycle_limit: usize,

    spfa_calls: u64,
    spfa_ns_sum: u64,
    spfa_avg_ns: u64,
    spfa_max_ns: u64,

    cycles_found_total: u64,
    cycles_found_avg: f64,
    cycles_found_max: u64,

    sim_calls: u64,
    sim_verified: u64,
    sim_errors: u64,
    sim_ns_sum: u64,
    sim_avg_ns: u64,
    sim_max_ns: u64,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchLoopReport {
    trace_path: String,
    warmup_frames: u64,
    duration_sec_target: u64,
    duration_sec: f64,

    pair_count: usize,
    valid_pairs_after_warmup: usize,

    frames: u64,
    bytes: u64,
    wrap_count: u64,

    loops: u64,
    loops_per_sec: f64,

    ws_msg_count: u64,
    ws_msg_bytes: u64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,

    ws_avg_parse_ns: u64,
    ws_max_parse_ns: u64,
    ws_avg_apply_ns: u64,
    ws_max_apply_ns: u64,

    compute: PerfTestBenchLoopComputeStats,
}

#[derive(Debug, Serialize)]
struct PerfTestBenchLoopMultiReport {
    trace_path: String,
    warmup_frames: u64,
    duration_sec_target: u64,
    replay_limit: Option<u64>,
    repeat: usize,
    runs: Vec<PerfTestBenchLoopReport>,
    avg: PerfTestBenchLoopAgg,
    min: PerfTestBenchLoopAgg,
    max: PerfTestBenchLoopAgg,
}

#[derive(Debug, Serialize, Clone)]
struct PerfTestBenchLoopAgg {
    loops_per_sec: f64,
    ws_msg_per_sec: f64,
    ws_mb_per_sec: f64,
    spfa_avg_ns: f64,
    sim_avg_ns: f64,
    cycles_found_avg: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PerfTraceEncoding {
    Utf8,
    Hex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PerfTraceLine {
    Meta {
        version: u32,
        kind: String,
        recorded_at_epoch_ms: u64,
        taker_fee_rate: Decimal,
        ws_chunk_size: usize,
        pairs: Vec<PerfTracePair>,
    },
    Frame {
        t_us: u64,
        chunk: u32,
        encoding: PerfTraceEncoding,
        payload: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerfTracePair {
    pair: String,
    binance_symbol: String,
    base: String,
    quote: String,
}

pub async fn run(args: &[String]) -> anyhow::Result<()> {
    let subargs = perf_test_subargs(args);
    if subargs.first().is_some_and(|s| s == "micro") {
        return run_micro(subargs.get(1..).unwrap_or_default()).await;
    }
    run_bench(subargs).await
}

async fn run_micro(subargs: &[String]) -> anyhow::Result<()> {
    match subargs.first().map(|s| s.as_str()) {
        None => {
            let trace = trace_path_from_env_or_default();
            if trace.exists() {
                return run_replay_micro(trace).await;
            }
            print_usage();
            anyhow::bail!(
                "未找到 trace 文件：{}（先运行 perf-test record 录制基准）",
                trace.display()
            );
        }
        Some("live") => run_live_micro().await,
        Some("record") => {
            let trace = trace_path_from_arg_env_or_default(subargs.get(1))?;
            run_record(trace).await
        }
        Some("replay") => {
            let trace = trace_path_from_arg_env_or_default(subargs.get(1))?;
            run_replay_micro(trace).await
        }
        Some("help") | Some("-h") | Some("--help") => {
            print_usage();
            Ok(())
        }
        Some(maybe_path) => {
            let trace = PathBuf::from(maybe_path);
            if trace.exists() {
                return run_replay_micro(trace).await;
            }
            print_usage();
            anyhow::bail!("未知 perf-test micro 子命令: {}", maybe_path);
        }
    }
}

async fn run_bench(subargs: &[String]) -> anyhow::Result<()> {
    match subargs.first().map(|s| s.as_str()) {
        None => {
            let trace = trace_path_from_env_or_default();
            if trace.exists() {
                return run_replay_bench(trace).await;
            }
            print_usage();
            anyhow::bail!(
                "未找到 trace 文件：{}（先运行 perf-test record 录制基准）",
                trace.display()
            );
        }
        Some("live") => run_live_bench().await,
        Some("record") => {
            let trace = trace_path_from_arg_env_or_default(subargs.get(1))?;
            run_record(trace).await
        }
        Some("replay") => {
            let trace = trace_path_from_arg_env_or_default(subargs.get(1))?;
            run_replay_bench(trace).await
        }
        Some("loop") => {
            let trace = trace_path_from_arg_env_or_default(subargs.get(1))?;
            run_replay_loop_bench(trace).await
        }
        Some("help") | Some("-h") | Some("--help") => {
            print_usage();
            Ok(())
        }
        Some(maybe_path) => {
            let trace = PathBuf::from(maybe_path);
            if trace.exists() {
                return run_replay_bench(trace).await;
            }
            print_usage();
            anyhow::bail!("未知 perf-test 子命令: {}", maybe_path);
        }
    }
}

async fn run_live_micro() -> anyhow::Result<()> {
    let duration = Duration::from_secs(env_u64_or("PERF_TEST_DURATION_SEC", 100));
    let max_pairs = env_u64("PERF_TEST_MAX_PAIRS").map(|n| n as usize);

    let cfg = Arc::new(RwLock::new(Config::from_env_or_default()));
    let rest = BinanceRestClient::new_public()?;

    let (markets, websocket_symbols) =
        select_pairs_for_online(cfg.clone(), &rest, max_pairs).await?;

    let ws_chunk_size = cfg.read().await.websocket_chunk_size;
    info!(
        "perf-test live: 将监听 {} 个交易对，chunk_size={}",
        websocket_symbols.len(),
        ws_chunk_size
    );

    let tickers = Arc::new(TickerStore::new(websocket_symbols.clone()));
    let graph = Arc::new(build_static_graph(
        &markets,
        &websocket_symbols,
        cfg.read().await.taker_fee_rate,
    )?);

    let perf = Arc::new(PerfCounters::default());
    let ticker_notify = Arc::new(tokio::sync::Notify::new());
    let ticker_notify_armed = Arc::new(AtomicBool::new(false));

    let (ws_tasks, ws_conn_ok, ws_chunk_pair_counts) = spawn_book_ticker_streams(
        websocket_symbols.clone(),
        markets,
        tickers,
        graph,
        ticker_notify,
        ticker_notify_armed,
        perf.clone(),
        ws_chunk_size,
    );

    if !wait_any_connected(&ws_conn_ok, Duration::from_secs(10)).await {
        for t in ws_tasks {
            t.abort();
        }
        anyhow::bail!("perf-test live: 10s 内未建立任何 WebSocket 连接（检查网络/代理/DNS）");
    }

    perf.reset_ws();
    let start = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = start.elapsed();

    for t in ws_tasks {
        t.abort();
    }

    let duration_sec = elapsed.as_secs_f64().max(1e-9);
    let ws_chunks_total = ws_conn_ok.len();
    let ws_chunks_connected = ws_conn_ok
        .iter()
        .filter(|b| b.load(Ordering::Relaxed))
        .count();

    let pairs_connected_estimate: usize = ws_chunk_pair_counts
        .iter()
        .zip(ws_conn_ok.iter())
        .filter(|(_, ok)| ok.load(Ordering::Relaxed))
        .map(|(n, _)| *n)
        .sum();

    let ws_msg_count = perf.ws_msg_count.load(Ordering::Relaxed);
    let ws_msg_bytes = perf.ws_msg_bytes.load(Ordering::Relaxed);

    let ws_avg_parse_ns = perf.ws_avg_parse_ns();
    let ws_avg_apply_ns = perf.ws_avg_apply_ns();

    let report = PerfTestLiveReport {
        duration_sec,

        pair_count: websocket_symbols.len(),
        ws_chunk_size,
        ws_chunks_total,
        ws_chunks_connected,
        pairs_connected_estimate,

        ws_msg_count,
        ws_msg_bytes,
        ws_msg_per_sec: (ws_msg_count as f64) / duration_sec,
        ws_mb_per_sec: (ws_msg_bytes as f64) / (1024.0 * 1024.0) / duration_sec,

        ws_avg_parse_ns,
        ws_max_parse_ns: perf.ws_parse_ns_max.load(Ordering::Relaxed),
        ws_avg_apply_ns,
        ws_max_apply_ns: perf.ws_apply_ns_max.load(Ordering::Relaxed),
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_live_bench() -> anyhow::Result<()> {
    let duration = Duration::from_secs(env_u64_or("PERF_TEST_DURATION_SEC", 100));
    let max_pairs = env_u64("PERF_TEST_MAX_PAIRS").map(|n| n as usize);
    let bench_cycles = env_u64_or("PERF_TEST_BENCH_CYCLES", 50).max(1);
    let sim_cycle_limit = env_u64_or("PERF_TEST_SIM_CYCLE_LIMIT", 50) as usize;

    let cfg = Arc::new(RwLock::new(Config::from_env_or_default()));
    let rest = BinanceRestClient::new_public()?;

    let (markets, websocket_symbols) =
        select_pairs_for_online(cfg.clone(), &rest, max_pairs).await?;

    let ws_chunk_size = cfg.read().await.websocket_chunk_size;
    info!(
        "perf-test live: 将监听 {} 个交易对，chunk_size={}",
        websocket_symbols.len(),
        ws_chunk_size
    );

    let tickers = Arc::new(TickerStore::new(websocket_symbols.clone()));
    let graph = Arc::new(build_static_graph(
        &markets,
        &websocket_symbols,
        cfg.read().await.taker_fee_rate,
    )?);

    let perf = Arc::new(PerfCounters::default());
    let ticker_notify = Arc::new(tokio::sync::Notify::new());
    let ticker_notify_armed = Arc::new(AtomicBool::new(false));

    let (ws_tasks, ws_conn_ok, ws_chunk_pair_counts) = spawn_book_ticker_streams(
        websocket_symbols.clone(),
        markets.clone(),
        tickers.clone(),
        graph.clone(),
        ticker_notify,
        ticker_notify_armed,
        perf.clone(),
        ws_chunk_size,
    );

    if !wait_any_connected(&ws_conn_ok, Duration::from_secs(10)).await {
        for t in ws_tasks {
            t.abort();
        }
        anyhow::bail!("perf-test live: 10s 内未建立任何 WebSocket 连接（检查网络/代理/DNS）");
    }

    perf.reset_ws();
    let start = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = start.elapsed();

    for t in ws_tasks {
        t.abort();
    }

    let duration_sec = elapsed.as_secs_f64().max(1e-9);
    let ws_chunks_total = ws_conn_ok.len();
    let ws_chunks_connected = ws_conn_ok
        .iter()
        .filter(|b| b.load(Ordering::Relaxed))
        .count();

    let pairs_connected_estimate: usize = ws_chunk_pair_counts
        .iter()
        .zip(ws_conn_ok.iter())
        .filter(|(_, ok)| ok.load(Ordering::Relaxed))
        .map(|(n, _)| *n)
        .sum();

    let ws_msg_count = perf.ws_msg_count.load(Ordering::Relaxed);
    let ws_msg_bytes = perf.ws_msg_bytes.load(Ordering::Relaxed);

    let ws_avg_parse_ns = perf.ws_avg_parse_ns();
    let ws_avg_apply_ns = perf.ws_avg_apply_ns();

    let mut cfg_snapshot = cfg.read().await.clone();
    cfg_snapshot.auto_trade_enabled = false;
    let compute = run_compute_bench(
        graph.as_ref(),
        tickers.as_ref(),
        &websocket_symbols,
        &markets,
        &cfg_snapshot,
        bench_cycles,
        sim_cycle_limit,
    )?;

    eprintln!(
        "perf-test live: ingest {:.3}s，{:.0} msg/s，{:.2} MB/s；SPFA avg {:.3} ms，sim avg {:.3} µs",
        duration_sec,
        (ws_msg_count as f64) / duration_sec,
        (ws_msg_bytes as f64) / (1024.0 * 1024.0) / duration_sec,
        (compute.spfa_avg_ns as f64) / 1_000_000.0,
        (compute.sim_avg_ns as f64) / 1000.0,
    );

    let report = serde_json::json!({
        "mode": "bench_live",
        "duration_sec": duration_sec,
        "pair_count": websocket_symbols.len(),
        "ws_chunk_size": ws_chunk_size,
        "ws_chunks_total": ws_chunks_total,
        "ws_chunks_connected": ws_chunks_connected,
        "pairs_connected_estimate": pairs_connected_estimate,
        "ws_msg_count": ws_msg_count,
        "ws_msg_bytes": ws_msg_bytes,
        "ws_msg_per_sec": (ws_msg_count as f64) / duration_sec,
        "ws_mb_per_sec": (ws_msg_bytes as f64) / (1024.0 * 1024.0) / duration_sec,
        "ws_avg_parse_ns": ws_avg_parse_ns,
        "ws_max_parse_ns": perf.ws_parse_ns_max.load(Ordering::Relaxed),
        "ws_avg_apply_ns": ws_avg_apply_ns,
        "ws_max_apply_ns": perf.ws_apply_ns_max.load(Ordering::Relaxed),
        "compute": compute,
    });

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_record(trace_path: PathBuf) -> anyhow::Result<()> {
    let duration_sec = env_u64_or("PERF_TEST_DURATION_SEC", 100);
    let duration = Duration::from_secs(duration_sec);
    let max_pairs = env_u64("PERF_TEST_MAX_PAIRS").map(|n| n as usize);

    ensure_trace_parent_dir(&trace_path).await?;
    let tmp_path = trace_path_tmp_path(&trace_path);
    if tmp_path.exists() {
        let _ = tokio::fs::remove_file(&tmp_path).await;
    }

    let cfg = Arc::new(RwLock::new(Config::from_env_or_default()));
    let rest = BinanceRestClient::new_public()?;
    let (markets, websocket_symbols) =
        select_pairs_for_online(cfg.clone(), &rest, max_pairs).await?;
    let ws_chunk_size = cfg.read().await.websocket_chunk_size;

    let pairs_meta = build_pairs_meta(&markets, &websocket_symbols)?;
    let meta = PerfTraceLine::Meta {
        version: TRACE_VERSION,
        kind: TRACE_KIND_BOOK_TICKER.to_string(),
        recorded_at_epoch_ms: now_ms(),
        taker_fee_rate: cfg.read().await.taker_fee_rate,
        ws_chunk_size,
        pairs: pairs_meta,
    };

    let stream_chunks =
        build_book_ticker_stream_chunks(&markets, &websocket_symbols, ws_chunk_size)?;
    let (tx, rx) = tokio::sync::mpsc::channel::<PerfTraceLine>(8192);

    let tmp_path_for_writer = tmp_path.clone();
    let writer =
        tokio::spawn(async move { write_trace_file(&tmp_path_for_writer, meta, rx).await });

    let start = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let tasks: Vec<tokio::task::JoinHandle<()>> = stream_chunks
        .into_iter()
        .enumerate()
        .map(|(chunk, streams)| {
            let tx = tx.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                record_ws_chunk(chunk as u32, streams, tx, start, stop).await;
            })
        })
        .collect();
    drop(tx);

    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.abort();
    }

    let stats = writer
        .await
        .context("perf-test record: 写入任务异常退出")??;

    finalize_trace_file(&tmp_path, &trace_path).await?;

    if stats.frames == 0 {
        anyhow::bail!("perf-test record: 未录到任何消息（检查网络/代理/DNS）");
    }

    let trace_duration_sec = (stats.last_t_us as f64) / 1_000_000.0;
    let report = PerfTestRecordReport {
        trace_path: trace_path.display().to_string(),
        duration_sec,
        trace_duration_sec,

        pair_count: websocket_symbols.len(),
        ws_chunk_size,
        ws_chunks_total: stats.ws_chunks_total,

        frames: stats.frames,
        bytes: stats.bytes,
        mb: (stats.bytes as f64) / (1024.0 * 1024.0),
        frames_per_sec: (stats.frames as f64) / trace_duration_sec.max(1e-9),
        mb_per_sec: ((stats.bytes as f64) / (1024.0 * 1024.0)) / trace_duration_sec.max(1e-9),
        trace_last_t_us: stats.last_t_us,
    };

    eprintln!(
        "perf-test record: 录制 {:.3}s，{} pairs，{} conns，{} frames，{:.2} MB（{:.0} msg/s，{:.3} MB/s）",
        report.trace_duration_sec,
        report.pair_count,
        report.ws_chunks_total,
        report.frames,
        report.mb,
        report.frames_per_sec,
        report.mb_per_sec,
    );
    eprintln!("perf-test record: trace={}", report.trace_path);
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_replay_bench(trace_path: PathBuf) -> anyhow::Result<()> {
    let pace_enabled = env_bool("PERF_TEST_REPLAY_PACE").unwrap_or(false);
    let replay_limit =
        env_u64("PERF_TEST_REPLAY_LIMIT").and_then(|n| if n == 0 { None } else { Some(n) });
    let repeat = env_u64_or("PERF_TEST_REPEAT", 1).max(1).min(1000) as usize;

    let bench_cycles = env_u64_or("PERF_TEST_BENCH_CYCLES", 50).max(1);
    let sim_cycle_limit = env_u64_or("PERF_TEST_SIM_CYCLE_LIMIT", 50) as usize;

    if repeat == 1 {
        let report = replay_bench_once(
            &trace_path,
            pace_enabled,
            replay_limit,
            bench_cycles,
            sim_cycle_limit,
        )
        .await?;
        print_bench_human_summary(None, &report);
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let mut runs = Vec::with_capacity(repeat);
    for i in 0..repeat {
        let report = replay_bench_once(
            &trace_path,
            pace_enabled,
            replay_limit,
            bench_cycles,
            sim_cycle_limit,
        )
        .await?;
        print_bench_human_summary(Some((i + 1, repeat)), &report);
        runs.push(report);
    }

    let (avg, min, max) = aggregate_bench_runs(&runs);
    eprintln!(
        "perf-test bench replay: avg：ingest {:.3}s，compute {:.3}s，SPFA avg {:.3} ms，sim avg {:.3} µs",
        avg.ingest_duration_sec,
        avg.compute_duration_sec,
        avg.spfa_avg_ns / 1_000_000.0,
        avg.sim_avg_ns / 1000.0,
    );
    eprintln!(
        "perf-test bench replay: avg：吞吐 {:+.0} msg/s，{:.2} MB/s，cycles/call {:.2}",
        avg.ws_msg_per_sec, avg.ws_mb_per_sec, avg.cycles_found_avg,
    );
    eprintln!(
        "perf-test bench replay: min/max：ingest {:.3}/{:.3}s，compute {:.3}/{:.3}s",
        min.ingest_duration_sec,
        max.ingest_duration_sec,
        min.compute_duration_sec,
        max.compute_duration_sec,
    );

    let out = PerfTestBenchReplayMultiReport {
        trace_path: trace_path.display().to_string(),
        pace_enabled,
        replay_limit,
        repeat,
        runs,
        avg,
        min,
        max,
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

async fn replay_bench_once(
    trace_path: &Path,
    pace_enabled: bool,
    replay_limit: Option<u64>,
    bench_cycles: u64,
    sim_cycle_limit: usize,
) -> anyhow::Result<PerfTestBenchReplayReport> {
    let file = tokio::fs::File::open(trace_path).await.with_context(|| {
        format!(
            "perf-test replay: 打开 trace 失败: {}",
            trace_path.display()
        )
    })?;
    let mut reader = tokio::io::BufReader::new(file);

    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        anyhow::bail!("perf-test replay: trace 为空: {}", trace_path.display());
    }

    let meta = parse_trace_meta(&line)?;
    validate_trace_meta(&meta)?;

    let websocket_symbols: Vec<String> = meta.pairs.iter().map(|p| p.pair.clone()).collect();
    let markets = build_markets_from_meta(&meta);
    let binance_to_id = build_binance_to_id_map(&meta);

    let tickers = TickerStore::new(websocket_symbols.clone());
    let graph = build_static_graph(&markets, &websocket_symbols, meta.taker_fee_rate)?;

    let ticker_notify = tokio::sync::Notify::new();
    let ticker_notify_armed = AtomicBool::new(false);
    let perf = PerfCounters::default();
    perf.reset_ws();

    let replay_start = Instant::now();
    let ingest_start = Instant::now();

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut trace_last_t_us = 0u64;

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        let PerfTraceLine::Frame {
            t_us,
            encoding,
            payload,
            ..
        } = serde_json::from_str::<PerfTraceLine>(trimmed)
            .context("perf-test replay: 解析 frame 失败")?
        else {
            continue;
        };

        trace_last_t_us = trace_last_t_us.max(t_us);
        if pace_enabled {
            let target = replay_start + Duration::from_micros(t_us);
            if let Some(remaining) = target.checked_duration_since(Instant::now()) {
                tokio::time::sleep(remaining).await;
            }
        }

        let bytes_vec = match encoding {
            PerfTraceEncoding::Utf8 => payload.into_bytes(),
            PerfTraceEncoding::Hex => {
                hex::decode(payload).context("perf-test replay: hex 解码失败")?
            }
        };
        bytes += bytes_vec.len() as u64;

        handle_book_ticker_ws_message(
            bytes_vec,
            &tickers,
            &graph,
            &ticker_notify,
            &ticker_notify_armed,
            &perf,
            &binance_to_id,
        )?;

        frames += 1;
        if let Some(limit) = replay_limit {
            if frames >= limit {
                break;
            }
        }
    }

    let trace_duration_sec = (trace_last_t_us as f64) / 1_000_000.0;
    let ingest_duration_sec = ingest_start.elapsed().as_secs_f64().max(1e-9);

    let ws_msg_count = perf.ws_msg_count.load(Ordering::Relaxed);
    let ws_msg_bytes = perf.ws_msg_bytes.load(Ordering::Relaxed);
    let ws_avg_parse_ns = perf.ws_avg_parse_ns();
    let ws_avg_apply_ns = perf.ws_avg_apply_ns();

    let mut cfg = Config::from_env_or_default();
    cfg.set_fee_rate(meta.taker_fee_rate);
    cfg.auto_trade_enabled = false;

    let compute = run_compute_bench(
        &graph,
        &tickers,
        &websocket_symbols,
        &markets,
        &cfg,
        bench_cycles,
        sim_cycle_limit,
    )?;

    Ok(PerfTestBenchReplayReport {
        trace_path: trace_path.display().to_string(),
        pace_enabled,
        replay_limit,

        frames,
        bytes,
        trace_last_t_us,
        trace_duration_sec,

        ingest_duration_sec,
        ingest_wall_speedup_x: trace_duration_sec / ingest_duration_sec,

        ws_msg_count,
        ws_msg_bytes,
        ws_msg_per_sec: (ws_msg_count as f64) / ingest_duration_sec,
        ws_mb_per_sec: (ws_msg_bytes as f64) / (1024.0 * 1024.0) / ingest_duration_sec,

        ws_avg_parse_ns,
        ws_max_parse_ns: perf.ws_parse_ns_max.load(Ordering::Relaxed),
        ws_avg_apply_ns,
        ws_max_apply_ns: perf.ws_apply_ns_max.load(Ordering::Relaxed),

        compute,
    })
}

async fn run_replay_loop_bench(trace_path: PathBuf) -> anyhow::Result<()> {
    let warmup_frames = env_u64_or("PERF_TEST_LOOP_WARMUP_FRAMES", 100_000);
    let duration_sec_target = env_u64_or("PERF_TEST_LOOP_DURATION_SEC", 30).max(1);
    let replay_limit =
        env_u64("PERF_TEST_REPLAY_LIMIT").and_then(|n| if n == 0 { None } else { Some(n) });
    let repeat = env_u64_or("PERF_TEST_REPEAT", 1).max(1).min(1000) as usize;

    let sim_cycle_limit = env_u64_or("PERF_TEST_SIM_CYCLE_LIMIT", 50) as usize;

    if repeat == 1 {
        let report = replay_loop_once(
            &trace_path,
            warmup_frames,
            duration_sec_target,
            replay_limit,
            sim_cycle_limit,
        )
        .await?;
        print_loop_human_summary(None, &report);
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let mut runs = Vec::with_capacity(repeat);
    for i in 0..repeat {
        let report = replay_loop_once(
            &trace_path,
            warmup_frames,
            duration_sec_target,
            replay_limit,
            sim_cycle_limit,
        )
        .await?;
        print_loop_human_summary(Some((i + 1, repeat)), &report);
        runs.push(report);
    }

    let (avg, min, max) = aggregate_loop_runs(&runs);
    eprintln!(
        "perf-test bench loop: avg：{:.0} loops/s，SPFA avg {:.3} ms，sim avg {:.3} µs，cycles/call {:.2}",
        avg.loops_per_sec,
        avg.spfa_avg_ns / 1_000_000.0,
        avg.sim_avg_ns / 1000.0,
        avg.cycles_found_avg,
    );
    eprintln!(
        "perf-test bench loop: avg：吞吐 {:+.0} msg/s，{:.2} MB/s",
        avg.ws_msg_per_sec, avg.ws_mb_per_sec,
    );
    eprintln!(
        "perf-test bench loop: min/max：{:.0}/{:.0} loops/s",
        min.loops_per_sec, max.loops_per_sec,
    );

    let out = PerfTestBenchLoopMultiReport {
        trace_path: trace_path.display().to_string(),
        warmup_frames,
        duration_sec_target,
        replay_limit,
        repeat,
        runs,
        avg,
        min,
        max,
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

async fn replay_loop_once(
    trace_path: &Path,
    warmup_frames: u64,
    duration_sec_target: u64,
    replay_limit: Option<u64>,
    sim_cycle_limit: usize,
) -> anyhow::Result<PerfTestBenchLoopReport> {
    let file = tokio::fs::File::open(trace_path)
        .await
        .with_context(|| format!("perf-test loop: 打开 trace 失败: {}", trace_path.display()))?;
    let mut reader = tokio::io::BufReader::new(file);

    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        anyhow::bail!("perf-test loop: trace 为空: {}", trace_path.display());
    }

    let meta = parse_trace_meta(&line)?;
    validate_trace_meta(&meta)?;

    let websocket_symbols: Vec<String> = meta.pairs.iter().map(|p| p.pair.clone()).collect();
    let markets = build_markets_from_meta(&meta);
    let binance_to_id = build_binance_to_id_map(&meta);

    let tickers = TickerStore::new(websocket_symbols.clone());
    let graph = build_static_graph(&markets, &websocket_symbols, meta.taker_fee_rate)?;

    let ticker_notify = tokio::sync::Notify::new();
    let ticker_notify_armed = AtomicBool::new(false);
    let perf = PerfCounters::default();

    let mut warmup_done = 0u64;
    while warmup_done < warmup_frames {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            anyhow::bail!(
                "perf-test loop: trace frames 不足（warmup={}，done={}）: {}",
                warmup_frames,
                warmup_done,
                trace_path.display()
            );
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        let PerfTraceLine::Frame {
            encoding, payload, ..
        } = serde_json::from_str::<PerfTraceLine>(trimmed)
            .context("perf-test loop: 解析 frame 失败")?
        else {
            continue;
        };
        let bytes = match encoding {
            PerfTraceEncoding::Utf8 => payload.into_bytes(),
            PerfTraceEncoding::Hex => {
                hex::decode(payload).context("perf-test loop: hex 解码失败")?
            }
        };

        handle_book_ticker_ws_message(
            bytes,
            &tickers,
            &graph,
            &ticker_notify,
            &ticker_notify_armed,
            &perf,
            &binance_to_id,
        )?;
        warmup_done += 1;
    }

    let valid_pairs_after_warmup = tickers.valid_count();
    let loop_start_pos = reader.stream_position().await?;
    let file_len = tokio::fs::metadata(trace_path).await?.len();
    if loop_start_pos >= file_len {
        anyhow::bail!(
            "perf-test loop: warmup 已到文件末尾（pos={} len={}），无法开始计时：{}",
            loop_start_pos,
            file_len,
            trace_path.display()
        );
    }

    graph.maybe_rebuild_all_weights(&tickers);
    perf.reset_ws();

    let mut cfg = Config::from_env_or_default();
    cfg.set_fee_rate(meta.taker_fee_rate);
    cfg.auto_trade_enabled = false;

    let duration = Duration::from_secs(duration_sec_target);
    let bench_start = Instant::now();
    let deadline = bench_start + duration;

    let mut spfa_ws = SpfaWorkspace::new();

    let mut spfa_calls = 0u64;
    let mut spfa_ns_sum = 0u64;
    let mut spfa_ns_max = 0u64;

    let mut cycles_found_total = 0u64;
    let mut cycles_found_max = 0u64;

    let mut sim_calls = 0u64;
    let mut sim_verified = 0u64;
    let mut sim_errors = 0u64;
    let mut sim_ns_sum = 0u64;
    let mut sim_ns_max = 0u64;

    let mut frames = 0u64;
    let mut bytes_total = 0u64;
    let mut wrap_count = 0u64;

    while Instant::now() < deadline {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            reader.seek(SeekFrom::Start(loop_start_pos)).await?;
            wrap_count += 1;
            continue;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        let PerfTraceLine::Frame {
            encoding, payload, ..
        } = serde_json::from_str::<PerfTraceLine>(trimmed)
            .context("perf-test loop: 解析 frame 失败")?
        else {
            continue;
        };

        let bytes = match encoding {
            PerfTraceEncoding::Utf8 => payload.into_bytes(),
            PerfTraceEncoding::Hex => {
                hex::decode(payload).context("perf-test loop: hex 解码失败")?
            }
        };
        bytes_total += bytes.len() as u64;

        handle_book_ticker_ws_message(
            bytes,
            &tickers,
            &graph,
            &ticker_notify,
            &ticker_notify_armed,
            &perf,
            &binance_to_id,
        )?;

        let spfa_start = Instant::now();
        let cycles = find_negative_cycles_spfa_with_workspace(
            &graph,
            &websocket_symbols,
            cfg.max_arbitrage_depth,
            &mut spfa_ws,
        )?;
        let spfa_ns = spfa_start.elapsed().as_nanos() as u64;

        spfa_calls += 1;
        spfa_ns_sum = spfa_ns_sum.saturating_add(spfa_ns);
        spfa_ns_max = spfa_ns_max.max(spfa_ns);

        let cycle_count = cycles.len() as u64;
        cycles_found_total = cycles_found_total.saturating_add(cycle_count);
        cycles_found_max = cycles_found_max.max(cycle_count);

        for cycle in cycles.iter().take(sim_cycle_limit) {
            for base_asset in &cfg.base_assets {
                let sim_start = Instant::now();
                let res = simulate_full(
                    cycle,
                    base_asset,
                    cfg.simulation_start_amount,
                    Some(base_asset),
                    &tickers,
                    &markets,
                    &cfg,
                );
                let sim_ns = sim_start.elapsed().as_nanos() as u64;

                sim_calls += 1;
                sim_ns_sum = sim_ns_sum.saturating_add(sim_ns);
                sim_ns_max = sim_ns_max.max(sim_ns);

                match res {
                    Ok(sim) => {
                        if sim.verified {
                            sim_verified += 1;
                        }
                    }
                    Err(_) => {
                        sim_errors += 1;
                    }
                }
            }
        }

        frames += 1;
        if let Some(limit) = replay_limit {
            if frames >= limit {
                break;
            }
        }
    }

    let duration_sec = bench_start.elapsed().as_secs_f64().max(1e-9);
    let loops_per_sec = (spfa_calls as f64) / duration_sec;

    let ws_msg_count = perf.ws_msg_count.load(Ordering::Relaxed);
    let ws_msg_bytes = perf.ws_msg_bytes.load(Ordering::Relaxed);
    let ws_avg_parse_ns = perf.ws_avg_parse_ns();
    let ws_avg_apply_ns = perf.ws_avg_apply_ns();

    let spfa_avg_ns = if spfa_calls == 0 {
        0
    } else {
        spfa_ns_sum / spfa_calls
    };
    let sim_avg_ns = if sim_calls == 0 {
        0
    } else {
        sim_ns_sum / sim_calls
    };
    let cycles_found_avg = if spfa_calls == 0 {
        0.0
    } else {
        (cycles_found_total as f64) / (spfa_calls as f64)
    };

    Ok(PerfTestBenchLoopReport {
        trace_path: trace_path.display().to_string(),
        warmup_frames,
        duration_sec_target,
        duration_sec,

        pair_count: meta.pairs.len(),
        valid_pairs_after_warmup,

        frames,
        bytes: bytes_total,
        wrap_count,

        loops: spfa_calls,
        loops_per_sec,

        ws_msg_count,
        ws_msg_bytes,
        ws_msg_per_sec: (ws_msg_count as f64) / duration_sec,
        ws_mb_per_sec: (ws_msg_bytes as f64) / (1024.0 * 1024.0) / duration_sec,

        ws_avg_parse_ns,
        ws_max_parse_ns: perf.ws_parse_ns_max.load(Ordering::Relaxed),
        ws_avg_apply_ns,
        ws_max_apply_ns: perf.ws_apply_ns_max.load(Ordering::Relaxed),

        compute: PerfTestBenchLoopComputeStats {
            max_depth: cfg.max_arbitrage_depth,
            base_assets: cfg.base_assets.clone(),
            sim_cycle_limit,

            spfa_calls,
            spfa_ns_sum,
            spfa_avg_ns,
            spfa_max_ns: spfa_ns_max,

            cycles_found_total,
            cycles_found_avg,
            cycles_found_max,

            sim_calls,
            sim_verified,
            sim_errors,
            sim_ns_sum,
            sim_avg_ns,
            sim_max_ns: sim_ns_max,
        },
    })
}

fn print_loop_human_summary(run: Option<(usize, usize)>, report: &PerfTestBenchLoopReport) {
    let prefix = match run {
        Some((idx, total)) => format!("perf-test bench loop[{}/{}]", idx, total),
        None => "perf-test bench loop".to_string(),
    };
    eprintln!(
        "{prefix}: warmup {} frames（valid {}/{}），计时 {:.3}s，{:.0} loops/s，{:.0} msg/s",
        report.warmup_frames,
        report.valid_pairs_after_warmup,
        report.pair_count,
        report.duration_sec,
        report.loops_per_sec,
        report.ws_msg_per_sec,
    );
    eprintln!(
        "{prefix}: SPFA avg {:.3} ms / max {:.3} ms，sim avg {:.3} µs / max {:.3} µs，cycles/call {:.2}",
        (report.compute.spfa_avg_ns as f64) / 1_000_000.0,
        (report.compute.spfa_max_ns as f64) / 1_000_000.0,
        (report.compute.sim_avg_ns as f64) / 1000.0,
        (report.compute.sim_max_ns as f64) / 1000.0,
        report.compute.cycles_found_avg,
    );
    eprintln!(
        "{prefix}: parse avg {:.3} µs，apply avg {:.3} µs，wraps={}",
        (report.ws_avg_parse_ns as f64) / 1000.0,
        (report.ws_avg_apply_ns as f64) / 1000.0,
        report.wrap_count
    );
    if run.is_none() {
        eprintln!("{prefix}: trace={}", report.trace_path);
    }
}

fn aggregate_loop_runs(
    runs: &[PerfTestBenchLoopReport],
) -> (
    PerfTestBenchLoopAgg,
    PerfTestBenchLoopAgg,
    PerfTestBenchLoopAgg,
) {
    let Some(_first) = runs.first() else {
        let z = PerfTestBenchLoopAgg {
            loops_per_sec: 0.0,
            ws_msg_per_sec: 0.0,
            ws_mb_per_sec: 0.0,
            spfa_avg_ns: 0.0,
            sim_avg_ns: 0.0,
            cycles_found_avg: 0.0,
        };
        return (z.clone(), z.clone(), z);
    };

    let mut min = PerfTestBenchLoopAgg {
        loops_per_sec: f64::INFINITY,
        ws_msg_per_sec: f64::INFINITY,
        ws_mb_per_sec: f64::INFINITY,
        spfa_avg_ns: f64::INFINITY,
        sim_avg_ns: f64::INFINITY,
        cycles_found_avg: f64::INFINITY,
    };
    let mut max = PerfTestBenchLoopAgg {
        loops_per_sec: 0.0,
        ws_msg_per_sec: 0.0,
        ws_mb_per_sec: 0.0,
        spfa_avg_ns: 0.0,
        sim_avg_ns: 0.0,
        cycles_found_avg: 0.0,
    };

    let mut sum_loops_s = 0.0;
    let mut sum_msg_s = 0.0;
    let mut sum_mb_s = 0.0;
    let mut sum_spfa_avg = 0.0;
    let mut sum_sim_avg = 0.0;
    let mut sum_cycles_avg = 0.0;

    for r in runs {
        min.loops_per_sec = min.loops_per_sec.min(r.loops_per_sec);
        max.loops_per_sec = max.loops_per_sec.max(r.loops_per_sec);

        min.ws_msg_per_sec = min.ws_msg_per_sec.min(r.ws_msg_per_sec);
        max.ws_msg_per_sec = max.ws_msg_per_sec.max(r.ws_msg_per_sec);

        min.ws_mb_per_sec = min.ws_mb_per_sec.min(r.ws_mb_per_sec);
        max.ws_mb_per_sec = max.ws_mb_per_sec.max(r.ws_mb_per_sec);

        let spfa_avg = r.compute.spfa_avg_ns as f64;
        min.spfa_avg_ns = min.spfa_avg_ns.min(spfa_avg);
        max.spfa_avg_ns = max.spfa_avg_ns.max(spfa_avg);

        let sim_avg = r.compute.sim_avg_ns as f64;
        min.sim_avg_ns = min.sim_avg_ns.min(sim_avg);
        max.sim_avg_ns = max.sim_avg_ns.max(sim_avg);

        min.cycles_found_avg = min.cycles_found_avg.min(r.compute.cycles_found_avg);
        max.cycles_found_avg = max.cycles_found_avg.max(r.compute.cycles_found_avg);

        sum_loops_s += r.loops_per_sec;
        sum_msg_s += r.ws_msg_per_sec;
        sum_mb_s += r.ws_mb_per_sec;
        sum_spfa_avg += spfa_avg;
        sum_sim_avg += sim_avg;
        sum_cycles_avg += r.compute.cycles_found_avg;
    }

    let n = (runs.len() as f64).max(1.0);
    let avg = PerfTestBenchLoopAgg {
        loops_per_sec: sum_loops_s / n,
        ws_msg_per_sec: sum_msg_s / n,
        ws_mb_per_sec: sum_mb_s / n,
        spfa_avg_ns: sum_spfa_avg / n,
        sim_avg_ns: sum_sim_avg / n,
        cycles_found_avg: sum_cycles_avg / n,
    };

    (avg, min, max)
}

fn print_bench_human_summary(run: Option<(usize, usize)>, report: &PerfTestBenchReplayReport) {
    let prefix = match run {
        Some((idx, total)) => format!("perf-test bench replay[{}/{}]", idx, total),
        None => "perf-test bench replay".to_string(),
    };
    eprintln!(
        "{prefix}: ingest {:.3}s（{:.1}x trace），{:.0} msg/s，{:.2} MB/s",
        report.ingest_duration_sec,
        report.ingest_wall_speedup_x,
        report.ws_msg_per_sec,
        report.ws_mb_per_sec,
    );
    eprintln!(
        "{prefix}: compute {:.3}s（{} cycles），SPFA avg {:.3} ms，sim avg {:.3} µs，cycles/call {:.2}",
        report.compute.compute_duration_sec,
        report.compute.bench_cycles,
        (report.compute.spfa_avg_ns as f64) / 1_000_000.0,
        (report.compute.sim_avg_ns as f64) / 1000.0,
        report.compute.cycles_found_avg,
    );
    if run.is_none() {
        eprintln!("{prefix}: trace={}", report.trace_path);
    }
}

fn aggregate_bench_runs(
    runs: &[PerfTestBenchReplayReport],
) -> (
    PerfTestBenchReplayAgg,
    PerfTestBenchReplayAgg,
    PerfTestBenchReplayAgg,
) {
    let Some(first) = runs.first() else {
        let z = PerfTestBenchReplayAgg {
            ingest_duration_sec: 0.0,
            ws_msg_per_sec: 0.0,
            ws_mb_per_sec: 0.0,
            compute_duration_sec: 0.0,
            spfa_avg_ns: 0.0,
            sim_avg_ns: 0.0,
            cycles_found_avg: 0.0,
        };
        return (z.clone(), z.clone(), z);
    };

    let mut min = PerfTestBenchReplayAgg {
        ingest_duration_sec: f64::INFINITY,
        ws_msg_per_sec: f64::INFINITY,
        ws_mb_per_sec: f64::INFINITY,
        compute_duration_sec: f64::INFINITY,
        spfa_avg_ns: f64::INFINITY,
        sim_avg_ns: f64::INFINITY,
        cycles_found_avg: f64::INFINITY,
    };
    let mut max = PerfTestBenchReplayAgg {
        ingest_duration_sec: 0.0,
        ws_msg_per_sec: 0.0,
        ws_mb_per_sec: 0.0,
        compute_duration_sec: 0.0,
        spfa_avg_ns: 0.0,
        sim_avg_ns: 0.0,
        cycles_found_avg: 0.0,
    };

    let mut sum_ingest_sec = 0.0;
    let mut sum_msg_s = 0.0;
    let mut sum_mb_s = 0.0;
    let mut sum_compute_sec = 0.0;
    let mut sum_spfa_avg = 0.0;
    let mut sum_sim_avg = 0.0;
    let mut sum_cycles_avg = 0.0;

    for r in runs {
        min.ingest_duration_sec = min.ingest_duration_sec.min(r.ingest_duration_sec);
        max.ingest_duration_sec = max.ingest_duration_sec.max(r.ingest_duration_sec);

        min.ws_msg_per_sec = min.ws_msg_per_sec.min(r.ws_msg_per_sec);
        max.ws_msg_per_sec = max.ws_msg_per_sec.max(r.ws_msg_per_sec);

        min.ws_mb_per_sec = min.ws_mb_per_sec.min(r.ws_mb_per_sec);
        max.ws_mb_per_sec = max.ws_mb_per_sec.max(r.ws_mb_per_sec);

        min.compute_duration_sec = min.compute_duration_sec.min(r.compute.compute_duration_sec);
        max.compute_duration_sec = max.compute_duration_sec.max(r.compute.compute_duration_sec);

        let spfa_avg = r.compute.spfa_avg_ns as f64;
        min.spfa_avg_ns = min.spfa_avg_ns.min(spfa_avg);
        max.spfa_avg_ns = max.spfa_avg_ns.max(spfa_avg);

        let sim_avg = r.compute.sim_avg_ns as f64;
        min.sim_avg_ns = min.sim_avg_ns.min(sim_avg);
        max.sim_avg_ns = max.sim_avg_ns.max(sim_avg);

        min.cycles_found_avg = min.cycles_found_avg.min(r.compute.cycles_found_avg);
        max.cycles_found_avg = max.cycles_found_avg.max(r.compute.cycles_found_avg);

        sum_ingest_sec += r.ingest_duration_sec;
        sum_msg_s += r.ws_msg_per_sec;
        sum_mb_s += r.ws_mb_per_sec;
        sum_compute_sec += r.compute.compute_duration_sec;
        sum_spfa_avg += spfa_avg;
        sum_sim_avg += sim_avg;
        sum_cycles_avg += r.compute.cycles_found_avg;
    }

    let n = (runs.len() as f64).max(1.0);
    let avg = PerfTestBenchReplayAgg {
        ingest_duration_sec: sum_ingest_sec / n,
        ws_msg_per_sec: sum_msg_s / n,
        ws_mb_per_sec: sum_mb_s / n,
        compute_duration_sec: sum_compute_sec / n,
        spfa_avg_ns: sum_spfa_avg / n,
        sim_avg_ns: sum_sim_avg / n,
        cycles_found_avg: sum_cycles_avg / n,
    };

    let _ = first;
    (avg, min, max)
}

fn run_compute_bench(
    graph: &crate::arbitrage::graph::Graph,
    tickers: &TickerStore,
    websocket_symbols: &[String],
    markets: &HashMap<String, Market>,
    cfg: &Config,
    bench_cycles: u64,
    sim_cycle_limit: usize,
) -> anyhow::Result<PerfTestBenchComputeStats> {
    let bench_cycles = bench_cycles.max(1);

    let mut spfa_ws = SpfaWorkspace::new();

    let mut spfa_calls = 0u64;
    let mut spfa_ns_sum = 0u64;
    let mut spfa_ns_max = 0u64;

    let mut cycles_found_total = 0u64;
    let mut cycles_found_max = 0u64;

    let mut sim_calls = 0u64;
    let mut sim_verified = 0u64;
    let mut sim_errors = 0u64;
    let mut sim_ns_sum = 0u64;
    let mut sim_ns_max = 0u64;

    let compute_start = Instant::now();
    for _ in 0..bench_cycles {
        graph.maybe_rebuild_all_weights(tickers);

        let spfa_start = Instant::now();
        let cycles = find_negative_cycles_spfa_with_workspace(
            graph,
            websocket_symbols,
            cfg.max_arbitrage_depth,
            &mut spfa_ws,
        )?;
        let spfa_ns = spfa_start.elapsed().as_nanos() as u64;

        spfa_calls += 1;
        spfa_ns_sum = spfa_ns_sum.saturating_add(spfa_ns);
        spfa_ns_max = spfa_ns_max.max(spfa_ns);

        let cycle_count = cycles.len() as u64;
        cycles_found_total = cycles_found_total.saturating_add(cycle_count);
        cycles_found_max = cycles_found_max.max(cycle_count);

        for cycle in cycles.iter().take(sim_cycle_limit) {
            for base_asset in &cfg.base_assets {
                let sim_start = Instant::now();
                let res = simulate_full(
                    cycle,
                    base_asset,
                    cfg.simulation_start_amount,
                    Some(base_asset),
                    tickers,
                    markets,
                    cfg,
                );
                let sim_ns = sim_start.elapsed().as_nanos() as u64;

                sim_calls += 1;
                sim_ns_sum = sim_ns_sum.saturating_add(sim_ns);
                sim_ns_max = sim_ns_max.max(sim_ns);

                match res {
                    Ok(sim) => {
                        if sim.verified {
                            sim_verified += 1;
                        }
                    }
                    Err(_) => {
                        sim_errors += 1;
                    }
                }
            }
        }
    }

    let compute_duration_sec = compute_start.elapsed().as_secs_f64();
    let spfa_avg_ns = if spfa_calls == 0 {
        0
    } else {
        spfa_ns_sum / spfa_calls
    };
    let sim_avg_ns = if sim_calls == 0 {
        0
    } else {
        sim_ns_sum / sim_calls
    };

    let cycles_found_avg = if spfa_calls == 0 {
        0.0
    } else {
        (cycles_found_total as f64) / (spfa_calls as f64)
    };

    Ok(PerfTestBenchComputeStats {
        bench_cycles,
        max_depth: cfg.max_arbitrage_depth,
        base_assets: cfg.base_assets.clone(),
        sim_cycle_limit,

        compute_duration_sec,

        spfa_calls,
        spfa_ns_sum,
        spfa_avg_ns,
        spfa_max_ns: spfa_ns_max,

        cycles_found_total,
        cycles_found_avg,
        cycles_found_max,

        sim_calls,
        sim_verified,
        sim_errors,
        sim_ns_sum,
        sim_avg_ns,
        sim_max_ns: sim_ns_max,
    })
}

async fn run_replay_micro(trace_path: PathBuf) -> anyhow::Result<()> {
    let pace_enabled = env_bool("PERF_TEST_REPLAY_PACE").unwrap_or(false);
    let replay_limit =
        env_u64("PERF_TEST_REPLAY_LIMIT").and_then(|n| if n == 0 { None } else { Some(n) });
    let repeat = env_u64_or("PERF_TEST_REPEAT", 1).max(1).min(1000) as usize;

    if repeat == 1 {
        let report = replay_once(&trace_path, pace_enabled, replay_limit).await?;
        print_replay_human_summary(None, &report);
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let mut runs = Vec::with_capacity(repeat);
    for i in 0..repeat {
        let report = replay_once(&trace_path, pace_enabled, replay_limit).await?;
        print_replay_human_summary(Some((i + 1, repeat)), &report);
        runs.push(report);
    }

    let (avg, min, max) = aggregate_replay_runs(&runs);
    eprintln!(
        "perf-test micro replay: avg：墙钟 {:.3}s（{:.1}x realtime），{:.0} msg/s，{:.2} MB/s",
        avg.duration_sec, avg.replay_speedup_x, avg.ws_msg_per_sec, avg.ws_mb_per_sec,
    );
    eprintln!(
        "perf-test micro replay: avg：parse avg {:.3} µs / max {:.3} µs，apply avg {:.3} µs / max {:.3} µs",
        avg.ws_avg_parse_ns / 1000.0,
        avg.ws_max_parse_ns / 1000.0,
        avg.ws_avg_apply_ns / 1000.0,
        avg.ws_max_apply_ns / 1000.0,
    );
    eprintln!(
        "perf-test micro replay: min/max：墙钟 {:.3}/{:.3}s，msg/s {:.0}/{:.0}，MB/s {:.2}/{:.2}",
        min.duration_sec,
        max.duration_sec,
        min.ws_msg_per_sec,
        max.ws_msg_per_sec,
        min.ws_mb_per_sec,
        max.ws_mb_per_sec,
    );
    eprintln!("perf-test micro replay: trace={}", trace_path.display());

    let out = PerfTestReplayMultiReport {
        trace_path: trace_path.display().to_string(),
        pace_enabled,
        replay_limit,
        repeat,
        runs,
        avg,
        min,
        max,
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

async fn replay_once(
    trace_path: &Path,
    pace_enabled: bool,
    replay_limit: Option<u64>,
) -> anyhow::Result<PerfTestReplayReport> {
    let file = tokio::fs::File::open(trace_path).await.with_context(|| {
        format!(
            "perf-test replay: 打开 trace 失败: {}",
            trace_path.display()
        )
    })?;
    let mut reader = tokio::io::BufReader::new(file);

    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        anyhow::bail!("perf-test replay: trace 为空: {}", trace_path.display());
    }
    let meta = parse_trace_meta(&line)?;
    validate_trace_meta(&meta)?;

    let websocket_symbols: Vec<String> = meta.pairs.iter().map(|p| p.pair.clone()).collect();
    let markets = build_markets_from_meta(&meta);
    let binance_to_id = build_binance_to_id_map(&meta);

    let tickers = TickerStore::new(websocket_symbols.clone());
    let graph = build_static_graph(&markets, &websocket_symbols, meta.taker_fee_rate)?;
    let ticker_notify = tokio::sync::Notify::new();
    let ticker_notify_armed = AtomicBool::new(false);
    let perf = PerfCounters::default();
    perf.reset_ws();

    let replay_start = Instant::now();
    let wall_start = Instant::now();

    let mut frames = 0u64;
    let mut trace_last_t_us = 0u64;

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        let PerfTraceLine::Frame {
            t_us,
            encoding,
            payload,
            ..
        } = serde_json::from_str::<PerfTraceLine>(trimmed)
            .context("perf-test replay: 解析 frame 失败")?
        else {
            continue;
        };

        trace_last_t_us = trace_last_t_us.max(t_us);
        if pace_enabled {
            let target = replay_start + Duration::from_micros(t_us);
            if let Some(remaining) = target.checked_duration_since(Instant::now()) {
                tokio::time::sleep(remaining).await;
            }
        }

        let bytes = match encoding {
            PerfTraceEncoding::Utf8 => payload.into_bytes(),
            PerfTraceEncoding::Hex => {
                hex::decode(payload).context("perf-test replay: hex 解码失败")?
            }
        };

        handle_book_ticker_ws_message(
            bytes,
            &tickers,
            &graph,
            &ticker_notify,
            &ticker_notify_armed,
            &perf,
            &binance_to_id,
        )?;

        frames += 1;
        if let Some(limit) = replay_limit {
            if frames >= limit {
                break;
            }
        }
    }

    let elapsed = wall_start.elapsed().as_secs_f64().max(1e-9);
    let trace_duration_sec = (trace_last_t_us as f64) / 1_000_000.0;
    let replay_speedup_x = trace_duration_sec / elapsed;

    let ws_msg_count = perf.ws_msg_count.load(Ordering::Relaxed);
    let ws_msg_bytes = perf.ws_msg_bytes.load(Ordering::Relaxed);

    let _ = replay_start;
    Ok(PerfTestReplayReport {
        trace_path: trace_path.display().to_string(),
        pace_enabled,

        frames,
        trace_last_t_us,
        trace_duration_sec,
        duration_sec: elapsed,
        replay_speedup_x,

        recorded_at_epoch_ms: meta.recorded_at_epoch_ms,
        ws_chunk_size: meta.ws_chunk_size,
        pair_count: meta.pairs.len(),

        ws_msg_count,
        ws_msg_bytes,
        ws_msg_per_sec: (ws_msg_count as f64) / elapsed,
        ws_mb_per_sec: (ws_msg_bytes as f64) / (1024.0 * 1024.0) / elapsed,

        ws_avg_parse_ns: perf.ws_avg_parse_ns(),
        ws_max_parse_ns: perf.ws_parse_ns_max.load(Ordering::Relaxed),
        ws_avg_apply_ns: perf.ws_avg_apply_ns(),
        ws_max_apply_ns: perf.ws_apply_ns_max.load(Ordering::Relaxed),
    })
}

fn print_replay_human_summary(run: Option<(usize, usize)>, report: &PerfTestReplayReport) {
    let prefix = match run {
        Some((idx, total)) => format!("perf-test micro replay[{}/{}]", idx, total),
        None => "perf-test micro replay".to_string(),
    };
    eprintln!(
        "{prefix}: 输入 {:.3}s，墙钟 {:.3}s（{:.1}x realtime），{} frames，{:.0} msg/s，{:.2} MB/s",
        report.trace_duration_sec,
        report.duration_sec,
        report.replay_speedup_x,
        report.frames,
        report.ws_msg_per_sec,
        report.ws_mb_per_sec,
    );
    eprintln!(
        "{prefix}: parse avg {:.3} µs / max {:.3} µs，apply avg {:.3} µs / max {:.3} µs",
        (report.ws_avg_parse_ns as f64) / 1000.0,
        (report.ws_max_parse_ns as f64) / 1000.0,
        (report.ws_avg_apply_ns as f64) / 1000.0,
        (report.ws_max_apply_ns as f64) / 1000.0,
    );
    if run.is_none() {
        eprintln!("{prefix}: trace={}", report.trace_path);
    }
}

fn aggregate_replay_runs(
    runs: &[PerfTestReplayReport],
) -> (PerfTestReplayAgg, PerfTestReplayAgg, PerfTestReplayAgg) {
    let Some(first) = runs.first() else {
        let z = PerfTestReplayAgg {
            frames: 0,
            trace_duration_sec: 0.0,
            duration_sec: 0.0,
            replay_speedup_x: 0.0,
            ws_msg_per_sec: 0.0,
            ws_mb_per_sec: 0.0,
            ws_avg_parse_ns: 0.0,
            ws_max_parse_ns: 0.0,
            ws_avg_apply_ns: 0.0,
            ws_max_apply_ns: 0.0,
        };
        return (z.clone(), z.clone(), z);
    };

    let mut min = PerfTestReplayAgg {
        frames: first.frames,
        trace_duration_sec: first.trace_duration_sec,
        duration_sec: f64::INFINITY,
        replay_speedup_x: f64::INFINITY,
        ws_msg_per_sec: f64::INFINITY,
        ws_mb_per_sec: f64::INFINITY,
        ws_avg_parse_ns: f64::INFINITY,
        ws_max_parse_ns: f64::INFINITY,
        ws_avg_apply_ns: f64::INFINITY,
        ws_max_apply_ns: f64::INFINITY,
    };
    let mut max = PerfTestReplayAgg {
        frames: first.frames,
        trace_duration_sec: first.trace_duration_sec,
        duration_sec: 0.0,
        replay_speedup_x: 0.0,
        ws_msg_per_sec: 0.0,
        ws_mb_per_sec: 0.0,
        ws_avg_parse_ns: 0.0,
        ws_max_parse_ns: 0.0,
        ws_avg_apply_ns: 0.0,
        ws_max_apply_ns: 0.0,
    };

    let mut sum_duration_sec = 0.0;
    let mut sum_speedup = 0.0;
    let mut sum_msg_s = 0.0;
    let mut sum_mb_s = 0.0;
    let mut sum_parse_avg = 0.0;
    let mut sum_parse_max = 0.0;
    let mut sum_apply_avg = 0.0;
    let mut sum_apply_max = 0.0;

    for r in runs {
        min.duration_sec = min.duration_sec.min(r.duration_sec);
        max.duration_sec = max.duration_sec.max(r.duration_sec);

        min.replay_speedup_x = min.replay_speedup_x.min(r.replay_speedup_x);
        max.replay_speedup_x = max.replay_speedup_x.max(r.replay_speedup_x);

        min.ws_msg_per_sec = min.ws_msg_per_sec.min(r.ws_msg_per_sec);
        max.ws_msg_per_sec = max.ws_msg_per_sec.max(r.ws_msg_per_sec);

        min.ws_mb_per_sec = min.ws_mb_per_sec.min(r.ws_mb_per_sec);
        max.ws_mb_per_sec = max.ws_mb_per_sec.max(r.ws_mb_per_sec);

        min.ws_avg_parse_ns = min.ws_avg_parse_ns.min(r.ws_avg_parse_ns as f64);
        max.ws_avg_parse_ns = max.ws_avg_parse_ns.max(r.ws_avg_parse_ns as f64);

        min.ws_max_parse_ns = min.ws_max_parse_ns.min(r.ws_max_parse_ns as f64);
        max.ws_max_parse_ns = max.ws_max_parse_ns.max(r.ws_max_parse_ns as f64);

        min.ws_avg_apply_ns = min.ws_avg_apply_ns.min(r.ws_avg_apply_ns as f64);
        max.ws_avg_apply_ns = max.ws_avg_apply_ns.max(r.ws_avg_apply_ns as f64);

        min.ws_max_apply_ns = min.ws_max_apply_ns.min(r.ws_max_apply_ns as f64);
        max.ws_max_apply_ns = max.ws_max_apply_ns.max(r.ws_max_apply_ns as f64);

        sum_duration_sec += r.duration_sec;
        sum_speedup += r.replay_speedup_x;
        sum_msg_s += r.ws_msg_per_sec;
        sum_mb_s += r.ws_mb_per_sec;
        sum_parse_avg += r.ws_avg_parse_ns as f64;
        sum_parse_max += r.ws_max_parse_ns as f64;
        sum_apply_avg += r.ws_avg_apply_ns as f64;
        sum_apply_max += r.ws_max_apply_ns as f64;
    }

    let n = runs.len() as f64;
    let avg = PerfTestReplayAgg {
        frames: first.frames,
        trace_duration_sec: first.trace_duration_sec,
        duration_sec: sum_duration_sec / n,
        replay_speedup_x: sum_speedup / n,
        ws_msg_per_sec: sum_msg_s / n,
        ws_mb_per_sec: sum_mb_s / n,
        ws_avg_parse_ns: sum_parse_avg / n,
        ws_max_parse_ns: sum_parse_max / n,
        ws_avg_apply_ns: sum_apply_avg / n,
        ws_max_apply_ns: sum_apply_max / n,
    };

    (avg, min, max)
}

fn perf_test_subargs(args: &[String]) -> &[String] {
    let pos = args
        .iter()
        .position(|a| a == "perf-test" || a == "--perf-test");
    match pos {
        Some(i) if i + 1 < args.len() => &args[i + 1..],
        _ => &[],
    }
}

fn trace_path_from_env_or_default() -> PathBuf {
    env::var("PERF_TEST_TRACE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_trace_path())
}

fn trace_path_from_arg_env_or_default(arg: Option<&String>) -> anyhow::Result<PathBuf> {
    if let Some(s) = arg {
        return Ok(PathBuf::from(s));
    }
    Ok(trace_path_from_env_or_default())
}

fn default_trace_path() -> PathBuf {
    PathBuf::from("perf_traces").join("book_ticker.jsonl")
}

fn trace_path_tmp_path(final_path: &Path) -> PathBuf {
    let mut p = final_path.to_path_buf();
    p.set_extension("tmp");
    p
}

async fn ensure_trace_parent_dir(trace_path: &Path) -> anyhow::Result<()> {
    let Some(dir) = trace_path.parent() else {
        return Ok(());
    };
    if dir.as_os_str().is_empty() {
        return Ok(());
    }
    tokio::fs::create_dir_all(dir).await?;
    Ok(())
}

async fn finalize_trace_file(tmp_path: &Path, final_path: &Path) -> anyhow::Result<()> {
    if final_path.exists() {
        tokio::fs::remove_file(final_path).await?;
    }
    tokio::fs::rename(tmp_path, final_path).await?;
    Ok(())
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.trim().parse::<u64>().ok()
}

fn env_u64_or(key: &str, default_value: u64) -> u64 {
    env_u64(key).unwrap_or(default_value)
}

fn env_bool(key: &str) -> Option<bool> {
    let v = env::var(key).ok()?;
    let v = v.trim().to_lowercase();
    Some(matches!(v.as_str(), "1" | "true" | "yes" | "on"))
}

fn print_usage() {
    eprintln!(concat!(
        "perf-test 用法（默认 bench）:\n",
        "  cargo run --release -- perf-test record [trace_path]\n",
        "  cargo run --release -- perf-test replay [trace_path]\n",
        "  cargo run --release -- perf-test loop [trace_path]\n",
        "  cargo run --release -- perf-test live\n",
        "  cargo run --release -- perf-test <trace_path>\n",
        "\n",
        "micro（摄入链路微基准）:\n",
        "  cargo run --release -- perf-test micro replay [trace_path]\n",
        "  cargo run --release -- perf-test micro live\n",
        "  cargo run --release -- perf-test micro <trace_path>\n",
        "\n",
        "环境变量:\n",
        "  PERF_TEST_DURATION_SEC=100\n",
        "  PERF_TEST_MAX_PAIRS=200\n",
        "  PERF_TEST_TRACE_PATH=perf_traces/book_ticker.jsonl\n",
        "  PERF_TEST_REPEAT=5\n",
        "  PERF_TEST_BENCH_CYCLES=50\n",
        "  PERF_TEST_SIM_CYCLE_LIMIT=50\n",
        "  PERF_TEST_LOOP_DURATION_SEC=30\n",
        "  PERF_TEST_LOOP_WARMUP_FRAMES=100000\n",
        "  PERF_TEST_REPLAY_PACE=0|1\n",
        "  PERF_TEST_REPLAY_LIMIT=100000 (不设置/为 0 表示不限制)\n",
    ));
}

async fn wait_any_connected(ws_conn_ok: &Arc<Vec<AtomicBool>>, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if ws_conn_ok.iter().any(|b| b.load(Ordering::Relaxed)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    warn!("perf-test: 等待连接超时 {:?}", timeout);
    false
}

async fn select_pairs_for_online(
    cfg: Arc<RwLock<Config>>,
    rest: &BinanceRestClient,
    max_pairs: Option<usize>,
) -> anyhow::Result<(HashMap<String, Market>, Vec<String>)> {
    info!("perf-test: 加载现货市场...");
    let markets = load_spot_markets(rest).await?;

    info!("perf-test: 获取并过滤 24h Ticker（用于选交易对）...");
    let mut websocket_symbols = filter_symbols_by_quote_volume(rest, &markets, cfg.clone()).await?;
    websocket_symbols.sort();
    if let Some(n) = max_pairs {
        websocket_symbols.truncate(n);
    }
    if websocket_symbols.is_empty() {
        anyhow::bail!("perf-test: 未选到任何交易对（可能是过滤阈值过高）");
    }

    Ok((markets, websocket_symbols))
}

fn build_pairs_meta(
    markets: &HashMap<String, Market>,
    pairs: &[String],
) -> anyhow::Result<Vec<PerfTracePair>> {
    let mut out = Vec::with_capacity(pairs.len());
    for p in pairs {
        let m = markets
            .get(p)
            .ok_or_else(|| anyhow::anyhow!("perf-test record: 缺少市场数据: {}", p))?;
        out.push(PerfTracePair {
            pair: p.clone(),
            binance_symbol: m.binance_symbol.clone(),
            base: m.base.clone(),
            quote: m.quote.clone(),
        });
    }
    Ok(out)
}

fn build_book_ticker_stream_chunks(
    markets: &HashMap<String, Market>,
    pairs: &[String],
    chunk_size: usize,
) -> anyhow::Result<Vec<Vec<String>>> {
    let mut streams = Vec::with_capacity(pairs.len());
    for p in pairs {
        let m = markets
            .get(p)
            .ok_or_else(|| anyhow::anyhow!("perf-test record: 缺少市场数据: {}", p))?;
        streams.push(format!("{}@bookTicker", m.binance_symbol.to_lowercase()));
    }

    let mut out = Vec::new();
    let size = chunk_size.max(1);
    for c in streams.chunks(size) {
        out.push(c.to_vec());
    }
    Ok(out)
}

async fn record_ws_chunk(
    chunk: u32,
    streams: Vec<String>,
    tx: tokio::sync::mpsc::Sender<PerfTraceLine>,
    start: Instant,
    stop: Arc<AtomicBool>,
) {
    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    );

    let mut backoff = Duration::from_millis(200);
    let backoff_max = Duration::from_secs(3);

    while !stop.load(Ordering::Relaxed) {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws, _)) => {
                let (_, mut reader) = ws.split();
                while let Some(msg) = reader.next().await {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let line = match msg {
                        Ok(Message::Text(text)) => Some(PerfTraceLine::Frame {
                            t_us: start.elapsed().as_micros() as u64,
                            chunk,
                            encoding: PerfTraceEncoding::Utf8,
                            payload: text,
                        }),
                        Ok(Message::Binary(bin)) => match String::from_utf8(bin) {
                            Ok(text) => Some(PerfTraceLine::Frame {
                                t_us: start.elapsed().as_micros() as u64,
                                chunk,
                                encoding: PerfTraceEncoding::Utf8,
                                payload: text,
                            }),
                            Err(e) => Some(PerfTraceLine::Frame {
                                t_us: start.elapsed().as_micros() as u64,
                                chunk,
                                encoding: PerfTraceEncoding::Hex,
                                payload: hex::encode(e.into_bytes()),
                            }),
                        },
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => None,
                    };

                    let Some(line) = line else {
                        continue;
                    };
                    if tx.send(line).await.is_err() {
                        return;
                    }
                }
            }
            Err(_) => {}
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}

struct TraceWriteStats {
    frames: u64,
    bytes: u64,
    last_t_us: u64,
    ws_chunks_total: usize,
}

async fn write_trace_file(
    path: &Path,
    meta: PerfTraceLine,
    mut rx: tokio::sync::mpsc::Receiver<PerfTraceLine>,
) -> anyhow::Result<TraceWriteStats> {
    let file = tokio::fs::File::create(path)
        .await
        .with_context(|| format!("perf-test record: 创建 trace 失败: {}", path.display()))?;
    let mut w = tokio::io::BufWriter::new(file);

    let meta_json = serde_json::to_string(&meta)?;
    w.write_all(meta_json.as_bytes()).await?;
    w.write_all(b"\n").await?;

    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut last_t_us = 0u64;
    while let Some(line) = rx.recv().await {
        if let PerfTraceLine::Frame {
            t_us,
            encoding,
            payload,
            ..
        } = &line
        {
            frames += 1;
            last_t_us = last_t_us.max(*t_us);
            bytes += match encoding {
                PerfTraceEncoding::Utf8 => payload.len() as u64,
                PerfTraceEncoding::Hex => (payload.len() / 2) as u64,
            };
        }

        let json = serde_json::to_string(&line)?;
        w.write_all(json.as_bytes()).await?;
        w.write_all(b"\n").await?;
    }
    w.flush().await?;

    let ws_chunks_total = match meta {
        PerfTraceLine::Meta {
            ws_chunk_size,
            pairs,
            ..
        } => {
            let size = ws_chunk_size.max(1);
            (pairs.len() + size - 1) / size
        }
        _ => 0,
    };

    Ok(TraceWriteStats {
        frames,
        bytes,
        last_t_us,
        ws_chunks_total,
    })
}

fn parse_trace_meta(line: &str) -> anyhow::Result<TraceMeta> {
    let PerfTraceLine::Meta {
        version,
        kind,
        recorded_at_epoch_ms,
        taker_fee_rate,
        ws_chunk_size,
        pairs,
    } = serde_json::from_str::<PerfTraceLine>(line.trim_end())
        .context("perf-test replay: 解析 meta 失败")?
    else {
        anyhow::bail!("perf-test replay: trace 第一行不是 meta");
    };

    Ok(TraceMeta {
        version,
        kind,
        recorded_at_epoch_ms,
        taker_fee_rate,
        ws_chunk_size,
        pairs,
    })
}

fn validate_trace_meta(meta: &TraceMeta) -> anyhow::Result<()> {
    if meta.version != TRACE_VERSION {
        anyhow::bail!("perf-test replay: 不支持的 trace version: {}", meta.version);
    }
    if meta.kind != TRACE_KIND_BOOK_TICKER {
        anyhow::bail!("perf-test replay: 不支持的 trace kind: {}", meta.kind);
    }
    if meta.pairs.is_empty() {
        anyhow::bail!("perf-test replay: meta.pairs 为空");
    }
    Ok(())
}

fn build_markets_from_meta(meta: &TraceMeta) -> HashMap<String, Market> {
    let mut out = HashMap::with_capacity(meta.pairs.len());
    for p in &meta.pairs {
        out.insert(
            p.pair.clone(),
            Market {
                symbol: p.pair.clone(),
                binance_symbol: p.binance_symbol.clone(),
                base: p.base.clone(),
                quote: p.quote.clone(),
                min_qty: Decimal::ZERO,
                min_notional: Decimal::ZERO,
                active: true,
                spot: true,
            },
        );
    }
    out
}

fn build_binance_to_id_map(meta: &TraceMeta) -> HashMap<String, usize> {
    let mut out = HashMap::with_capacity(meta.pairs.len());
    for (i, p) in meta.pairs.iter().enumerate() {
        out.insert(p.binance_symbol.clone(), i);
    }
    out
}

#[derive(Debug, Clone)]
struct TraceMeta {
    version: u32,
    kind: String,
    recorded_at_epoch_ms: u64,
    taker_fee_rate: Decimal,
    ws_chunk_size: usize,
    pairs: Vec<PerfTracePair>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}
