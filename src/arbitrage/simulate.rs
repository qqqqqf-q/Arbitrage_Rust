use std::collections::HashMap;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::config::Config;
use crate::domain::{CycleInfo, Market};
use crate::store::TickerStore;

#[derive(Debug, Clone)]
pub struct FullSimResult {
    pub verified: bool,
    pub profit_percent: f64,
    pub profit_amount: f64,
    pub final_amount: f64,
    pub final_currency: String,
    pub reason: String,
}

pub fn simulate_full(
    cycle: &CycleInfo,
    actual_start_currency: &str,
    actual_start_amount: Decimal,
    end_with_usdt: bool,
    tickers: &TickerStore,
    markets: &HashMap<String, Market>,
    cfg: &Config,
) -> anyhow::Result<FullSimResult> {
    let fee_rate = cfg
        .taker_fee_rate
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("taker_fee_rate 无法转换为 f64"))?;
    let min_profit_req = cfg
        .min_profit_full_sim_percent
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("min_profit_full_sim_percent 无法转换为 f64"))?;

    if cycle.nodes.is_empty() {
        return Ok(FullSimResult {
            verified: false,
            profit_percent: -999.0,
            profit_amount: 0.0,
            final_amount: 0.0,
            final_currency: actual_start_currency.to_string(),
            reason: "cycle 缺少 nodes".to_string(),
        });
    }

    let cycle_start_currency = &cycle.nodes[0];
    let mut sim_current_currency = actual_start_currency.to_string();
    let mut sim_current_amount = actual_start_amount
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("actual_start_amount 无法转换为 f64"))?;

    let mut start_trade_index = 0usize;
    let mut skip_initial_swap_and_first_step = false;
    if !cycle.trades.is_empty() && cycle.trades.len() > 1 {
        let first = &cycle.trades[0];
        if actual_start_currency != cycle_start_currency
            && first.from == *cycle_start_currency
            && first.to == actual_start_currency
        {
            skip_initial_swap_and_first_step = true;
            start_trade_index = 1;
        }
    }

    if !skip_initial_swap_and_first_step && sim_current_currency != *cycle_start_currency {
        let swap = simulate_swap(
            &sim_current_currency,
            cycle_start_currency,
            sim_current_amount,
            tickers,
            fee_rate,
        );
        if swap.estimated_to_amount > 1e-12 {
            sim_current_amount = swap.estimated_to_amount;
            sim_current_currency = cycle_start_currency.clone();
        } else {
            return Ok(FullSimResult {
                verified: false,
                profit_percent: -999.0,
                profit_amount: 0.0,
                final_amount: 0.0,
                final_currency: sim_current_currency,
                reason: format!(
                    "模拟初始闪兑 ({} -> {}) 失败或无路径",
                    actual_start_currency, cycle_start_currency
                ),
            });
        }
    }

    for trade in cycle.trades.iter().skip(start_trade_index) {
        let pair = &trade.pair;
        let Some(market) = markets.get(pair) else {
            return Err(anyhow::anyhow!("模拟时缺少核心市场数据 for {}", pair));
        };
        let Some(ticker) = tickers.get_by_pair(pair) else {
            return Err(anyhow::anyhow!("模拟时缺少 Ticker 数据 for {}", pair));
        };

        let net = match trade.kind.as_str() {
            "BUY" => {
                let price = ticker.ask;
                if !(price.is_finite() && price > 0.0) {
                    return Err(anyhow::anyhow!("Ticker {} ask 价无效", pair));
                }
                sim_current_currency = market.base.clone();
                (sim_current_amount / price) * (1.0 - fee_rate)
            }
            "SELL" => {
                let price = ticker.bid;
                if !(price.is_finite() && price > 0.0) {
                    return Err(anyhow::anyhow!("Ticker {} bid 价无效", pair));
                }
                sim_current_currency = market.quote.clone();
                (sim_current_amount * price) * (1.0 - fee_rate)
            }
            other => return Err(anyhow::anyhow!("未知交易类型: {}", other)),
        };

        sim_current_amount = net;
        if sim_current_amount < -1e-9 {
            return Err(anyhow::anyhow!("模拟中金额变为负数"));
        }
        if sim_current_amount < 1e-12 {
            sim_current_amount = 0.0;
            break;
        }
    }

    if end_with_usdt && sim_current_currency != "USDT" {
        let swap = simulate_swap(
            &sim_current_currency,
            "USDT",
            sim_current_amount,
            tickers,
            fee_rate,
        );
        if swap.estimated_to_amount > 1e-12 {
            sim_current_amount = swap.estimated_to_amount;
            sim_current_currency = "USDT".to_string();
        }
    }

    let profit_target_currency = if end_with_usdt {
        "USDT"
    } else {
        actual_start_currency
    };
    let profit_amount: f64;
    let profit_percent: f64;
    let final_amount = sim_current_amount;
    let final_currency = sim_current_currency.clone();

    let mut verified = false;
    let mut reason = "未达标".to_string();

    if final_currency == profit_target_currency {
        if actual_start_currency == profit_target_currency {
            profit_amount = final_amount
                - actual_start_amount
                    .to_f64()
                    .ok_or_else(|| anyhow::anyhow!("actual_start_amount 无法转换为 f64"))?;
            if actual_start_amount > Decimal::ZERO {
                let start = actual_start_amount.to_f64().unwrap_or(0.0);
                profit_percent = if start > 1e-12 {
                    (profit_amount / start) * 100.0
                } else {
                    0.0
                };
            } else {
                profit_percent = 0.0;
            }
        } else {
            profit_amount = final_amount;
            profit_percent = -998.0;
            reason = format!(
                "起始({}), 目标({}), 无法计算利润率",
                actual_start_currency, profit_target_currency
            );
        }

        if profit_percent.is_finite() && profit_percent != -998.0 {
            if profit_percent > min_profit_req {
                verified = true;
                reason = "模拟利润率达标".to_string();
            } else {
                reason = "模拟利润率未达标".to_string();
            }
        }
    } else {
        reason = "最终货币与目标不符".to_string();
        profit_percent = -997.0;
        profit_amount = 0.0;
    }

    Ok(FullSimResult {
        verified,
        profit_percent,
        profit_amount,
        final_amount,
        final_currency,
        reason,
    })
}

#[derive(Debug, Clone)]
struct SwapResult {
    estimated_to_amount: f64,
    method: Option<&'static str>,
}

fn simulate_swap(
    from_currency: &str,
    to_currency: &str,
    from_amount: f64,
    tickers: &TickerStore,
    fee_rate: f64,
) -> SwapResult {
    let mut out = SwapResult {
        estimated_to_amount: 0.0,
        method: None,
    };

    let direct_forward = format!("{}/{}", to_currency, from_currency);
    let direct_backward = format!("{}/{}", from_currency, to_currency);
    let intermediate_currency = "USDT";

    // 1) 直接卖出 from -> to (symbol: from/to, 用 bid)
    if let Some(tkr) = tickers.get_by_pair(&direct_backward) {
        if tkr.bid > 0.0 {
            out.estimated_to_amount = (from_amount * tkr.bid) * (1.0 - fee_rate);
            out.method = Some("sell");
        }
    }

    // 2) 直接买入 to (symbol: to/from, 用 ask)
    if out.estimated_to_amount <= 0.0 {
        if let Some(tkr) = tickers.get_by_pair(&direct_forward) {
            if tkr.ask > 0.0 {
                out.estimated_to_amount = (from_amount / tkr.ask) * (1.0 - fee_rate);
                out.method = Some("buy_cost");
            }
        }
    }

    // 3) 通过 USDT 中转
    if out.estimated_to_amount <= 0.0 {
        let sym_from_int = format!("{}/{}", from_currency, intermediate_currency);
        let sym_to_int = format!("{}/{}", to_currency, intermediate_currency);

        if let (Some(from_t), Some(to_t)) = (
            tickers.get_by_pair(&sym_from_int),
            tickers.get_by_pair(&sym_to_int),
        ) {
            if from_t.bid > 0.0 && to_t.ask > 0.0 {
                let intermediate_amount_net = (from_amount * from_t.bid) * (1.0 - fee_rate);
                if intermediate_amount_net > 1e-12 {
                    out.estimated_to_amount =
                        (intermediate_amount_net / to_t.ask) * (1.0 - fee_rate);
                    out.method = Some("intermediate");
                }
            }
        }
    }

    if out.estimated_to_amount < 0.0 || !out.estimated_to_amount.is_finite() {
        out.estimated_to_amount = 0.0;
        out.method = None;
    }
    out
}
