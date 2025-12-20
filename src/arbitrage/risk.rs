use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Semaphore;

use crate::binance::rest::BinanceRestClient;
use crate::config::Config;
use crate::domain::{CycleInfo, Market, OrderBook};
use crate::store::TickerStore;

#[derive(Debug, Clone)]
pub struct StepDetail {
    pub step: usize,
    pub pair: String,
    pub kind: String,
    pub slippage_percent: f64,
    pub spread_percent: f64,
    pub depth_ok: bool,
    pub depth_usd: f64,
    pub limits_ok: bool,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct RiskResult {
    pub is_viable: bool,
    pub estimated_profit_percent_after_slippage: f64,
    pub total_estimated_slippage_percent: f64,
    pub reasons: Vec<String>,
    pub details: Vec<StepDetail>,
}

pub async fn assess_risk(
    cycle: &CycleInfo,
    start_amount: Decimal,
    rest: &BinanceRestClient,
    markets: &HashMap<String, Market>,
    tickers: &TickerStore,
    cfg: &Config,
) -> anyhow::Result<RiskResult> {
    if !cfg.risk_assessment_enabled {
        return Ok(RiskResult {
            is_viable: true,
            reasons: vec!["风险评估未启用".to_string()],
            estimated_profit_percent_after_slippage: -1.0,
            total_estimated_slippage_percent: 0.0,
            details: Vec::new(),
        });
    }

    let taker_fee_rate = cfg
        .taker_fee_rate
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("taker_fee_rate 无法转换为 f64"))?;
    let min_profit_req = cfg
        .min_profit_after_slippage_percent
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("min_profit_after_slippage_percent 无法转换为 f64"))?;
    let max_slip_req = cfg
        .max_allowed_slippage_percent_total
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("max_allowed_slippage_percent_total 无法转换为 f64"))?;
    let min_depth_usd_req = cfg
        .min_depth_required_usd
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("min_depth_required_usd 无法转换为 f64"))?;
    let max_spread_req = cfg
        .max_bid_ask_spread_percent_per_step
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("max_bid_ask_spread_percent_per_step 无法转换为 f64"))?;

    let stablecoin_prefs: [&str; 2] = ["USDT", "USDC"];

    if cycle.nodes.is_empty() {
        return Ok(RiskResult {
            is_viable: false,
            reasons: vec!["cycle 缺少 nodes".to_string()],
            estimated_profit_percent_after_slippage: -999.0,
            total_estimated_slippage_percent: -999.0,
            details: Vec::new(),
        });
    }

    let path_start_currency = cycle.nodes[0].clone();
    let mut current_currency = path_start_currency.clone();
    let mut intermediate_amount = start_amount
        .to_f64()
        .ok_or_else(|| anyhow::anyhow!("start_amount 无法转换为 f64"))?;

    let start_value_usd_est = estimate_value_usd(&current_currency, intermediate_amount, tickers, &stablecoin_prefs)
        .unwrap_or_else(|| {
            if current_currency == "USDT" {
                intermediate_amount
            } else {
                0.0
            }
        });

    let order_books = fetch_order_books_for_cycle(rest, cycle, markets, cfg).await?;

    let mut reasons: Vec<String> = Vec::new();
    let mut details: Vec<StepDetail> = Vec::new();
    let mut total_slippage_cost_usd = 0.0f64;

    for (i, trade) in cycle.trades.iter().enumerate() {
        let step_num = i + 1;
        let mut step = StepDetail {
            step: step_num,
            pair: trade.pair.clone(),
            kind: trade.kind.clone(),
            slippage_percent: f64::NAN,
            spread_percent: f64::NAN,
            depth_ok: false,
            depth_usd: 0.0,
            limits_ok: true,
            message: String::new(),
        };

        if intermediate_amount <= 0.0 {
            let msg = format!("步骤 {}: 上一步金额无效 ({})", step_num, format_f64(intermediate_amount, 8));
            reasons.push(msg.clone());
            step.message = msg;
            details.push(step);
            intermediate_amount = -1.0;
            break;
        }
        if current_currency != trade.from {
            let msg = format!("逻辑错误：步骤 {} 需发 {}, 持有 {}", step_num, trade.from, current_currency);
            reasons.push(msg.clone());
            step.message = msg;
            details.push(step);
            intermediate_amount = -1.0;
            break;
        }

        let Some(market) = markets.get(&trade.pair) else {
            let msg = format!("步骤 {}: 市场数据无效 {}", step_num, trade.pair);
            reasons.push(msg.clone());
            step.message = msg;
            details.push(step);
            intermediate_amount = -1.0;
            continue;
        };

        let Some(ob) = order_books.get(&trade.pair) else {
            let msg = format!("步骤 {}: 未能获取订单簿 {}", step_num, trade.pair);
            reasons.push(msg.clone());
            step.message = msg;
            step.limits_ok = false;
            details.push(step);
            intermediate_amount = -1.0;
            continue;
        };

        if ob.bids.is_empty() || ob.asks.is_empty() {
            let msg = format!("步骤 {}: 订单簿为空 {}", step_num, trade.pair);
            reasons.push(msg.clone());
            step.message = msg;
            step.limits_ok = false;
            details.push(step);
            intermediate_amount = -1.0;
            continue;
        }

        let top_bid = ob.bids[0].0;
        let top_ask = ob.asks[0].0;
        let spread_percent = if top_bid > 0.0 && top_ask.is_finite() && top_ask > 0.0 {
            ((top_ask - top_bid) / top_ask) * 100.0
        } else {
            f64::INFINITY
        };
        step.spread_percent = spread_percent;
        if !spread_percent.is_finite() {
            let msg = "无法计算价差".to_string();
            reasons.push(format!("步骤 {}: {}", step_num, msg));
            step.message.push_str(&format!("{}; ", msg));
        } else if spread_percent > max_spread_req {
            let msg = format!("价差过高 ({}%)", format_f64(spread_percent, 3));
            reasons.push(format!("步骤 {}: {}", step_num, msg));
            step.message.push_str(&format!("{}; ", msg));
        }

        let approx_trade_value_usd =
            estimate_value_usd(&current_currency, intermediate_amount, tickers, &stablecoin_prefs).unwrap_or(0.0);

        let mut estimated_executed_amount = 0.0f64;
        let mut slippage_percent_step = f64::NAN;
        let mut depth_usd_available = 0.0f64;

        let min_amount = market.min_qty.to_f64().unwrap_or(0.0);
        let min_cost = market.min_notional.to_f64().unwrap_or(0.0);

        if trade.kind == "BUY" {
            let amount_to_spend = intermediate_amount;
            if min_cost > 0.0 && amount_to_spend < min_cost {
                let msg = format!(
                    "花费 {} 低于最小成本 {}",
                    format_f64(amount_to_spend, 8),
                    format_f64(min_cost, 8)
                );
                reasons.push(format!("步骤 {}: {}", step_num, msg));
                step.limits_ok = false;
                step.message.push_str(&format!("{}; ", msg));
            }

            let mut accumulated_base = 0.0f64;
            let mut cost_accumulated = 0.0f64;

            for &(price, amount) in &ob.asks {
                if price <= 0.0 || amount <= 0.0 {
                    continue;
                }
                let cost_at_level = price * amount;
                depth_usd_available += estimate_value_usd(&market.quote, cost_at_level, tickers, &stablecoin_prefs).unwrap_or(0.0);

                let remaining_spend = amount_to_spend - cost_accumulated;
                if remaining_spend <= 1e-9 {
                    break;
                }
                let buy_base = (remaining_spend / price).min(amount);
                let cost_this = buy_base * price;
                accumulated_base += buy_base;
                cost_accumulated += cost_this;
            }

            estimated_executed_amount = accumulated_base;
            if accumulated_base > 1e-12 {
                let sim_average_price = cost_accumulated / accumulated_base;
                if top_ask.is_finite() && top_ask > 0.0 {
                    slippage_percent_step = ((sim_average_price - top_ask) / top_ask) * 100.0;
                }
                if min_amount > 0.0 && estimated_executed_amount < min_amount {
                    let msg = format!(
                        "买入量 {} 低于最小 {}",
                        format_f64(estimated_executed_amount, 8),
                        format_f64(min_amount, 8)
                    );
                    reasons.push(format!("步骤 {}: {}", step_num, msg));
                    step.limits_ok = false;
                    step.message.push_str(&format!("{}; ", msg));
                }
            } else {
                slippage_percent_step = f64::INFINITY;
                let msg = "无法模拟买入".to_string();
                reasons.push(format!("步骤 {}: {}", step_num, msg));
                step.limits_ok = false;
                step.message.push_str(&format!("{}; ", msg));
                intermediate_amount = -1.0;
            }
        } else if trade.kind == "SELL" {
            let amount_to_sell = intermediate_amount;
            if min_amount > 0.0 && amount_to_sell < min_amount {
                let msg = format!(
                    "卖出量 {} 低于最小 {}",
                    format_f64(amount_to_sell, 8),
                    format_f64(min_amount, 8)
                );
                reasons.push(format!("步骤 {}: {}", step_num, msg));
                step.limits_ok = false;
                step.message.push_str(&format!("{}; ", msg));
            }

            let mut accumulated_quote = 0.0f64;
            let mut amount_sold = 0.0f64;

            for &(price, amount) in &ob.bids {
                if price <= 0.0 || amount <= 0.0 {
                    continue;
                }

                // 这里复刻 C++：以 base->USD 估值深度
                    let level_usd = if market.base == "USDT" {
                        amount * price
                    } else {
                        estimate_value_usd(&market.base, amount, tickers, &stablecoin_prefs).unwrap_or(0.0)
                    };
                depth_usd_available += level_usd;

                let remaining_sell = amount_to_sell - amount_sold;
                if remaining_sell <= 1e-12 {
                    break;
                }
                let sell_base = remaining_sell.min(amount);
                let quote_received = sell_base * price;
                accumulated_quote += quote_received;
                amount_sold += sell_base;
            }

            estimated_executed_amount = accumulated_quote;
            if amount_sold > 1e-12 {
                let sim_average_price = accumulated_quote / amount_sold;
                if top_bid > 0.0 {
                    slippage_percent_step = ((top_bid - sim_average_price) / top_bid) * 100.0;
                }
                if min_cost > 0.0 && estimated_executed_amount < min_cost {
                    let msg = format!(
                        "卖出所得 {} 低于最小成本 {}",
                        format_f64(estimated_executed_amount, 8),
                        format_f64(min_cost, 8)
                    );
                    step.message.push_str(&format!("{}; ", msg));
                }
            } else {
                slippage_percent_step = f64::INFINITY;
                let msg = "无法模拟卖出".to_string();
                reasons.push(format!("步骤 {}: {}", step_num, msg));
                step.limits_ok = false;
                step.message.push_str(&format!("{}; ", msg));
                intermediate_amount = -1.0;
            }
        } else {
            let msg = format!("未知交易类型: {}", trade.kind);
            reasons.push(format!("步骤 {}: {}", step_num, msg));
            step.limits_ok = false;
            step.message.push_str(&format!("{}; ", msg));
            intermediate_amount = -1.0;
        }

        step.depth_usd = depth_usd_available;
        if depth_usd_available >= min_depth_usd_req {
            step.depth_ok = true;
        } else {
            let msg = format!("深度不足 (仅约 ${})", format_f64(depth_usd_available, 2));
            reasons.push(format!("步骤 {}: {}", step_num, msg));
            step.depth_ok = false;
            step.message.push_str(&format!("{}; ", msg));
        }

        if intermediate_amount >= 0.0 {
            step.slippage_percent = slippage_percent_step;
            let mut slippage_cost_step_usd = 0.0;
            if slippage_percent_step.is_finite() && approx_trade_value_usd > 0.0 && slippage_percent_step > 0.0 {
                slippage_cost_step_usd = approx_trade_value_usd * (slippage_percent_step / 100.0);
            }
            total_slippage_cost_usd += slippage_cost_step_usd;

            let next_intermediate_amount = estimated_executed_amount * (1.0 - taker_fee_rate);
            current_currency = trade.to.clone();
            intermediate_amount = next_intermediate_amount;
            if intermediate_amount < 0.0 {
                intermediate_amount = 0.0;
            }
        }

        details.push(step);
        if intermediate_amount < 0.0 {
            reasons.push(format!("因步骤 {} 错误中止评估", step_num));
            break;
        }
    }

    let mut estimated_profit_percent_after_slippage = -999.0;
    let mut total_estimated_slippage_percent = -999.0;
    if intermediate_amount >= 0.0 && current_currency == path_start_currency {
        let profit_amount = intermediate_amount
            - start_amount
                .to_f64()
                .ok_or_else(|| anyhow::anyhow!("start_amount 无法转换为 f64"))?;
        let start_f = start_amount.to_f64().unwrap_or(0.0);
        estimated_profit_percent_after_slippage = if start_f > 1e-12 {
            (profit_amount / start_f) * 100.0
        } else {
            0.0
        };

        if start_value_usd_est > 1e-9 {
            total_estimated_slippage_percent = (total_slippage_cost_usd / start_value_usd_est) * 100.0;
        } else if total_slippage_cost_usd == 0.0 {
            total_estimated_slippage_percent = 0.0;
        } else {
            total_estimated_slippage_percent = f64::INFINITY;
        }

        if estimated_profit_percent_after_slippage < min_profit_req {
            reasons.push(format!(
                "利润率 ({}%) 低于要求",
                format_f64(estimated_profit_percent_after_slippage, 4)
            ));
        }
        if total_estimated_slippage_percent.is_finite() && total_estimated_slippage_percent > max_slip_req {
            reasons.push(format!(
                "总滑点 ({}%) 高于阈值",
                format_f64(total_estimated_slippage_percent, 4)
            ));
        } else if total_estimated_slippage_percent.is_infinite() {
            reasons.push("无法计算总滑点%".to_string());
        }
    } else if intermediate_amount < 0.0 {
        estimated_profit_percent_after_slippage = -998.0;
    } else if current_currency != path_start_currency {
        reasons.push(format!("最终货币({})与起始不符", current_currency));
        estimated_profit_percent_after_slippage = -997.0;
    }

    let mut is_viable = true;
    if intermediate_amount < 0.0 {
        is_viable = false;
    }
    if !estimated_profit_percent_after_slippage.is_finite() || estimated_profit_percent_after_slippage < min_profit_req {
        is_viable = false;
    }
    if total_estimated_slippage_percent.is_finite() && total_estimated_slippage_percent > max_slip_req {
        is_viable = false;
    }
    if intermediate_amount >= 0.0 && current_currency != path_start_currency {
        is_viable = false;
    }
    for d in &details {
        if !d.depth_ok || !d.limits_ok {
            is_viable = false;
        }
    }

    Ok(RiskResult {
        is_viable,
        estimated_profit_percent_after_slippage,
        total_estimated_slippage_percent,
        reasons,
        details,
    })
}

async fn fetch_order_books_for_cycle(
    rest: &BinanceRestClient,
    cycle: &CycleInfo,
    markets: &HashMap<String, Market>,
    cfg: &Config,
) -> anyhow::Result<HashMap<String, OrderBook>> {
    let mut pairs: HashSet<String> = HashSet::new();
    for t in &cycle.trades {
        pairs.insert(t.pair.clone());
    }
    if pairs.is_empty() {
        return Ok(HashMap::new());
    }

    let limit = cfg.order_book_depth;
    let sem = Arc::new(Semaphore::new(cfg.orderbook_fetch_max_workers.max(1)));

    let mut tasks = Vec::new();
    for pair in pairs {
        let Some(m) = markets.get(&pair) else { continue };
        let binance_symbol = m.binance_symbol.clone();
        let sem = sem.clone();
        let rest = rest.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            let depth = rest.depth(&binance_symbol, limit).await.ok()?;
            let bids = depth
                .bids
                .into_iter()
                .filter_map(|lv| parse_level(&lv))
                .collect::<Vec<_>>();
            let asks = depth
                .asks
                .into_iter()
                .filter_map(|lv| parse_level(&lv))
                .collect::<Vec<_>>();
            Some((pair, OrderBook { bids, asks }))
        }));
    }

    let mut out = HashMap::new();
    for t in tasks {
        if let Ok(Some((pair, ob))) = t.await {
            out.insert(pair, ob);
        }
    }
    Ok(out)
}

fn parse_level(lv: &[String; 2]) -> Option<(f64, f64)> {
    let p = lv[0].parse::<f64>().ok()?;
    let a = lv[1].parse::<f64>().ok()?;
    if p.is_finite() && a.is_finite() && p > 0.0 && a > 0.0 {
        Some((p, a))
    } else {
        None
    }
}

fn estimate_value_usd(currency: &str, amount: f64, tickers: &TickerStore, stable_prefs: &[&str]) -> Option<f64> {
    if amount <= 0.0 || !amount.is_finite() {
        return None;
    }
    if currency == "USDT" {
        return Some(amount);
    }

    let fwd = format!("{}/USDT", currency);
    if let Some(t) = tickers.get_by_pair(&fwd) {
        if t.bid > 0.0 {
            return Some(amount * t.bid);
        }
    }

    let rev = format!("USDT/{}", currency);
    if let Some(t) = tickers.get_by_pair(&rev) {
        if t.ask > 0.0 {
            return Some(amount * (1.0 / t.ask));
        }
    }

    if stable_prefs.iter().any(|s| *s == currency) {
        return Some(amount);
    }

    None
}

fn format_f64(v: f64, precision: usize) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() { "Infinity" } else { "-Infinity" }.to_string();
    }
    format!("{:.*}", precision, v)
}
