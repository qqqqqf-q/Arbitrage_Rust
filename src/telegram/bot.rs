use rust_decimal::Decimal;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use teloxide::dispatching::UpdateFilterExt;
use teloxide::payloads::SendMessageSetters;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, ParseMode};
use teloxide::types::{MaybeInaccessibleMessage, User};
use tracing::{info, warn};

use crate::app::AppContext;

fn format_duration_high_precision(sec: f64) -> String {
    let sec = if sec.is_finite() && sec > 0.0 {
        sec
    } else {
        0.0
    };
    let ns = (sec * 1_000_000_000.0).round();
    if !ns.is_finite() || ns <= 0.0 {
        return "0 ns".to_string();
    }

    let ns_u128 = ns as u128;
    if ns_u128 >= 1_000_000_000 {
        format!("{:.3} s", sec)
    } else if ns_u128 >= 1_000_000 {
        format!("{:.3} ms", (ns_u128 as f64) / 1_000_000.0)
    } else if ns_u128 >= 1_000 {
        format!("{:.3} µs", (ns_u128 as f64) / 1_000.0)
    } else {
        format!("{} ns", ns_u128)
    }
}

fn disable_link_preview() -> LinkPreviewOptions {
    LinkPreviewOptions {
        is_disabled: true,
        url: None,
        prefer_small_media: false,
        prefer_large_media: false,
        show_above_text: false,
    }
}

#[derive(Clone)]
pub struct TelegramController {
    bot: Bot,
    authorized_user_id: i64,
    ctx: Arc<AppContext>,
}

impl TelegramController {
    pub fn new(bot: Bot, authorized_user_id: i64, ctx: Arc<AppContext>) -> anyhow::Result<Self> {
        Ok(Self {
            bot,
            authorized_user_id,
            ctx,
        })
    }

    pub async fn run(self) {
        let me = self.bot.get_me().await;
        match me {
            Ok(m) => info!(
                "Telegram bot 已启动: @{}",
                m.user.username.unwrap_or_default()
            ),
            Err(e) => warn!("Telegram bot 初始化失败: {}", e),
        }

        let controller = self.clone();
        let handler = Update::filter_message().endpoint(move |bot: Bot, msg: Message| {
            let controller = controller.clone();
            async move { controller.handle_message(bot, msg).await }
        });

        let controller = self.clone();
        let cb_handler =
            Update::filter_callback_query().endpoint(move |bot: Bot, q: CallbackQuery| {
                let controller = controller.clone();
                async move { controller.handle_callback(bot, q).await }
            });

        let root = dptree::entry().branch(handler).branch(cb_handler);

        Dispatcher::builder(self.bot.clone(), root)
            .enable_ctrlc_handler()
            .build()
            .dispatch()
            .await;
    }

    async fn handle_message(&self, bot: Bot, msg: Message) -> ResponseResult<()> {
        let Some(text) = msg.text() else {
            return Ok(());
        };
        if !self.is_authorized(msg.from.as_ref()) {
            bot.send_message(msg.chat.id, "抱歉，您无权使用此机器人。")
                .await?;
            return Ok(());
        }

        if text.starts_with("/start") {
            self.on_start(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/help") {
            self.on_help(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/status") {
            self.on_status(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/pause") {
            self.on_pause(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/resume") {
            self.on_resume(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/balance") {
            self.on_balance(&bot, &msg).await?;
            return Ok(());
        }
        if text.starts_with("/trade") {
            self.on_trade(&bot, &msg, text).await?;
            return Ok(());
        }
        if text.starts_with("/set") {
            self.on_set(&bot, &msg, text).await?;
            return Ok(());
        }
        Ok(())
    }

    async fn handle_callback(&self, bot: Bot, q: CallbackQuery) -> ResponseResult<()> {
        let user = &q.from;
        if self.authorized_user_id != 0 && user.id.0 as i64 != self.authorized_user_id {
            return Ok(());
        }
        bot.answer_callback_query(q.id.clone()).await?;

        let Some(data) = q.data.clone() else {
            return Ok(());
        };
        if data == "confirm_trade_on" {
            {
                let mut cfg = self.ctx.cfg.write().await;
                cfg.auto_trade_enabled = true;
            }
            info!("用户已确认，自动交易已启用。");
            if let Some(MaybeInaccessibleMessage::Regular(msg)) = q.message {
                bot.edit_message_text(
                    msg.chat.id,
                    msg.id,
                    "自动交易已确认启用。\n<b>请密切监控!</b>",
                )
                .parse_mode(ParseMode::Html)
                .await?;
            }
        }
        Ok(())
    }

    fn is_authorized(&self, from: Option<&User>) -> bool {
        if self.authorized_user_id == 0 {
            return true;
        }
        from.map(|u| u.id.0 as i64 == self.authorized_user_id)
            .unwrap_or(false)
    }

    async fn on_start(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        *self.ctx.user_chat_id.lock().await = Some(msg.chat.id);
        {
            let mut cfg = self.ctx.cfg.write().await;
            cfg.running = true;
        }

        let auto_trade = { self.ctx.cfg.read().await.auto_trade_enabled };
        let welcome = format!(
            "欢迎, {}!\n\n套利机器人已启动并运行。\n自动交易: {}\n\n使用 /status 查看详细状态，/help 获取命令列表。",
            msg.from
                .as_ref()
                .map(|u| u.full_name())
                .unwrap_or_else(|| "用户".to_string()),
            if auto_trade { "已启用" } else { "已禁用" }
        );
        bot.send_message(msg.chat.id, welcome)
            .parse_mode(ParseMode::Html)
            .link_preview_options(disable_link_preview())
            .await?;
        Ok(())
    }

    async fn on_help(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        let help_text = concat!(
            "<b>套利机器人帮助</b>\n\n",
            "<b>基础命令:</b>\n",
            "  <code>/start</code> - 初始化机器人。\n",
            "  <code>/status</code> - 查看详细运行状态和配置。\n",
            "  <code>/balance</code> - 显示当前账户余额。\n",
            "  <code>/help</code> - 显示此帮助信息。\n\n",
            "<b>控制命令:</b>\n",
            "  <code>/trade [on|off]</code> - <b>[!!]</b> 启用或禁用自动真实交易。\n",
            "  <code>/pause</code> - 暂停套利计算。\n",
            "  <code>/resume</code> - 恢复套利计算。\n\n",
            "<b>配置命令:</b> <code>/set [参数] [值]</code>\n",
            "  - <code>fee_rate [小数]</code> (例如 0.00075)\n",
            "  - <code>min_profit [百分比]</code> (例如 0.05)\n",
            "  - <code>depth [整数]</code> (例如 5)\n",
        );

        bot.send_message(msg.chat.id, help_text)
            .parse_mode(ParseMode::Html)
            .link_preview_options(disable_link_preview())
            .await?;
        Ok(())
    }

    async fn on_status(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        let now_ms = crate::app::now_ms();
        let tickers_len = self.ctx.tickers.valid_count();
        let last_update_ms = self.ctx.tickers.last_update_ms();
        let last_update_ago = if last_update_ms == 0 {
            f64::INFINITY
        } else {
            (now_ms.saturating_sub(last_update_ms) as f64) / 1000.0
        };

        let ws_total = self.ctx.ws_conn_ok.len();
        let ws_ok = self
            .ctx
            .ws_conn_ok
            .iter()
            .filter(|b| b.load(Ordering::Relaxed))
            .count();

        let perf_guard = self.ctx.perf.lock().await;
        let perf = perf_guard.snapshot();
        let elapsed = (now_ms.saturating_sub(perf_guard.start_epoch_ms) as f64) / 1000.0;
        let cps = if elapsed > 1.0 {
            (perf.cycle_count_total as f64) / elapsed
        } else {
            0.0
        };

        let cfg = self.ctx.cfg.read().await.clone();

        let status_text = format!(
            concat!(
                "--- <b>机器人状态</b> ---\n",
                "<b>运行控制:</b>\n",
                "  计算循环: {}\n",
                "  自动交易: {}\n",
                "<b>连接与数据:</b>\n",
                "  WebSocket: {}/{} 连接块活跃\n",
                "  缓存Tickers: {} (最后更新: {:.1}s 前)\n",
                "  账户余额: 持有 {} 种资产\n",
                "<b>性能统计:</b>\n",
                "  循环速率: {:.2} 周期/秒\n",
                "  上次计算耗时: {}\n",
                "    - 快照: {}, 图构建: {}\n",
                "    - BF: {}, 验证: {}\n",
                "<b>Rust 核心:</b>\n",
                "  图构建: 已启用\n",
                "  SPFA: 已启用\n",
                "  风控/模拟: 已启用\n",
            ),
            if cfg.running {
                "运行中"
            } else {
                "已暂停"
            },
            if cfg.auto_trade_enabled {
                "<b>已启用</b>"
            } else {
                "已禁用"
            },
            ws_ok,
            ws_total,
            tickers_len,
            last_update_ago,
            self.ctx.balances.snapshot_sorted().len(),
            cps,
            format_duration_high_precision(perf.last_cycle_duration_sec),
            format_duration_high_precision(perf.snap_copy_duration_sec),
            format_duration_high_precision(perf.graph_build_duration_sec),
            format_duration_high_precision(perf.bf_call_duration_sec),
            format_duration_high_precision(perf.verification_duration_sec),
        );

        bot.send_message(msg.chat.id, status_text)
            .parse_mode(ParseMode::Html)
            .await?;
        Ok(())
    }

    async fn on_set(&self, bot: &Bot, msg: &Message, text: &str) -> ResponseResult<()> {
        let parts: Vec<&str> = text.split_whitespace().collect();
        if parts.len() != 3 {
            bot.send_message(msg.chat.id, "用法: /set [参数名] [值]")
                .await?;
            return Ok(());
        }

        let param = parts[1].to_lowercase();
        let value_str = parts[2];
        let mut cfg = self.ctx.cfg.write().await;
        match param.as_str() {
            "fee_rate" => match value_str.parse::<Decimal>() {
                Ok(v) => {
                    cfg.set_fee_rate(v);
                    if let Err(e) = self.ctx.graph.set_fee_rate(v) {
                        bot.send_message(msg.chat.id, format!("fee_rate 更新失败: {}", e))
                            .await?;
                        return Ok(());
                    }
                }
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("无效值格式: {}", e))
                        .await?;
                    return Ok(());
                }
            },
            "min_profit" => match value_str.parse::<Decimal>() {
                Ok(v) => cfg.set_min_profit(v),
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("无效值格式: {}", e))
                        .await?;
                    return Ok(());
                }
            },
            "depth" => match value_str.parse::<usize>() {
                Ok(v) => cfg.set_max_depth(v),
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("无效值格式: {}", e))
                        .await?;
                    return Ok(());
                }
            },
            _ => {
                bot.send_message(msg.chat.id, format!("未知参数: {}", param))
                    .await?;
                return Ok(());
            }
        }

        info!("配置更新 via TG: {} -> {}", param, value_str);
        bot.send_message(
            msg.chat.id,
            format!("参数 `{}` 已更新为 `{}`", param, value_str),
        )
        .parse_mode(ParseMode::Html)
        .await?;
        Ok(())
    }

    async fn on_trade(&self, bot: &Bot, msg: &Message, text: &str) -> ResponseResult<()> {
        let parts: Vec<&str> = text.split_whitespace().collect();
        if parts.len() == 1 {
            let status = if self.ctx.cfg.read().await.auto_trade_enabled {
                "已启用"
            } else {
                "已禁用"
            };
            bot.send_message(
                msg.chat.id,
                format!(
                    "当前自动交易状态: {}\n使用 `/trade on` 或 `/trade off` 切换。",
                    status
                ),
            )
            .parse_mode(ParseMode::Html)
            .await?;
            return Ok(());
        }

        match parts[1].to_lowercase().as_str() {
            "on" => {
                let keyboard =
                    InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                        "确认启用自动交易",
                        "confirm_trade_on",
                    )]]);
                bot
                    .send_message(
                        msg.chat.id,
                        "<b><u>警告!</u></b> 您确定要启用自动交易吗?\n启用后将自动执行真实交易，可能导致资金损失。",
                    )
                    .reply_markup(keyboard)
                    .parse_mode(ParseMode::Html)
                    .await?;
            }
            "off" => {
                {
                    let mut cfg = self.ctx.cfg.write().await;
                    cfg.auto_trade_enabled = false;
                }
                info!("自动交易已由用户禁用。");
                bot.send_message(msg.chat.id, "自动交易已禁用。").await?;
            }
            _ => {
                bot.send_message(msg.chat.id, "用法: `/trade on` 或 `/trade off`")
                    .parse_mode(ParseMode::Html)
                    .await?;
            }
        }
        Ok(())
    }

    async fn on_pause(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        {
            let mut cfg = self.ctx.cfg.write().await;
            cfg.running = false;
        }
        info!("套利计算已暂停。");
        bot.send_message(msg.chat.id, "套利计算循环已暂停。")
            .await?;
        Ok(())
    }

    async fn on_resume(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        {
            let mut cfg = self.ctx.cfg.write().await;
            cfg.running = true;
        }
        info!("套利计算已恢复。");
        bot.send_message(msg.chat.id, "套利计算循环已恢复。")
            .await?;
        Ok(())
    }

    async fn on_balance(&self, bot: &Bot, msg: &Message) -> ResponseResult<()> {
        let balances = self.ctx.balances.snapshot_sorted();
        if balances.is_empty() {
            bot.send_message(msg.chat.id, "余额信息尚不可用。").await?;
            return Ok(());
        }

        let mut text = String::from("<b>当前可用余额 (非零):</b>\n<pre>");
        for (k, v) in balances {
            text.push_str(&format!("{}: {}\n", k, v));
        }
        text.push_str("</pre>");

        bot.send_message(msg.chat.id, text)
            .parse_mode(ParseMode::Html)
            .link_preview_options(disable_link_preview())
            .await?;
        Ok(())
    }
}
