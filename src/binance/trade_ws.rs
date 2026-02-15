use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};
use urlencoding::encode;

use crate::binance::models::OrderResponse;
use crate::binance::sign::sign_query;

const BINANCE_WS_API_URL: &str = "wss://ws-api.binance.com/ws-api/v3";

#[derive(Clone)]
pub struct BinanceTradeWsClient {
    inner: Arc<Inner>,
}

struct Inner {
    api_key: String,
    api_secret: String,
    next_id: AtomicU64,
    conn_ok: AtomicBool,
    tx: mpsc::Sender<String>,
    pending: DashMap<u64, oneshot::Sender<anyhow::Result<OrderResponse>>>,
}

impl BinanceTradeWsClient {
    pub async fn connect(api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel::<String>(1024);
        let inner = Arc::new(Inner {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            next_id: AtomicU64::new(1),
            conn_ok: AtomicBool::new(false),
            tx,
            pending: DashMap::new(),
        });

        tokio::spawn(ws_task(inner.clone(), rx));

        let client = Self { inner };
        client.wait_connected(Duration::from_secs(10)).await?;
        Ok(client)
    }

    pub fn is_connected(&self) -> bool {
        self.inner.conn_ok.load(Ordering::Relaxed)
    }

    pub async fn wait_connected(&self, timeout: Duration) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.is_connected() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("交易 WebSocket 在 {:?} 内未连接成功", timeout);
    }

    pub async fn create_market_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: Option<Decimal>,
        quote_order_qty: Option<Decimal>,
    ) -> anyhow::Result<OrderResponse> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis()
            .to_string();

        let mut params = BTreeMap::<String, String>::new();
        params.insert("apiKey".to_string(), self.inner.api_key.clone());
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("side".to_string(), side.to_string());
        params.insert("type".to_string(), "MARKET".to_string());
        params.insert("recvWindow".to_string(), "5000".to_string());
        params.insert("timestamp".to_string(), ts);
        params.insert("newOrderRespType".to_string(), "FULL".to_string());
        if let Some(qty) = quantity {
            params.insert("quantity".to_string(), qty.to_string());
        }
        if let Some(qoq) = quote_order_qty {
            params.insert("quoteOrderQty".to_string(), qoq.to_string());
        }

        let signature = sign_query(&self.inner.api_secret, &to_query_string(&params))?;
        params.insert("signature".to_string(), signature);

        let payload = serde_json::json!({
            "id": id,
            "method": "order.place",
            "params": params,
        });
        let msg = serde_json::to_string(&payload).context("序列化下单请求失败")?;

        let (tx, rx) = oneshot::channel::<anyhow::Result<OrderResponse>>();
        self.inner.pending.insert(id, tx);

        if let Err(e) = self.inner.tx.send(msg).await {
            self.inner.pending.remove(&id);
            anyhow::bail!("发送下单请求失败: {}", e);
        }

        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_closed)) => {
                self.inner.pending.remove(&id);
                anyhow::bail!("下单响应通道关闭");
            }
            Err(_timeout) => {
                self.inner.pending.remove(&id);
                anyhow::bail!("下单超时");
            }
        }
    }
}

fn to_query_string(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

async fn ws_task(inner: Arc<Inner>, mut rx: mpsc::Receiver<String>) {
    let mut backoff = Duration::from_millis(200);
    let backoff_max = Duration::from_secs(3);

    loop {
        match tokio_tungstenite::connect_async(BINANCE_WS_API_URL).await {
            Ok((ws, _resp)) => {
                info!("交易 WebSocket 已连接: {}", BINANCE_WS_API_URL);
                inner.conn_ok.store(true, Ordering::Relaxed);
                backoff = Duration::from_millis(200);

                let (mut writer, mut reader) = ws.split();
                let mut ping = tokio::time::interval(Duration::from_secs(25));
                ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(e) = writer.send(Message::Ping(Vec::new())).await {
                                warn!("交易 WebSocket ping 失败: {}", e);
                                break;
                            }
                        }
                        maybe_out = rx.recv() => {
                            let Some(out) = maybe_out else {
                                warn!("交易 WebSocket 发送通道关闭，退出");
                                return;
                            };
                            if let Err(e) = writer.send(Message::Text(out)).await {
                                warn!("交易 WebSocket 发送失败: {}", e);
                                break;
                            }
                        }
                        maybe_in = reader.next() => {
                            let Some(msg) = maybe_in else {
                                warn!("交易 WebSocket 读取结束");
                                break;
                            };
                            match msg {
                                Ok(Message::Text(text)) => {
                                    handle_incoming_text(inner.as_ref(), &text);
                                }
                                Ok(Message::Binary(bin)) => {
                                    if let Ok(text) = String::from_utf8(bin) {
                                        handle_incoming_text(inner.as_ref(), &text);
                                    }
                                }
                                Ok(Message::Ping(data)) => {
                                    let _ = writer.send(Message::Pong(data)).await;
                                }
                                Ok(Message::Pong(_)) => {}
                                Ok(Message::Close(_)) => break,
                                Err(e) => {
                                    warn!("交易 WebSocket 错误: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("交易 WebSocket 连接失败: {}", e);
            }
        }

        inner.conn_ok.store(false, Ordering::Relaxed);
        fail_all_pending(&inner, anyhow::anyhow!("交易 WebSocket 断开"));
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}

fn fail_all_pending(inner: &Inner, err: anyhow::Error) {
    let err = Arc::new(err);
    let ids: Vec<u64> = inner.pending.iter().map(|kv| *kv.key()).collect();
    for id in ids {
        if let Some((_, tx)) = inner.pending.remove(&id) {
            let _ = tx.send(Err(anyhow::anyhow!("{}", err)));
        }
    }
}

fn handle_incoming_text(inner: &Inner, text: &str) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };

    let id = match v.get("id") {
        Some(serde_json::Value::Number(n)) => n.as_u64(),
        Some(serde_json::Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    };
    let Some(id) = id else {
        return;
    };

    let Some((_, tx)) = inner.pending.remove(&id) else {
        return;
    };

    let status = v.get("status").and_then(|s| s.as_u64()).unwrap_or(0) as u16;

    if status == 200 {
        let res = v
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("缺少 result"))
            .and_then(|vv| {
                serde_json::from_value::<OrderResponse>(vv).context("反序列化成交回报失败")
            });
        let _ = tx.send(res);
        return;
    }

    let code = v
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_i64());
    let msg = v
        .get("error")
        .and_then(|e| e.get("msg"))
        .and_then(|m| m.as_str())
        .unwrap_or("未知错误");

    let err = match code {
        Some(c) => anyhow::anyhow!("下单失败 status={} code={} msg={}", status, c, msg),
        None => anyhow::anyhow!("下单失败 status={} msg={}", status, msg),
    };
    let _ = tx.send(Err(err));
}
