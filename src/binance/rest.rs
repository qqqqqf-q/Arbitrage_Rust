use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use urlencoding::encode;

use crate::binance::models::{
    AccountInfo, DepthResponse, ExchangeInfo, OrderResponse, Ticker24h,
};
use crate::binance::sign::sign_query;

#[derive(Clone)]
pub struct BinanceRestClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl BinanceRestClient {
    pub fn new(api_key: &str, api_secret: &str) -> anyhow::Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-MBX-APIKEY",
            HeaderValue::from_str(api_key).map_err(|_| anyhow::anyhow!("API_KEY 含非法字符"))?,
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(15))
            .build()?;

        Ok(Self {
            http,
            base_url: "https://api.binance.com".to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
        })
    }

    pub async fn exchange_info(&self) -> anyhow::Result<ExchangeInfo> {
        self.get_json("/api/v3/exchangeInfo", None::<&str>).await
    }

    pub async fn tickers_24h(&self, binance_symbols: &[String]) -> anyhow::Result<Vec<Ticker24h>> {
        let symbols_json = serde_json::to_string(binance_symbols)?;
        let path = format!("/api/v3/ticker/24hr?symbols={}", encode(&symbols_json));
        self.get_json(&path, None::<&str>).await
    }

    pub async fn depth(&self, symbol: &str, limit: usize) -> anyhow::Result<DepthResponse> {
        let path = format!("/api/v3/depth?symbol={}&limit={}", symbol, limit);
        self.get_json(&path, None::<&str>).await
    }

    pub async fn account(&self) -> anyhow::Result<AccountInfo> {
        let query = self.signed_query("recvWindow=5000")?;
        let path = format!("/api/v3/account?{}", query);
        self.get_json(&path, Some("SIGNED")).await
    }

    pub async fn create_market_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: Option<Decimal>,
        quote_order_qty: Option<Decimal>,
    ) -> anyhow::Result<OrderResponse> {
        let mut parts = vec![
            format!("symbol={}", symbol),
            format!("side={}", side),
            "type=MARKET".to_string(),
            "recvWindow=5000".to_string(),
        ];

        if let Some(qty) = quantity {
            parts.push(format!("quantity={}", qty));
        }
        if let Some(qoq) = quote_order_qty {
            parts.push(format!("quoteOrderQty={}", qoq));
        }

        let query_base = parts.join("&");
        let query = self.signed_query(&query_base)?;

        let url = format!("{}/api/v3/order?{}", self.base_url, query);
        let resp = self
            .http
            .post(url)
            .send()
            .await?
            .error_for_status()?
            .json::<OrderResponse>()
            .await?;
        Ok(resp)
    }

    fn signed_query(&self, base: &str) -> anyhow::Result<String> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis();
        let query = format!("{}&timestamp={}", base, ts);
        let signature = sign_query(&self.api_secret, &query)?;
        Ok(format!("{}&signature={}", query, signature))
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        path_with_query: &str,
        _kind: Option<&str>,
    ) -> anyhow::Result<T> {
        let url = format!("{}{}", self.base_url, path_with_query);
        let resp = self.http.get(url).send().await?.error_for_status()?;
        Ok(resp.json::<T>().await?)
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

