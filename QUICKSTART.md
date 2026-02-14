# rust_recode 快速开始

## 1. 准备环境变量

本项目不会在代码或文件中写入任何密钥，请通过环境变量提供：

- `API_KEY`
- `API_SECRET`
- `TELEGRAM_BOT_TOKEN`
- `AUTHORIZED_USER_ID`（可选；为 `0` 表示不限制）
- `BASE_ASSETS`（可选；起始/结算基币列表，逗号分隔，例如 `USDT` / `USDC` / `USDT,USDC`）
- `MAX_ARBITRAGE_DEPTH`（可选；最大跳数/深度，默认 `6`）
- `WEBSOCKET_CHUNK_SIZE`（可选；单条 WebSocket 连接的订阅数量；遇到 502 可适当调小，例如 `80`）
- `TICKER_WARMUP_RATIO`（可选；Ticker 预热比例，取值 (0,1]，默认 `0.8`）
- `TICKER_WARMUP_TIMEOUT_SEC`（可选；Ticker 预热超时秒数，默认 `20`；设为 `0` 表示不超时）
- `TICKER_WARMUP_MIN_VALID`（可选；Ticker 预热最小有效数量下限，默认 `0`）
- `MAKER_ONLY`（可选；默认 `false`。本项目当前使用 MARKET（市价/吃单）执行真实交易；如你想禁止自动交易走市价，可设置 `MAKER_ONLY=true` 并自行接入限价单逻辑）
- 说明：行情使用 Binance 行情 WebSocket；订单簿风控使用 Depth WebSocket（`@depth10@100ms` 等）；下单使用 Binance 交易 WebSocket API（`wss://ws-api.binance.com/ws-api/v3`）。余额/交易所信息等仍会走 REST。

Windows（当前 PowerShell 会话）示例：

```powershell
$env:API_KEY="你的key"
$env:API_SECRET="你的secret"
$env:TELEGRAM_BOT_TOKEN="你的tg token"
$env:AUTHORIZED_USER_ID="0"
$env:BASE_ASSETS="USDT,USDC"
$env:MAX_ARBITRAGE_DEPTH="6"
$env:MAKER_ONLY="false"
```

也可以使用 `setup_api.bat`（只设置当前 cmd 会话变量）。

## 2. 运行（release）

在仓库根目录执行：

```powershell
cd rust_recode
cargo run --release
```

## 3. Telegram 命令对齐

支持与 `arbitrage_recode.py` 对齐的命令：

- `/start` `/help` `/status` `/balance`
- `/pause` `/resume`
- `/trade on|off`（默认关闭，避免误交易）
- `/set fee_rate|min_profit|depth 值`

## 4. 最佳性能编译（可选）

在你自己的机器上可以开启 CPU 指令集优化：

```powershell
$env:RUSTFLAGS="-C target-cpu=native"
cargo build --release
```
