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

## 5. 性能基准（perf-test）

用于跑一段标准化的行情 WebSocket 基准，不依赖 `API_KEY` / `API_SECRET` / `TELEGRAM_BOT_TOKEN`。

```powershell
cargo run --release -- perf-test record   # 先录制（默认 100 秒）
cargo run --release -- perf-test replay   # 基准重放：WS 摄入 + SPFA + 模拟下单（不交易）
cargo run --release -- perf-test loop     # 计时基准：从 warmup 后开始逐帧“更新 + SPFA + 模拟”，看 30 秒能跑多少轮
cargo run --release -- perf-test micro replay  # 微基准：只测 WS 摄入链路
cargo run --release -- perf-test live     # 在线接收后做一次基准（输入不固定）
cargo run --release -- perf-test micro live    # 在线微基准：只测 WS 摄入链路
```

说明：

- `perf-test replay` 是默认 benchmark：会在 ingest 之后额外跑 `PERF_TEST_BENCH_CYCLES` 次计算，用于更稳定地看 SPFA/模拟耗时。
- `perf-test loop` 是计时 benchmark：会先 warmup N 条 frame，然后在固定墙钟时长内逐帧执行“更新 + SPFA + 模拟”，输出 `loops/s`（trace 不够会自动循环回放，输出里有 `wrap_count`）。
- `perf-test micro replay` 才是只测“trace 解包/解码 + JSON 解析 + 更新 ticker/边权重”的吞吐（不跑 SPFA）。
- 如果你希望按录制时间节奏回放（大约跑满 trace 时长），设置 `PERF_TEST_REPLAY_PACE=1`；否则会尽可能快回放（更偏吞吐测试）。

可选环境变量：

- `PERF_TEST_DURATION_SEC`（默认 `100`）
- `PERF_TEST_MAX_PAIRS`（不设置表示不过滤）
- `PERF_TEST_TRACE_PATH`（默认 `perf_traces/book_ticker.jsonl`）
- `PERF_TEST_REPEAT`（默认 `1`；replay 重放次数，用于多次运行取平均）
- `PERF_TEST_BENCH_CYCLES`（默认 `50`；benchmark 里 SPFA+模拟循环次数）
- `PERF_TEST_SIM_CYCLE_LIMIT`（默认 `50`；每次 SPFA 最多取多少条 cycle 去做模拟）
- `PERF_TEST_LOOP_DURATION_SEC`（默认 `30`；`perf-test loop` 的计时长度）
- `PERF_TEST_LOOP_WARMUP_FRAMES`（默认 `100000`；`perf-test loop` 开始计时前先处理多少条 frame）
- `PERF_TEST_REPLAY_PACE`（默认 `0`；为 `1` 表示按录制时间节奏回放）
- `PERF_TEST_REPLAY_LIMIT`（可选；限制回放的 frame 数）
