# Oxidarb for qqqqqf
这是一个基于Rust的自动三角套利程序
* 控制基于TelegramBot
* 其中图构建(三角套利等)和大量计算均使用Rust完成
* 单次循环在7600x和M1Pro测试机上可以做到10.0µs

## 首次使用
```bash
$env:API_KEY="设置为你的binance api key"
$env:API_SECRET="设置为你的binance api secret"
$env:TELEGRAM_BOT_TOKEN="设置为你的TG BOT TOKEN"

cargo run --release
```
## 最佳性能编译（可选）

在你自己的机器上可以开启 CPU 指令集优化：

```powershell
$env:RUSTFLAGS="-C target-cpu=native"
cargo build --release
```

## 题外话
这算是对[这个项目](https://github.com/qqqqqf-q/Arbitrage_trade)Readme中诺言的一次实现`或许有朝一日我会重写这个项目的`  
现在我实现了,并且使用了Rust重写  
重构了代码,现在没有那么屎山了  
也不存在Python+C++的这种胶水了  

另外再给看到这个项目并且想使用 Oxidarb 这个程序的开发者：  
这个程序的市场操作空间其实非常小，大部分人在这里是做不到盈利的。原因如下：  
1. 成本门槛高：你还需要购买一台 AWS Japan 的机器。  
2. 竞争压力大：你的对手是使用了 FPGA 直接进行机器编程的大商（大厂）。   
所以这个项目基本上只作为技术展示使用，并不建议任何人直接使用。三角套利也并非真的无风险套利，大部分时间，你只会看到你的钱包慢慢亏损

### 如果你想优化代码,这里有一个性能测试的入口
```bash
export PERF_TEST_REPEAT=5
cargo run --release -- perf-test record
cargo run --release -- perf-test replay        # 默认 benchmark：带 SPFA + 模拟
cargo run --release -- perf-test loop          # 计时 benchmark：30 秒能跑多少轮（更新 + SPFA + 模拟）
cargo run --release -- perf-test micro replay  # 微基准：只测 WS 摄入链路（可选）
```
这样就可以比较算法优化差异了
