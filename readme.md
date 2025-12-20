# 套利程序 for qqqqqf
这是一个基于Rust的自动三角套利程序
* 控制基于TelegramBot
* 其中图构建(三角套利等)和大量计算均使用Rust完成
* 单次循环在7600x测试机上可以做到10.0µs

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