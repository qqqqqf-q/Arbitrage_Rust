use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let perf_test = is_perf_test_mode(&args);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(if perf_test { "warn" } else { "info" }.parse()?),
        )
        .init();

    if perf_test {
        return rust_recode::perf_test::run(&args).await;
    }
    rust_recode::app::run().await
}

fn is_perf_test_mode(args: &[String]) -> bool {
    args.iter().any(|a| a == "perf-test" || a == "--perf-test")
}
