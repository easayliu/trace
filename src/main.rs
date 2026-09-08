//! 可执行入口：读一份 YAML 配置，收 OTLP trace 写进存储。
//!
//! ```bash
//! tracepipe                     # 读当前目录的 tracepipe.yaml
//! tracepipe /etc/tracepipe.yaml # 指定配置
//! tracepipe --check <config>    # 只校验配置
//! tracepipe --ddl   <config>    # 打印 ClickHouse 建表语句
//! ```

use std::process::ExitCode;

use tracepipe::config::Config;

const DEFAULT_CONFIG: &str = "tracepipe.yaml";

const USAGE: &str = "\
tracepipe —— OTLP trace 采集入库

用法:
    tracepipe [配置文件]           启动采集（默认 ./tracepipe.yaml）
    tracepipe --check [配置文件]   只校验配置，不启动
    tracepipe --ddl   [配置文件]   打印 ClickHouse 建表语句
    tracepipe --help

日志级别用 RUST_LOG 控制，例如 RUST_LOG=debug。
";

// 收请求、解码、序列化都能并行，但默认按机器核数起线程，跑在几十核的节点上就会有
// 几十个线程去抢 Pod 那点 cpu 配额。四个够用：一个 gRPC 连接一个 HTTP/2 流本来就串行。
#[tokio::main(worker_threads = 4)]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("错误: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> tracepipe::Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, path) = match args.split_first() {
        None => (Mode::Run, DEFAULT_CONFIG.to_owned()),
        Some((first, rest)) => {
            let path = rest
                .first()
                .cloned()
                .unwrap_or_else(|| DEFAULT_CONFIG.to_owned());
            match first.as_str() {
                "--help" | "-h" => {
                    print!("{USAGE}");
                    return Ok(ExitCode::SUCCESS);
                }
                "--check" => (Mode::Check, path),
                "--ddl" => (Mode::Ddl, path),
                other if other.starts_with('-') => {
                    eprint!("未知参数 {other}\n\n{USAGE}");
                    return Ok(ExitCode::from(2));
                }
                config => (Mode::Run, config.to_owned()),
            }
        }
    };

    let config = Config::load(&path)?;

    match mode {
        Mode::Check => {
            // 会校验监听地址、时区名、sink 参数
            config.check()?;
            println!("配置 {path} 校验通过");
            Ok(ExitCode::SUCCESS)
        }
        Mode::Ddl => {
            println!("{};", config.ddl()?);
            Ok(ExitCode::SUCCESS)
        }
        Mode::Run => {
            tracing::info!(config = %path, "启动 tracepipe");
            config.build()?.run().await?;
            tracing::info!("已退出");
            Ok(ExitCode::SUCCESS)
        }
    }
}

enum Mode {
    Run,
    Check,
    Ddl,
}
