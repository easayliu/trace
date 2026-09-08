//! 退出信号：k8s 终止 Pod 发的是 SIGTERM，不是 SIGINT。只认 Ctrl-C 的话，
//! 容器里那套收尾逻辑（冲刷最后一批数据）一次都跑不到。
//!
//! 这里直接把真正的二进制拉起来发信号，因为要测的恰恰是进程级的信号处理。
#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const TRACE_ID: &str = "e89a476882236ce0f1186d1522c8f59f";

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// 拿一个当前空闲的端口。绑了再放掉，进程启动前被别人抢走的概率可以忽略。
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 起进程、发一条 span、确认采到之后发 `signal`，返回（退出是否正常，进程输出）。
async fn run_and_signal(signal: &str) -> (bool, String) {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = dir.path().join("tracepipe.yaml");
    std::fs::write(
        &config,
        format!(
            "source:\n  \
               type: otlp\n  \
               grpc: null\n  \
               http: 127.0.0.1:{port}\n\
             sink:\n  \
               type: console\n  \
               encoding: json\n\
             batch:\n  \
               timeout_secs: 1\n"
        ),
    )
    .unwrap();

    let out = dir.path().join("out.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_tracepipe"))
        .arg(&config)
        .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // 等端口起来再发；起不来就是测试环境的问题，别一直等
    let body = format!(
        r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"order-service"}}}}]}},"scopeSpans":[{{"spans":[{{"traceId":"{TRACE_ID}","spanId":"e8b0e73e2132f21c","name":"GET /orders","kind":2,"startTimeUnixNano":"1788750248914293456","endTimeUnixNano":"1788750248926638000"}}]}}]}}]}}"#
    );
    let client = reqwest::Client::new();
    let start = Instant::now();
    loop {
        let sent = client
            .post(format!("http://127.0.0.1:{port}/v1/traces"))
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await;
        if matches!(&sent, Ok(response) if response.status().is_success()) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "等待超时: 接收端起来 ({sent:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 等 span 确实打出来了，再发信号 —— 否则测的就不是收尾了
    let start = Instant::now();
    while !read(&out).contains(TRACE_ID) {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "等待超时: 输出第一条 span"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(child.id().to_string())
        .status()
        .unwrap();

    let status = child.wait().unwrap();
    (status.success(), read(&out))
}

#[tokio::test]
async fn sigterm_triggers_graceful_shutdown() {
    let (ok, output) = run_and_signal("TERM").await;

    assert!(
        ok,
        "SIGTERM 之后应当正常退出（被信号打死会是 143）:\n{output}"
    );
    assert!(
        output.contains("收到退出信号"),
        "SIGTERM 没有走到收尾分支:\n{output}"
    );
    assert!(output.contains("已退出"), "收尾没跑完:\n{output}");
}

/// Ctrl-C 也要通。
#[tokio::test]
async fn sigint_still_triggers_graceful_shutdown() {
    let (ok, output) = run_and_signal("INT").await;

    assert!(ok, "SIGINT 之后应当正常退出:\n{output}");
    assert!(output.contains("收到退出信号"), "{output}");
    assert!(output.contains("已退出"), "{output}");
}
