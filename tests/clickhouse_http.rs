//! ClickHouse 走的是 HTTP 接口，这里直接对着一个假服务端看发出去的原始请求。

use std::io::Read;
use std::time::Duration;

use flate2::read::GzDecoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracepipe::sink::{ClickhouseSink, Sink};
use tracepipe::SpanEvent;

/// 2026-09-07 03:04:08.914293456 UTC
const START: u64 = 1_788_750_248_914_293_456;

fn span() -> SpanEvent {
    SpanEvent {
        timestamp: START,
        trace_id: "e89a476882236ce0f1186d1522c8f59f".into(),
        span_id: "e8b0e73e2132f21c".into(),
        span_name: "压缩测试".into(),
        events: vec![tracepipe::TimedEvent {
            timestamp: START + 1,
            name: "exception".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// 收一个**完整**请求（按 Content-Length 把 body 读全），交出请求头文本和 body 原始字节。
async fn capture_full(
    response_body: &'static str,
) -> (String, tokio::task::JoinHandle<(String, Vec<u8>)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];

        let head_end = loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break raw.len();
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let head = String::from_utf8_lossy(&raw[..head_end]).to_string();

        let len: usize = head
            .to_lowercase()
            .split("content-length:")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        while raw.len() - head_end < len {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let body = raw[head_end..].to_vec();

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
            response_body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        (head, body)
    });

    (format!("http://{addr}"), handle)
}

/// 空 body 的 POST（`SELECT 1` 这种健康检查）没有 Content-Length，ClickHouse 回 411。
#[tokio::test]
async fn empty_body_still_carries_content_length_and_is_not_gzipped() {
    let (endpoint, server) = capture_full("1\n").await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_trace").timeout(Duration::from_secs(5));

    sink.execute("SELECT 1").await.unwrap();

    let (head, _) = server.await.unwrap();
    let head = head.to_lowercase();
    assert!(head.starts_with("post "), "{head}");
    assert!(head.contains("content-length:"), "411 的坑:\n{head}");
    assert!(
        !head.contains("content-encoding: gzip"),
        "空 body 不该压:\n{head}"
    );
}

/// 声明了 gzip 就得真的是 gzip：解出来必须和原始 JSONEachRow 一模一样。
#[tokio::test]
async fn insert_body_is_gzipped_and_round_trips() {
    let (endpoint, server) = capture_full("").await;
    let mut sink =
        ClickhouseSink::new(endpoint, "logs", "otel_trace").timeout(Duration::from_secs(5));

    sink.write(&[span()]).await.unwrap();

    let (head, body) = server.await.unwrap();
    let lower = head.to_lowercase();
    assert!(
        lower.contains("content-encoding: gzip"),
        "没有声明 gzip:\n{head}"
    );
    assert!(lower.contains("content-length:"), "{head}");
    assert!(
        head.contains("date_time_input_format=best_effort"),
        "带偏移的时间戳要开宽松解析:\n{head}"
    );

    let mut plain = String::new();
    GzDecoder::new(&body[..])
        .read_to_string(&mut plain)
        .expect("body 应当是合法的 gzip");
    assert!(
        plain.contains("\"span_name\":\"压缩测试\""),
        "解压后对不上: {plain}"
    );
    assert!(
        plain.ends_with('\n'),
        "JSONEachRow 每行都要以换行结尾: {plain:?}"
    );
    // 没配时区按 UTC，仍然带偏移
    assert!(
        plain.contains("\"timestamp\":\"2026-09-07 03:04:08.914293456+00:00\""),
        "{plain}"
    );
    assert!(
        plain.contains("\"events.timestamp\":[\"2026-09-07 03:04:08.914293457+00:00\"]"),
        "{plain}"
    );
}

/// 中间代理不认压缩 body 时的退路。
#[tokio::test]
async fn compression_can_be_turned_off() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_trace")
        .timeout(Duration::from_secs(5))
        .compress(false);

    sink.write(&[span()]).await.unwrap();

    let (head, body) = server.await.unwrap();
    assert!(
        !head.to_lowercase().contains("content-encoding: gzip"),
        "关掉了还在压:\n{head}"
    );
    assert!(String::from_utf8_lossy(&body).contains("\"span_name\":\"压缩测试\""));
}

/// 配了 timezone，INSERT 里的时间戳是那个时区的墙上时间 + 偏移。
#[tokio::test]
async fn timezone_puts_wall_clock_and_offset_on_timestamps() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_trace")
        .timeout(Duration::from_secs(5))
        .compress(false)
        .timezone(chrono_tz::Asia::Shanghai);

    sink.write(&[span()]).await.unwrap();

    let (_, body) = server.await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(
        body.contains("\"timestamp\":\"2026-09-07 11:04:08.914293456+08:00\""),
        "{body}"
    );
    assert!(
        body.contains("\"events.timestamp\":[\"2026-09-07 11:04:08.914293457+08:00\"]"),
        "{body}"
    );
}

/// 按顺序应答多个请求（同一条 keep-alive 连接或多条连接都行），交出每个请求的 query 参数。
async fn serve_sequence(
    responses: Vec<&'static str>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let mut queries = Vec::new();
        let mut pending = responses.into_iter();
        'conn: while pending.len() > 0 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut raw: Vec<u8> = Vec::new();
            let mut buf = [0u8; 8192];
            for response_body in pending.by_ref() {
                let head_end = loop {
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        continue 'conn;
                    }
                    raw.extend_from_slice(&buf[..n]);
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let len: usize = head
                    .to_lowercase()
                    .split("content-length:")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(0);
                while raw.len() - head_end < len {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                raw.drain(..head_end + len);

                let query = head
                    .split_whitespace()
                    .nth(1)
                    .and_then(|path| path.split("query=").nth(1))
                    .and_then(|rest| rest.split('&').next())
                    .map(percent_decode)
                    .unwrap_or_default();
                queries.push(query);

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
            }
        }
        queries
    });

    (format!("http://{addr}"), handle)
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

const ALL_COLUMNS: &str = "timestamp\ntrace_id\nspan_id\nparent_span_id\ntrace_state\nspan_name\nspan_kind\nservice_name\nduration_ns\nstatus_code\nstatus_message\nscope_name\nscope_version\nresource_attributes\nspan_attributes\nevents.timestamp\nevents.name\nevents.attributes\nlinks.trace_id\nlinks.span_id\nlinks.trace_state\nlinks.attributes\n";

/// 表存在但列没跟上配置：healthcheck 必须把缺的列点出来（Nested 的子列也算）。
#[tokio::test]
async fn healthcheck_reports_missing_columns() {
    let without_events_ts = ALL_COLUMNS.replace("events.timestamp\n", "");
    let columns: &'static str = Box::leak(format!("{without_events_ts}cluster\n").into_boxed_str());
    let (endpoint, server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_trace")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![
            ("cluster".to_owned(), "LowCardinality(String)".to_owned()),
            ("env".to_owned(), "LowCardinality(String)".to_owned()),
        ]);

    let err = sink.healthcheck().await.expect_err("缺列应当报错");
    let msg = err.to_string();
    assert!(msg.contains("缺列 events.timestamp, env"), "{msg}");
    assert!(msg.contains("--ddl"), "要告诉人怎么补: {msg}");

    let queries = server.await.unwrap();
    assert_eq!(queries.len(), 3, "{queries:?}");
    assert!(
        queries[2].contains("system.columns")
            && queries[2].contains("database = 'logs'")
            && queries[2].contains("table = 'otel_trace'"),
        "{}",
        queries[2]
    );
}

#[tokio::test]
async fn healthcheck_passes_when_columns_present() {
    let columns: &'static str =
        Box::leak(format!("{ALL_COLUMNS}env\nextra_col\n").into_boxed_str());
    let (endpoint, server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_trace")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![(
            "env".to_owned(),
            "LowCardinality(String)".to_owned(),
        )]);

    sink.healthcheck().await.expect("列齐了不该报错");
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn healthcheck_reports_missing_table() {
    let (endpoint, _server) = serve_sequence(vec!["1\n", "0\n"]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_trace").timeout(Duration::from_secs(5));
    let err = sink.healthcheck().await.expect_err("表不存在应当报错");
    assert!(err.to_string().contains("不存在"), "{err}");
}
