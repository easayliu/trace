//! 端到端：OTLP 客户端 -> 接收端 -> 攒批 -> 入库。

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as Any;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use tracepipe::batch::{BatchConfig, RetryConfig};
use tracepipe::pipeline::OnError;
use tracepipe::sink::{MemorySink, Sink};
use tracepipe::source::OtlpSource;
use tracepipe::{Pipeline, SpanEvent, SpanKind, StatusCode};

const TRACE_ID: [u8; 16] = [
    0xe8, 0x9a, 0x47, 0x68, 0x82, 0x23, 0x6c, 0xe0, 0xf1, 0x18, 0x6d, 0x15, 0x22, 0xc8, 0xf5, 0x9f,
];
const SERVER_SPAN: [u8; 8] = [0xe8, 0xb0, 0xe7, 0x3e, 0x21, 0x32, 0xf2, 0x1c];
const CLIENT_SPAN: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
/// 2026-09-07 03:04:08.914293456 UTC
const START: u64 = 1_788_750_248_914_293_456;

fn kv(key: &str, value: Any) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// 一个 resource、一个 scope、两个 span：server 出错带 exception 事件，client 是它的子 span。
fn sample_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", Any::StringValue("order-service".into())),
                    kv(
                        "k8s.pod.name",
                        Any::StringValue("order-service-7d9f8b6c4-abcde".into()),
                    ),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "io.opentelemetry.tomcat-10.0".into(),
                    version: "2.9.0".into(),
                    ..Default::default()
                }),
                spans: vec![
                    Span {
                        trace_id: TRACE_ID.to_vec(),
                        span_id: SERVER_SPAN.to_vec(),
                        name: "GET /orders/{id}".into(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: START,
                        end_time_unix_nano: START + 12_345_678,
                        attributes: vec![
                            kv("http.request.method", Any::StringValue("GET".into())),
                            kv("http.response.status_code", Any::IntValue(500)),
                        ],
                        events: vec![span::Event {
                            time_unix_nano: START + 10_000_000,
                            name: "exception".into(),
                            attributes: vec![kv(
                                "exception.type",
                                Any::StringValue("java.lang.IllegalStateException".into()),
                            )],
                            ..Default::default()
                        }],
                        status: Some(Status {
                            code: 2,
                            message: "order closed".into(),
                        }),
                        ..Default::default()
                    },
                    Span {
                        trace_id: TRACE_ID.to_vec(),
                        span_id: CLIENT_SPAN.to_vec(),
                        parent_span_id: SERVER_SPAN.to_vec(),
                        name: "SELECT orders".into(),
                        kind: span::SpanKind::Client as i32,
                        start_time_unix_nano: START + 1_000_000,
                        end_time_unix_nano: START + 3_000_000,
                        links: vec![span::Link {
                            trace_id: TRACE_ID.to_vec(),
                            span_id: SERVER_SPAN.to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// 两个端口都用 0 绑好，交给 source，返回 (source, grpc 地址, http 地址)。
fn bound_source() -> (OtlpSource, String, String) {
    let grpc = TcpListener::bind("127.0.0.1:0").unwrap();
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc_addr = grpc.local_addr().unwrap().to_string();
    let http_addr = http.local_addr().unwrap().to_string();
    (
        OtlpSource::new().grpc_listener(grpc).http_listener(http),
        grpc_addr,
        http_addr,
    )
}

fn batch() -> BatchConfig {
    BatchConfig::default()
        .max_events(100)
        .timeout(Duration::from_millis(50))
}

async fn wait_for(mut ready: impl FnMut() -> bool, label: &str) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("等待超时: {label}");
}

fn assert_sample(events: &[SpanEvent]) {
    assert_eq!(events.len(), 2);
    let server = &events[0];
    assert_eq!(server.trace_id, "e89a476882236ce0f1186d1522c8f59f");
    assert_eq!(server.span_id, "e8b0e73e2132f21c");
    assert_eq!(server.parent_span_id, "");
    assert_eq!(&*server.service_name, "order-service");
    assert_eq!(server.span_name, "GET /orders/{id}");
    assert_eq!(server.span_kind, SpanKind::Server);
    assert_eq!(server.status_code, StatusCode::Error);
    assert_eq!(server.status_message, "order closed");
    assert_eq!(server.timestamp, START);
    assert_eq!(server.duration_ns, 12_345_678);
    assert_eq!(&*server.scope_name, "io.opentelemetry.tomcat-10.0");
    assert_eq!(
        server.attribute("http.response.status_code"),
        Some(&serde_json::json!(500))
    );
    assert_eq!(
        server.attribute("k8s.pod.name"),
        Some(&serde_json::json!("order-service-7d9f8b6c4-abcde"))
    );
    assert_eq!(server.events.len(), 1);
    assert_eq!(server.events[0].name, "exception");
    assert_eq!(
        server.events[0].attributes["exception.type"],
        "java.lang.IllegalStateException"
    );

    let client = &events[1];
    assert_eq!(client.parent_span_id, "e8b0e73e2132f21c");
    assert_eq!(client.span_kind, SpanKind::Client);
    assert_eq!(client.status_code, StatusCode::Unset);
    assert_eq!(client.links.len(), 1);
    assert_eq!(client.links[0].span_id, "e8b0e73e2132f21c");
    // 同一个 resource 下的 span 共享同一份属性
    assert!(Arc::ptr_eq(
        &server.resource_attributes,
        &client.resource_attributes
    ));
}

#[tokio::test]
async fn grpc_export_lands_in_sink() {
    let (source, grpc_addr, _) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let response = client.export(sample_request()).await.unwrap().into_inner();
    assert!(response.partial_success.is_none());

    wait_for(|| events.lock().unwrap().len() == 2, "两条 span").await;
    running.stop().await.unwrap();
    assert_sample(&events.lock().unwrap());
}

#[tokio::test]
async fn http_protobuf_and_json_land_in_sink() {
    let (source, _, http_addr) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    let url = format!("http://{http_addr}/v1/traces");
    let http = reqwest::Client::new();

    // protobuf
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .body(sample_request().encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    wait_for(|| events.lock().unwrap().len() == 2, "protobuf 两条").await;
    assert_sample(&events.lock().unwrap());

    // JSON：OTLP/JSON 的写法，id 是 hex 串、纳秒是十进制串
    let json = serde_json::to_string(&sample_request()).unwrap();
    assert!(
        json.contains("\"traceId\":\"e89a476882236ce0f1186d1522c8f59f\""),
        "{json}"
    );
    let response = http
        .post(&url)
        .header("content-type", "application/json")
        .body(json)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "{}");
    wait_for(|| events.lock().unwrap().len() == 4, "JSON 再两条").await;
    assert_sample(&events.lock().unwrap()[2..]);

    running.stop().await.unwrap();
}

#[tokio::test]
async fn http_accepts_gzip_and_rejects_bad_input() {
    use std::io::Write;

    let (source, _, http_addr) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    let url = format!("http://{http_addr}/v1/traces");
    let http = reqwest::Client::new();

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(&sample_request().encode_to_vec())
        .unwrap();
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(encoder.finish().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    wait_for(|| events.lock().unwrap().len() == 2, "gzip 两条").await;

    // 不认的 Content-Type
    let response = http.post(&url).body("x").send().await.unwrap();
    assert_eq!(response.status(), 415);
    // 解析不了
    let response = http
        .post(&url)
        .header("content-type", "application/json")
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    // 声明了 gzip 却不是
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    running.stop().await.unwrap();
    assert_eq!(events.lock().unwrap().len(), 2, "坏请求不该进来");
}

#[tokio::test]
async fn static_fields_and_transform() {
    let (source, grpc_addr, _) = bound_source();
    let fields = [("cluster".to_owned(), serde_json::Value::from("bj-prod"))]
        .into_iter()
        .collect();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source.fields(fields))
        .transform(|mut event: SpanEvent| {
            (event.status_code == StatusCode::Error).then(|| {
                event.insert("env", "prod");
                event
            })
        })
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    client.export(sample_request()).await.unwrap();

    wait_for(|| events.lock().unwrap().len() == 1, "只留 Error").await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events[0].status_code, StatusCode::Error);
    assert!(
        events[0].fields.len() == 1,
        "静态字段走 shared，不逐条拷进 fields"
    );
    let json: serde_json::Value = serde_json::from_str(&events[0].to_json_line().unwrap()).unwrap();
    assert_eq!(json["cluster"], "bj-prod");
    assert_eq!(json["env"], "prod");
    assert_eq!(json["timestamp"], "2026-09-07 03:04:08.914293456+00:00");
}

struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn write(&mut self, _events: &[SpanEvent]) -> tracepipe::Result<()> {
        Err(tracepipe::Error::other("存储挂了"))
    }
}

/// 没开 wait_for_write：客户端立刻拿到成功，写失败把 pipeline 停掉，根因不能被转述盖掉。
#[tokio::test]
async fn write_failure_stops_pipeline_and_reports_root_cause() {
    let (source, grpc_addr, _) = bound_source();
    let running = Pipeline::builder()
        .source(source)
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(20),
        })
        .on_error(OnError::Stop)
        .build()
        .unwrap()
        .spawn();

    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    client.export(sample_request()).await.unwrap();

    let err = running
        .wait()
        .await
        .expect_err("写入失败应当把 pipeline 停掉");
    assert!(
        err.to_string().contains("存储挂了"),
        "根因被转述盖掉了: {err}"
    );
}

/// 开了 wait_for_write：写失败要反映成客户端的导出失败（UNAVAILABLE），SDK 才会重发。
#[tokio::test]
async fn wait_for_write_surfaces_failure_to_client() {
    let (source, grpc_addr, _) = bound_source();
    let running = Pipeline::builder()
        .source(source.wait_for_write(true))
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        })
        .on_error(OnError::Drop)
        .build()
        .unwrap()
        .spawn();

    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let status = client
        .export(sample_request())
        .await
        .expect_err("没落库不该回成功");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");

    running.stop().await.unwrap();
}

struct CountingSink(Arc<Mutex<usize>>);

#[async_trait]
impl Sink for CountingSink {
    async fn write(&mut self, events: &[SpanEvent]) -> tracepipe::Result<()> {
        *self.0.lock().unwrap() += events.len();
        Ok(())
    }
}

/// 开了 wait_for_write：回成功的时候数据已经在存储里了。
#[tokio::test]
async fn wait_for_write_acks_after_write() {
    let (source, _, http_addr) = bound_source();
    let written = Arc::new(Mutex::new(0usize));
    let running = Pipeline::builder()
        .source(source.wait_for_write(true))
        .sink(CountingSink(Arc::clone(&written)))
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let response = reqwest::Client::new()
        .post(format!("http://{http_addr}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .body(sample_request().encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(*written.lock().unwrap(), 2, "回 200 之前就该写完");

    running.stop().await.unwrap();
}

struct SlowSink {
    writes: Arc<Mutex<usize>>,
    delay: Duration,
}

#[async_trait]
impl Sink for SlowSink {
    async fn write(&mut self, _events: &[SpanEvent]) -> tracepipe::Result<()> {
        tokio::time::sleep(self.delay).await;
        *self.writes.lock().unwrap() += 1;
        Ok(())
    }
}

/// 攒下一批要和写上一批重叠，而不是「攒满 -> 停下来写 -> 再从头攒」串成一条。
#[tokio::test]
async fn accumulation_overlaps_with_slow_writes() {
    const STEP: Duration = Duration::from_millis(120);
    const WINDOW: Duration = Duration::from_millis(960);

    let (source, grpc_addr, _) = bound_source();
    let writes = Arc::new(Mutex::new(0usize));
    let running = Pipeline::builder()
        .source(source)
        .sink(SlowSink {
            writes: Arc::clone(&writes),
            delay: STEP,
        })
        .batch(BatchConfig::default().max_events(1_000_000).timeout(STEP))
        .build()
        .unwrap()
        .spawn();

    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let start = std::time::Instant::now();
    while start.elapsed() < WINDOW {
        client.export(sample_request()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    running.stop().await.unwrap();

    let count = *writes.lock().unwrap();
    let serial = WINDOW.as_millis() / (STEP.as_millis() * 2);
    assert!(
        count as u128 > serial + 1,
        "攒批与写入没有重叠：{WINDOW:?} 内只写了 {count} 批，串行也能到 {serial} 批"
    );
}

/// 端口被占着就整个不启动，报错里要有端口。
#[tokio::test]
async fn port_in_use_fails_fast() {
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap();
    let err = Pipeline::builder()
        .source(OtlpSource::new().grpc(addr).no_http())
        .sink(MemorySink::new())
        .build()
        .unwrap()
        .spawn()
        .wait()
        .await
        .expect_err("端口被占应当启动失败");
    assert!(err.to_string().contains(&addr.port().to_string()), "{err}");
}
