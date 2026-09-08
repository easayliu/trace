//! 把 OTLP 的 `ExportTraceServiceRequest` 拆成一条条 [`SpanEvent`]。
//!
//! gRPC 和 HTTP 两个入口、stdin 的 JSON 行，解出来的都是同一个类型，都经过这里。

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as Any;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span;
use serde_json::Value as Json;

use crate::error::Result;
use crate::event::{Attributes, SpanEvent, SpanKind, SpanLink, StatusCode, TimedEvent};

/// resource 里没有 `service.name` 时的兜底，和 OTel SDK 自己的默认值一个写法。
pub const UNKNOWN_SERVICE: &str = "unknown_service";

/// 一次导出请求里的全部 span。`shared` 是要挂到每条上的静态字段。
pub fn convert(
    request: ExportTraceServiceRequest,
    shared: Option<&Arc<BTreeMap<String, Json>>>,
) -> Vec<SpanEvent> {
    let mut out = Vec::with_capacity(
        request
            .resource_spans
            .iter()
            .flat_map(|r| r.scope_spans.iter())
            .map(|s| s.spans.len())
            .sum(),
    );

    for resource_spans in request.resource_spans {
        let resource_attributes = Arc::new(
            resource_spans
                .resource
                .map(|r| attributes(r.attributes))
                .unwrap_or_default(),
        );
        let service_name: Arc<str> = Arc::from(
            resource_attributes
                .get("service.name")
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(UNKNOWN_SERVICE),
        );

        for scope_spans in resource_spans.scope_spans {
            let (scope_name, scope_version): (Arc<str>, Arc<str>) = match scope_spans.scope {
                Some(scope) => (Arc::from(scope.name), Arc::from(scope.version)),
                None => (Arc::from(""), Arc::from("")),
            };
            for span in scope_spans.spans {
                out.push(convert_span(
                    span,
                    Arc::clone(&service_name),
                    Arc::clone(&resource_attributes),
                    Arc::clone(&scope_name),
                    Arc::clone(&scope_version),
                    shared.cloned(),
                ));
            }
        }
    }
    out
}

fn convert_span(
    span: Span,
    service_name: Arc<str>,
    resource_attributes: Arc<Attributes>,
    scope_name: Arc<str>,
    scope_version: Arc<str>,
    shared: Option<Arc<BTreeMap<String, Json>>>,
) -> SpanEvent {
    let (status_code, status_message) = span
        .status
        .map(|s| (StatusCode::from_otlp(s.code), s.message))
        .unwrap_or_default();

    SpanEvent {
        timestamp: span.start_time_unix_nano,
        trace_id: hex(&span.trace_id),
        span_id: hex(&span.span_id),
        parent_span_id: hex(&span.parent_span_id),
        trace_state: span.trace_state,
        span_name: span.name,
        span_kind: SpanKind::from_otlp(span.kind),
        service_name,
        // SDK 的时钟偶尔会给出 end < start（时钟回拨），别让减法 panic
        duration_ns: span
            .end_time_unix_nano
            .saturating_sub(span.start_time_unix_nano),
        status_code,
        status_message,
        scope_name,
        scope_version,
        resource_attributes,
        span_attributes: attributes(span.attributes),
        events: span
            .events
            .into_iter()
            .map(|e| TimedEvent {
                timestamp: e.time_unix_nano,
                name: e.name,
                attributes: attributes(e.attributes),
            })
            .collect(),
        links: span
            .links
            .into_iter()
            .map(|l| SpanLink {
                trace_id: hex(&l.trace_id),
                span_id: hex(&l.span_id),
                trace_state: l.trace_state,
                attributes: attributes(l.attributes),
            })
            .collect(),
        fields: BTreeMap::new(),
        shared,
    }
}

/// 原始 id 字节 → 小写 hex。OTLP 里 trace id 是 16 字节、span id 是 8 字节，所以出来
/// 就是 32 / 16 位，正好和 logpipe 归一化之后的写法一样；空的（根 span 的 parent）
/// 给空串。
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// 属性列表 → map。同名 key 后者覆盖前者（OTLP 规定 key 唯一，真重复了也别报错）。
pub fn attributes(list: Vec<KeyValue>) -> Attributes {
    list.into_iter()
        .map(|kv| (kv.key, attribute_value(kv.value)))
        .collect()
}

/// `AnyValue` → JSON，类型原样保留，落到 ClickHouse 的 `JSON` 列里每个 key 就是带类型的
/// 子列：字符串、整数、小数、布尔照搬；bytes 转 base64 串；数组、嵌套对象就是 JSON 数组 /
/// 对象。没有值的属性记 `null`。
pub fn attribute_value(value: Option<AnyValue>) -> Json {
    value.and_then(|v| v.value).map_or(Json::Null, to_json)
}

fn to_json(value: Any) -> Json {
    match value {
        Any::StringValue(s) => Json::from(s),
        Any::BoolValue(b) => Json::from(b),
        Any::IntValue(i) => Json::from(i),
        // NaN / Inf 在 JSON 里没有写法，记 null
        Any::DoubleValue(d) => serde_json::Number::from_f64(d).map_or(Json::Null, Json::Number),
        Any::BytesValue(b) => Json::from(base64::engine::general_purpose::STANDARD.encode(b)),
        Any::ArrayValue(array) => Json::Array(
            array
                .values
                .into_iter()
                .map(|v| v.value.map_or(Json::Null, to_json))
                .collect(),
        ),
        Any::KvlistValue(list) => Json::Object(
            list.values
                .into_iter()
                .map(|kv| {
                    (
                        kv.key,
                        kv.value.and_then(|v| v.value).map_or(Json::Null, to_json),
                    )
                })
                .collect(),
        ),
        // 只在 profiling 信号里出现，trace 里遇到按 OTLP 的说法当空值处理
        Any::StringValueStrindex(_) => Json::Null,
    }
}

/// 解析一行 OTLP/JSON（`ExportTraceServiceRequest` 的 JSON 编码，也就是 OTel collector
/// `file` exporter 每行写的那种）。
pub fn decode_json(text: &str) -> Result<ExportTraceServiceRequest> {
    Ok(serde_json::from_str(text)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    fn kv(key: &str, value: Any) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    #[test]
    fn hex_is_lowercase_and_empty_for_missing() {
        assert_eq!(hex(&[0xe8, 0x9a, 0x47, 0x68]), "e89a4768");
        assert_eq!(hex(&[]), "");
        assert_eq!(hex(&[0u8; 16]).len(), 32);
    }

    #[test]
    fn attribute_values_keep_their_types() {
        let attrs = attributes(vec![
            kv("s", Any::StringValue("x".into())),
            kv("b", Any::BoolValue(true)),
            kv("i", Any::IntValue(-7)),
            kv("d", Any::DoubleValue(1.5)),
            kv("bytes", Any::BytesValue(vec![0xde, 0xad])),
            kv(
                "arr",
                Any::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Any::IntValue(1)),
                        },
                        AnyValue {
                            value: Some(Any::StringValue("two".into())),
                        },
                    ],
                }),
            ),
            kv(
                "obj",
                Any::KvlistValue(KeyValueList {
                    values: vec![kv("k", Any::BoolValue(false))],
                }),
            ),
            KeyValue {
                key: "empty".into(),
                value: None,
                ..Default::default()
            },
        ]);
        assert_eq!(attrs["s"], Json::from("x"));
        assert_eq!(attrs["b"], Json::from(true));
        assert_eq!(attrs["i"], Json::from(-7));
        assert_eq!(attrs["d"], Json::from(1.5));
        assert_eq!(attrs["bytes"], Json::from("3q0="));
        assert_eq!(attrs["arr"], serde_json::json!([1, "two"]));
        assert_eq!(attrs["obj"], serde_json::json!({"k": false}));
        assert_eq!(attrs["empty"], Json::Null);
        assert_eq!(
            attribute_value(Some(AnyValue {
                value: Some(Any::DoubleValue(f64::NAN))
            })),
            Json::Null
        );
    }

    #[test]
    fn decodes_otlp_json_with_string_encoded_ids_and_nanos() {
        // OTLP/JSON 的口径：id 是 hex 串、fixed64 是十进制串、枚举是数字
        let request = decode_json(
            r#"{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"order-service"}}]},"scopeSpans":[{"scope":{"name":"io.opentelemetry.tomcat","version":"2.9.0"},"spans":[{"traceId":"e89a476882236ce0f1186d1522c8f59f","spanId":"e8b0e73e2132f21c","parentSpanId":"","name":"GET /orders","kind":2,"startTimeUnixNano":"1789000000000000000","endTimeUnixNano":"1789000000012345678","attributes":[{"key":"http.response.status_code","value":{"intValue":"500"}}],"status":{"code":2,"message":"boom"}}]}]}]}"#,
        )
        .unwrap();
        let spans = convert(request, None);
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.trace_id, "e89a476882236ce0f1186d1522c8f59f");
        assert_eq!(span.span_id, "e8b0e73e2132f21c");
        assert_eq!(span.parent_span_id, "");
        assert_eq!(&*span.service_name, "order-service");
        assert_eq!(&*span.scope_name, "io.opentelemetry.tomcat");
        assert_eq!(span.span_kind, SpanKind::Server);
        assert_eq!(span.status_code, StatusCode::Error);
        assert_eq!(span.status_message, "boom");
        assert_eq!(span.timestamp, 1_789_000_000_000_000_000);
        assert_eq!(span.duration_ns, 12_345_678);
        // intValue 落库后仍是整数，不是 "500"
        assert_eq!(
            span.attribute("http.response.status_code"),
            Some(&Json::from(500))
        );
        assert_eq!(
            span.attribute("service.name"),
            Some(&Json::from("order-service"))
        );
    }

    #[test]
    fn missing_service_name_and_clock_skew_are_tolerated() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![opentelemetry_proto::tonic::trace::v1::ResourceSpans {
                resource: None,
                scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        start_time_unix_nano: 100,
                        end_time_unix_nano: 50,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let spans = convert(request, None);
        assert_eq!(&*spans[0].service_name, UNKNOWN_SERVICE);
        assert_eq!(spans[0].duration_ns, 0);
        assert_eq!(spans[0].span_kind, SpanKind::Unspecified);
        assert_eq!(spans[0].status_code, StatusCode::Unset);
    }
}
