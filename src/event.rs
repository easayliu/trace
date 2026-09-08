//! Span 事件：一条 OTLP span 拍平成一行记录，字段与 ClickHouse 表一一对应。
//!
//! ```text
//! ResourceSpans ─┬─ resource.attributes ──▶ resource_attributes / service_name
//!                └─ ScopeSpans ─┬─ scope ──▶ scope_name / scope_version
//!                               └─ Span ──▶ 其余所有列（events / links 展平成并列的数组列）
//! ```
//!
//! 时间戳是 UNIX 纳秒（OTLP 的口径），落库时按 sink 配置的时区换成墙上时间并带上偏移，
//! 见 [`format_timestamp`]。

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Offset, Timelike, Utc};
use chrono_tz::Tz;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;

/// 属性保留 OTLP 里的类型：ClickHouse 那边是 `JSON` 列，每个 key 是一个带类型的子列。
/// 字符串、整数、小数、布尔原样；bytes 转 base64 串；数组和嵌套对象就是 JSON 数组 / 对象。
pub type Attributes = BTreeMap<String, Value>;

/// OTLP 的 `SpanKind`。存的名字沿用 OTel collector clickhouse exporter 的写法
/// （`Server` / `Client` ……），现成的 Grafana 面板和查询能直接套。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpanKind {
    #[default]
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn from_otlp(raw: i32) -> Self {
        match raw {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SpanKind::Unspecified => "Unspecified",
            SpanKind::Internal => "Internal",
            SpanKind::Server => "Server",
            SpanKind::Client => "Client",
            SpanKind::Producer => "Producer",
            SpanKind::Consumer => "Consumer",
        }
    }
}

/// OTLP 的 `Status.code`。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatusCode {
    #[default]
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    pub fn from_otlp(raw: i32) -> Self {
        match raw {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            StatusCode::Unset => "Unset",
            StatusCode::Ok => "Ok",
            StatusCode::Error => "Error",
        }
    }
}

/// span 上的一个带时间的事件（异常、日志点……），OTLP 叫 `Span.Event`。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimedEvent {
    /// UNIX 纳秒。
    pub timestamp: u64,
    pub name: String,
    pub attributes: Attributes,
}

/// 指向另一个 span 的链接，OTLP 叫 `Span.Link`。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpanLink {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: Attributes,
}

#[derive(Clone, Debug, Default)]
pub struct SpanEvent {
    /// span 开始时间，UNIX 纳秒。
    pub timestamp: u64,
    /// 32 位小写 hex，和 logpipe 落库的 `trace_id` 精确相等。
    pub trace_id: String,
    /// 16 位小写 hex。
    pub span_id: String,
    /// 根 span 为空串。
    pub parent_span_id: String,
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: SpanKind,
    /// resource 里的 `service.name`，没有则是 `unknown_service`。同一个 resource 下的
    /// 所有 span 共享一份。
    pub service_name: Arc<str>,
    /// `end - start`，纳秒。
    pub duration_ns: u64,
    pub status_code: StatusCode,
    pub status_message: String,
    pub scope_name: Arc<str>,
    pub scope_version: Arc<str>,
    /// 同一个 resource 下的所有 span 共享一份，逐条只加引用计数。
    pub resource_attributes: Arc<Attributes>,
    pub span_attributes: Attributes,
    pub events: Vec<TimedEvent>,
    pub links: Vec<SpanLink>,
    /// 额外字段，落库前由调用方自行追加。
    pub fields: BTreeMap<String, Value>,
    /// 来源级的固定字段（配置里的静态 `fields`）。所有事件共享一份；序列化时和
    /// `fields` 一样平铺进 JSON，同名时 `fields` 里的优先。
    pub shared: Option<Arc<BTreeMap<String, Value>>>,
}

impl SpanEvent {
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.fields.insert(key.into(), value.into())
    }

    /// 先查 `fields`，再查 `shared`。
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields
            .get(key)
            .or_else(|| self.shared.as_ref().and_then(|shared| shared.get(key)))
    }

    /// 先查 span 属性，再查 resource 属性。
    pub fn attribute(&self, key: &str) -> Option<&Value> {
        self.span_attributes
            .get(key)
            .or_else(|| self.resource_attributes.get(key))
    }

    /// 估算编码成 JSON 后的字节数，用于按体积攒批。
    pub fn estimated_size(&self) -> usize {
        fn attrs(map: &Attributes) -> usize {
            map.iter()
                .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
                .sum::<usize>()
                + 2
        }
        let fixed = self.trace_id.len()
            + self.span_id.len()
            + self.parent_span_id.len()
            + self.trace_state.len()
            + self.span_name.len()
            + self.service_name.len()
            + self.status_message.len()
            + self.scope_name.len()
            + self.scope_version.len();
        let events: usize = self
            .events
            .iter()
            .map(|e| 40 + e.name.len() + attrs(&e.attributes))
            .sum();
        let links: usize = self
            .links
            .iter()
            .map(|l| {
                l.trace_id.len() + l.span_id.len() + l.trace_state.len() + attrs(&l.attributes)
            })
            .sum();
        let entry = |(k, v): (&String, &Value)| k.len() + estimated_value_size(v) + 4;
        let extra: usize = self.fields.iter().map(entry).sum::<usize>()
            + self
                .shared
                .as_ref()
                .map_or(0, |shared| shared.iter().map(entry).sum());
        fixed
            + attrs(&self.resource_attributes)
            + attrs(&self.span_attributes)
            + events
            + links
            + extra
            + 400
    }

    /// 编码成一行 JSON（ClickHouse `JSONEachRow`），时间戳按 UTC。
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// 便于人读的一行，用于 console sink。
    pub fn to_text_line(&self) -> String {
        format!(
            "{} {} {} {} {:.3}ms {} trace={} span={} parent={}",
            format_timestamp(self.timestamp, Tz::UTC).as_str(),
            self.service_name,
            self.span_name,
            self.span_kind.as_str(),
            self.duration_ns as f64 / 1_000_000.0,
            self.status_code.as_str(),
            self.trace_id,
            self.span_id,
            self.parent_span_id,
        )
    }
}

fn estimated_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => 2 + a.iter().map(estimated_value_size).sum::<usize>() + a.len(),
        Value::Object(o) => {
            2 + o
                .iter()
                .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
                .sum::<usize>()
        }
    }
}

/// 平铺成一层 JSON，时间戳按 UTC 带 `+00:00`。ClickHouse sink 配了时区用 [`WithZone`]。
impl Serialize for SpanEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.serialize_in(serializer, Tz::UTC)
    }
}

/// 时间戳按指定时区换成墙上时间再序列化：`2026-09-07 11:04:08.914293456+08:00`。
///
/// 偏移一定带着：存进去的绝对时刻不依赖列有没有标时区，列上的时区只决定「查出来
/// 显示成几点」。
pub struct WithZone<'a> {
    pub event: &'a SpanEvent,
    pub tz: Tz,
}

impl Serialize for WithZone<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.event.serialize_in(serializer, self.tz)
    }
}

/// 把一个迭代器当 JSON 数组序列化，省得为每个数组列各建一个 Vec。
struct Seq<I>(I);

impl<I> Serialize for Seq<I>
where
    I: Iterator + Clone,
    I::Item: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.clone())
    }
}

/// 时间戳序列化成带偏移的墙上时间。
struct Ts(u64, Tz);

impl Serialize for Ts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(format_timestamp(self.0, self.1).as_str())
    }
}

impl SpanEvent {
    /// 列的顺序和 [`crate::sink::ClickhouseSink`] 的建表语句一致。events / links 按
    /// ClickHouse `Nested` 的平铺写法给：`events.timestamp` 等各是一个等长数组。
    fn serialize_in<S: Serializer>(&self, serializer: S, tz: Tz) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("timestamp", &Ts(self.timestamp, tz))?;
        map.serialize_entry("trace_id", &self.trace_id)?;
        map.serialize_entry("span_id", &self.span_id)?;
        map.serialize_entry("parent_span_id", &self.parent_span_id)?;
        map.serialize_entry("trace_state", &self.trace_state)?;
        map.serialize_entry("span_name", &self.span_name)?;
        map.serialize_entry("span_kind", self.span_kind.as_str())?;
        map.serialize_entry("service_name", &*self.service_name)?;
        map.serialize_entry("duration_ns", &self.duration_ns)?;
        map.serialize_entry("status_code", self.status_code.as_str())?;
        map.serialize_entry("status_message", &self.status_message)?;
        map.serialize_entry("scope_name", &*self.scope_name)?;
        map.serialize_entry("scope_version", &*self.scope_version)?;
        map.serialize_entry("resource_attributes", &*self.resource_attributes)?;
        map.serialize_entry("span_attributes", &self.span_attributes)?;
        map.serialize_entry(
            "events.timestamp",
            &Seq(self.events.iter().map(|e| Ts(e.timestamp, tz))),
        )?;
        map.serialize_entry("events.name", &Seq(self.events.iter().map(|e| &e.name)))?;
        map.serialize_entry(
            "events.attributes",
            &Seq(self.events.iter().map(|e| &e.attributes)),
        )?;
        map.serialize_entry(
            "links.trace_id",
            &Seq(self.links.iter().map(|l| &l.trace_id)),
        )?;
        map.serialize_entry("links.span_id", &Seq(self.links.iter().map(|l| &l.span_id)))?;
        map.serialize_entry(
            "links.trace_state",
            &Seq(self.links.iter().map(|l| &l.trace_state)),
        )?;
        map.serialize_entry(
            "links.attributes",
            &Seq(self.links.iter().map(|l| &l.attributes)),
        )?;
        if let Some(shared) = &self.shared {
            for (key, value) in shared.iter() {
                if !self.fields.contains_key(key) {
                    map.serialize_entry(key, value)?;
                }
            }
        }
        for (key, value) in &self.fields {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// 格式化好的时间戳：`2026-09-07 11:04:08.914293456+08:00`，定长 35 字节。
pub struct Timestamp([u8; 35]);

impl Timestamp {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("format_timestamp 只写 ASCII")
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// UNIX 纳秒 → `tz` 里的墙上时间，纳秒精度，末尾带该时刻的偏移。
///
/// 按位写进栈上的定长缓冲而不是 `format!`：chrono 的 `format(...)` 每次都要重新解析
/// 格式串再分配，一条 span 连 events 有好几个时间戳，这里是序列化的大头。
pub fn format_timestamp(nanos: u64, tz: Tz) -> Timestamp {
    let secs = (nanos / 1_000_000_000) as i64;
    let sub = (nanos % 1_000_000_000) as u32;
    // u64 纳秒最多到 2554 年，一定在 chrono 的范围内
    let utc = DateTime::<Utc>::from_timestamp(secs, sub).unwrap_or(DateTime::UNIX_EPOCH);
    let local = utc.with_timezone(&tz);
    let offset = local.offset().fix().local_minus_utc();

    fn put(slot: &mut [u8], mut value: u32) {
        for byte in slot.iter_mut().rev() {
            *byte = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }

    let mut buf = *b"0000-00-00 00:00:00.000000000+00:00";
    put(&mut buf[0..4], local.year().clamp(0, 9999) as u32);
    put(&mut buf[5..7], local.month());
    put(&mut buf[8..10], local.day());
    put(&mut buf[11..13], local.hour());
    put(&mut buf[14..16], local.minute());
    put(&mut buf[17..19], local.second());
    put(&mut buf[20..29], sub);
    if offset < 0 {
        buf[29] = b'-';
    }
    let minutes = offset.unsigned_abs() / 60;
    put(&mut buf[30..32], minutes / 60);
    put(&mut buf[33..35], minutes % 60);
    Timestamp(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 2026-09-07 03:04:08.914293456 UTC
    fn nanos() -> u64 {
        let secs = Utc
            .with_ymd_and_hms(2026, 9, 7, 3, 4, 8)
            .unwrap()
            .timestamp() as u64;
        secs * 1_000_000_000 + 914_293_456
    }

    #[test]
    fn formats_wall_clock_with_offset() {
        assert_eq!(
            format_timestamp(nanos(), Tz::UTC).as_str(),
            "2026-09-07 03:04:08.914293456+00:00"
        );
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::Asia::Shanghai).as_str(),
            "2026-09-07 11:04:08.914293456+08:00"
        );
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::America::New_York).as_str(),
            "2026-09-06 23:04:08.914293456-04:00"
        );
        // 和 chrono 自己的格式化一致
        let expect = DateTime::<Utc>::from_timestamp(nanos() as i64 / 1_000_000_000, 914_293_456)
            .unwrap()
            .with_timezone(&chrono_tz::Asia::Shanghai)
            .format("%Y-%m-%d %H:%M:%S%.9f%:z")
            .to_string();
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::Asia::Shanghai).as_str(),
            expect
        );
        assert_eq!(
            format_timestamp(0, Tz::UTC).as_str(),
            "1970-01-01 00:00:00.000000000+00:00"
        );
    }

    #[test]
    fn serializes_flat_with_nested_arrays() {
        let mut event = SpanEvent {
            timestamp: nanos(),
            trace_id: "e89a476882236ce0f1186d1522c8f59f".into(),
            span_id: "e8b0e73e2132f21c".into(),
            span_name: "GET /orders".into(),
            span_kind: SpanKind::Server,
            service_name: Arc::from("order-service"),
            duration_ns: 12_345_678,
            status_code: StatusCode::Error,
            events: vec![TimedEvent {
                timestamp: nanos() + 1_000,
                name: "exception".into(),
                attributes: [(
                    "exception.type".to_owned(),
                    Value::from("IllegalStateException"),
                )]
                .into_iter()
                .collect(),
            }],
            ..Default::default()
        };
        event.shared = Some(Arc::new(
            [("cluster".to_owned(), Value::from("bj-prod"))]
                .into_iter()
                .collect(),
        ));
        event.insert("env", "prod");

        let json: Value = serde_json::from_str(&event.to_json_line().unwrap()).unwrap();
        assert_eq!(json["timestamp"], "2026-09-07 03:04:08.914293456+00:00");
        assert_eq!(json["span_kind"], "Server");
        assert_eq!(json["status_code"], "Error");
        assert_eq!(json["duration_ns"], 12_345_678);
        assert_eq!(
            json["events.timestamp"][0],
            "2026-09-07 03:04:08.914294456+00:00"
        );
        assert_eq!(json["events.name"][0], "exception");
        assert_eq!(
            json["events.attributes"][0]["exception.type"],
            "IllegalStateException"
        );
        assert_eq!(json["links.trace_id"], Value::Array(vec![]));
        assert_eq!(json["cluster"], "bj-prod");
        assert_eq!(json["env"], "prod");
        assert_eq!(event.get("cluster"), Some(&Value::from("bj-prod")));

        // 配了时区：墙上时间换算、偏移跟着变，其余不动
        let json: Value = serde_json::from_str(
            &serde_json::to_string(&WithZone {
                event: &event,
                tz: chrono_tz::Asia::Shanghai,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(json["timestamp"], "2026-09-07 11:04:08.914293456+08:00");
        assert_eq!(
            json["events.timestamp"][0],
            "2026-09-07 11:04:08.914294456+08:00"
        );
        assert!(event.estimated_size() > SpanEvent::default().estimated_size());
        assert!(event.to_text_line().contains("12.346ms"));
    }
}
