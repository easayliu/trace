//! YAML 配置：给可执行文件用。库使用者也可以直接用 `Pipeline::builder()` 拼装。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_yaml_ng::{Mapping, Value};

use crate::batch::{BatchConfig, RetryConfig};
use crate::error::{Error, Result};
use crate::pipeline::{OnError, Pipeline};
use crate::sink::console::Encoding;
use crate::sink::{ClickhouseSink, ConsoleSink};
use crate::source::otlp::{DEFAULT_GRPC_ADDR, DEFAULT_HTTP_ADDR, DEFAULT_MAX_REQUEST_BYTES};
use crate::source::{OtlpSource, Source, StdinSource};

#[derive(Debug)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub batch: BatchSettings,
    pub retry: RetrySettings,
    pub pipeline: PipelineSettings,
    /// 给每条 span 附加的静态字段，例如 `cluster = "bj-prod"`、`env = "prod"`。
    pub fields: BTreeMap<String, serde_json::Value>,
}

/// 从哪收 span。`type: otlp` 是正经部署用的；`type: stdin` 读 OTLP/JSON 行，调试用。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    /// OTLP 接收端：gRPC + HTTP。
    Otlp(OtlpSourceConfig),
    /// 从标准输入读 OTLP/JSON，一行一个导出请求。
    Stdin(StdinSourceConfig),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpSourceConfig {
    /// gRPC 监听地址，默认 `0.0.0.0:4317`。写 `null` 或空串关掉。
    #[serde(default = "default_grpc")]
    pub grpc: Option<String>,
    /// HTTP 监听地址，默认 `0.0.0.0:4318`。写 `null` 或空串关掉。
    #[serde(default = "default_http")]
    pub http: Option<String>,
    /// 单个请求（解压后）的大小上限，字节。
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,
    /// 下游队列满时最多等多久再回「稍后重试」，秒。
    #[serde(default = "five")]
    pub enqueue_timeout_secs: u64,
    /// 等数据真正写进存储再给客户端回成功。默认关。
    #[serde(default)]
    pub wait_for_write: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StdinSourceConfig {
    /// 攒多少行发一批。
    #[serde(default = "default_batch_lines")]
    pub batch_lines: usize,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Clickhouse {
        /// 形如 `http://127.0.0.1:8123`。
        endpoint: String,
        database: String,
        table: String,
        /// ClickHouse 集群名（`system.clusters` 里的，不是 k8s 集群，也和 `fields` 里
        /// 叫 cluster 的静态字段无关）。配了之后 `--ddl` 生成 `ReplicatedMergeTree`
        /// 本地表 + `Distributed` 表，两条都带 `ON CLUSTER`。
        cluster: Option<String>,
        /// 时间戳列的显示时区（IANA 名，如 `Asia/Shanghai`）。只影响 `--ddl` 建出来的
        /// 列类型；存的时刻总是对的，因为 INSERT 一律带偏移。和 logpipe 的日志表配成
        /// 一样的，两张表查出来的时间才是同一个口径。
        timezone: Option<String>,
        user: Option<String>,
        password: Option<String>,
        #[serde(default)]
        async_insert: bool,
        /// gzip 压缩 INSERT 请求体，默认开。只有中间代理不能正确转发压缩 body 时才关。
        #[serde(default = "yes")]
        compress: bool,
        #[serde(default = "thirty")]
        timeout_secs: u64,
    },
    Console {
        /// `json`（默认）或 `text`。
        #[serde(default)]
        encoding: ConsoleEncoding,
        #[serde(default)]
        stderr: bool,
    },
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleEncoding {
    #[default]
    Json,
    Text,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchSettings {
    #[serde(default = "default_max_events")]
    pub max_events: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "one")]
    pub timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySettings {
    #[serde(default = "five_attempts")]
    pub max_attempts: usize,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "thirty")]
    pub max_backoff_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSettings {
    /// source 与 sink 之间的队列深度（按批计）。
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// healthcheck 不通过就不启动。
    #[serde(default)]
    pub require_healthy: bool,
    /// 重试耗尽后：`stop`（默认）或 `drop`（丢掉继续跑）。
    #[serde(default)]
    pub on_error: OnErrorSetting,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnErrorSetting {
    #[default]
    Stop,
    Drop,
}

fn yes() -> bool {
    true
}
fn one() -> u64 {
    1
}
fn five() -> u64 {
    5
}
fn five_attempts() -> usize {
    5
}
fn thirty() -> u64 {
    30
}
fn default_grpc() -> Option<String> {
    Some(DEFAULT_GRPC_ADDR.to_owned())
}
fn default_http() -> Option<String> {
    Some(DEFAULT_HTTP_ADDR.to_owned())
}
fn default_max_request_bytes() -> usize {
    DEFAULT_MAX_REQUEST_BYTES
}
fn default_batch_lines() -> usize {
    500
}
fn default_max_events() -> usize {
    BatchConfig::default().max_events
}
fn default_max_bytes() -> usize {
    BatchConfig::default().max_bytes
}
fn default_initial_backoff_ms() -> u64 {
    RetryConfig::default().initial_backoff.as_millis() as u64
}
fn default_buffer() -> usize {
    64
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self {
            max_events: default_max_events(),
            max_bytes: default_max_bytes(),
            timeout_secs: one(),
        }
    }
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self {
            max_attempts: five_attempts(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_secs: thirty(),
        }
    }
}

impl Default for PipelineSettings {
    fn default() -> Self {
        Self {
            buffer: default_buffer(),
            require_healthy: false,
            on_error: OnErrorSetting::Stop,
        }
    }
}

fn required_section<T: DeserializeOwned>(mapping: &Mapping, name: &str) -> Result<T> {
    let value = mapping
        .get(name)
        .ok_or_else(|| Error::config(format!("缺少 `{name}` 配置")))?;
    section(value.clone(), name)
}

fn optional_section<T: DeserializeOwned + Default>(mapping: &Mapping, name: &str) -> Result<T> {
    match mapping.get(name) {
        // 整段留空（比如底下只有注释）就用默认值
        None | Some(Value::Null) => Ok(T::default()),
        Some(value) => section(value.clone(), name),
    }
}

fn section<T: DeserializeOwned>(value: Value, name: &str) -> Result<T> {
    serde_yaml_ng::from_value(value).map_err(|err| {
        let message = err.to_string();
        // source / sink 是按 type 分派的，字段填错多半是 type 和字段没对上
        let hint = if matches!(name, "source" | "sink") && message.contains("unknown field") {
            format!("；请检查 {name}.type 和下面的字段是否匹配")
        } else {
            String::new()
        };
        Error::config(format!("`{name}` 配置有问题: {message}{hint}"))
    })
}

/// 静态字段该建成什么列类型。
fn column_type_of(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(_) => "UInt8".to_owned(),
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "Int64".to_owned(),
        serde_json::Value::Number(_) => "Float64".to_owned(),
        // 字符串多是 cluster / env 这类枚举值，低基数列更省
        _ => "LowCardinality(String)".to_owned(),
    }
}

/// `null` / 空串都算关掉。
fn enabled(raw: &Option<String>) -> Option<&str> {
    raw.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

fn parse_addr(name: &str, raw: &str) -> Result<SocketAddr> {
    raw.parse().map_err(|_| {
        Error::config(format!(
            "source.{name} `{raw}` 不是合法的监听地址（例：0.0.0.0:4317）"
        ))
    })
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::config(format!("读取配置 {} 失败: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// 逐段解析。
    ///
    /// 不用一次性 derive 是为了让报错能点名是哪一段出了问题 —— `source` / `sink`
    /// 是按 `type` 分派的枚举，serde 会把整段缓冲起来再解析，行号就丢了，
    /// 只报一句 "unknown field `endpoint`" 很难定位。
    pub fn parse(text: &str) -> Result<Self> {
        const SECTIONS: [&str; 6] = ["source", "sink", "batch", "retry", "pipeline", "fields"];

        let root: Value = serde_yaml_ng::from_str(text)
            .map_err(|e| Error::config(format!("YAML 语法错误: {e}")))?;

        let mapping = match &root {
            Value::Mapping(mapping) => mapping,
            Value::Null => return Err(Error::config("配置是空的")),
            _ => return Err(Error::config("配置最外层应当是 key: value 形式")),
        };

        for key in mapping.keys() {
            let name = key.as_str().unwrap_or_default();
            if !SECTIONS.contains(&name) {
                return Err(Error::config(format!(
                    "未知的配置项 `{name}`；可用的有: {}",
                    SECTIONS.join(" / ")
                )));
            }
        }

        Ok(Self {
            source: required_section(mapping, "source")?,
            sink: required_section(mapping, "sink")?,
            batch: optional_section(mapping, "batch")?,
            retry: optional_section(mapping, "retry")?,
            pipeline: optional_section(mapping, "pipeline")?,
            fields: optional_section(mapping, "fields")?,
        })
    }

    /// ClickHouse sink 对应的建表语句，会带上 `fields` 里的静态字段列。
    pub fn ddl(&self) -> Result<String> {
        match &self.sink {
            SinkConfig::Clickhouse { .. } => Ok(self.build_clickhouse()?.create_table_ddl()),
            SinkConfig::Console { .. } => Err(Error::config("当前 sink 是 console，没有建表语句")),
        }
    }

    /// 除固定字段之外还会写入哪些列。
    fn extra_columns(&self) -> Vec<(String, String)> {
        self.fields
            .iter()
            .map(|(key, value)| (key.clone(), column_type_of(value)))
            .collect()
    }

    fn build_clickhouse(&self) -> Result<ClickhouseSink> {
        let SinkConfig::Clickhouse {
            endpoint,
            database,
            table,
            cluster,
            timezone,
            user,
            password,
            async_insert,
            compress,
            timeout_secs,
        } = &self.sink
        else {
            return Err(Error::config("sink 不是 clickhouse"));
        };

        let mut sink = ClickhouseSink::new(endpoint, database, table)
            .timeout(Duration::from_secs(*timeout_secs))
            .async_insert(*async_insert)
            .compress(*compress);
        if let Some(cluster) = cluster {
            sink = sink.cluster(cluster);
        }
        if let Some(timezone) = timezone {
            let tz: chrono_tz::Tz = timezone.parse().map_err(|_| {
                Error::config(format!(
                    "sink.timezone `{timezone}` 不是合法的 IANA 时区名（例：Asia/Shanghai）"
                ))
            })?;
            sink = sink.timezone(tz);
        }
        sink = sink.extra_columns(self.extra_columns());
        if let Some(user) = user {
            sink = sink.auth(user, password.clone().unwrap_or_default());
        }
        Ok(sink)
    }

    fn build_source(&self) -> Result<Box<dyn Source>> {
        let fields = self.fields.clone();
        Ok(match &self.source {
            SourceConfig::Otlp(otlp) => {
                let grpc = enabled(&otlp.grpc)
                    .map(|addr| parse_addr("grpc", addr))
                    .transpose()?;
                let http = enabled(&otlp.http)
                    .map(|addr| parse_addr("http", addr))
                    .transpose()?;
                if grpc.is_none() && http.is_none() {
                    return Err(Error::config("source.grpc 和 source.http 至少要开一个"));
                }

                let mut source = OtlpSource::new()
                    .no_grpc()
                    .no_http()
                    .max_request_bytes(otlp.max_request_bytes)
                    .enqueue_timeout(Duration::from_secs(otlp.enqueue_timeout_secs))
                    .wait_for_write(otlp.wait_for_write)
                    .fields(fields);
                if let Some(addr) = grpc {
                    source = source.grpc(addr);
                }
                if let Some(addr) = http {
                    source = source.http(addr);
                }
                Box::new(source)
            }
            SourceConfig::Stdin(stdin) => Box::new(
                StdinSource::new()
                    .batch_lines(stdin.batch_lines)
                    .fields(fields),
            ),
        })
    }

    /// 启动前的静态校验：配置能不能组装出组件。不绑端口、不连库。
    pub fn check(&self) -> Result<()> {
        self.build_source()?;
        if let SinkConfig::Clickhouse { .. } = &self.sink {
            self.build_clickhouse()?;
        }
        Ok(())
    }

    /// 按配置组装出一条可运行的 pipeline。
    pub fn build(self) -> Result<Pipeline> {
        let source = self.build_source()?;

        let builder = Pipeline::builder()
            .boxed_source(source)
            .batch(
                BatchConfig::default()
                    .max_events(self.batch.max_events)
                    .max_bytes(self.batch.max_bytes)
                    .timeout(Duration::from_secs(self.batch.timeout_secs)),
            )
            .retry(RetryConfig {
                max_attempts: self.retry.max_attempts.max(1),
                initial_backoff: Duration::from_millis(self.retry.initial_backoff_ms),
                max_backoff: Duration::from_secs(self.retry.max_backoff_secs),
            })
            .buffer(self.pipeline.buffer)
            .require_healthy(self.pipeline.require_healthy)
            .on_error(match self.pipeline.on_error {
                OnErrorSetting::Stop => OnError::Stop,
                OnErrorSetting::Drop => OnError::Drop,
            });

        Ok(match &self.sink {
            SinkConfig::Clickhouse { .. } => builder.sink(self.build_clickhouse()?).build()?,
            SinkConfig::Console { encoding, stderr } => {
                let sink = ConsoleSink::new(match encoding {
                    ConsoleEncoding::Json => Encoding::Json,
                    ConsoleEncoding::Text => Encoding::Text,
                });
                let sink = if *stderr { sink.stderr() } else { sink };
                builder.sink(sink).build()?
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otlp_source_needs_no_fields() {
        let config = Config::parse(
            r#"
source:
  type: otlp
sink:
  type: console
"#,
        )
        .unwrap();

        let SourceConfig::Otlp(otlp) = &config.source else {
            panic!("应当是 otlp source");
        };
        assert_eq!(otlp.grpc.as_deref(), Some("0.0.0.0:4317"));
        assert_eq!(otlp.http.as_deref(), Some("0.0.0.0:4318"));
        assert!(!otlp.wait_for_write);
        assert_eq!(config.batch.timeout_secs, 1);
        assert_eq!(config.pipeline.on_error, OnErrorSetting::Stop);
        config.check().unwrap();
        config.build().unwrap();
    }

    #[test]
    fn null_or_empty_disables_a_listener() {
        for (yaml, expect_grpc) in [
            ("grpc: null\n", false),
            ("grpc: \"\"\n", false),
            ("http: null\n", true),
        ] {
            let config = Config::parse(&format!(
                "source:\n  type: otlp\n  {yaml}sink:\n  type: console\n"
            ))
            .unwrap();
            let SourceConfig::Otlp(otlp) = &config.source else {
                panic!()
            };
            assert_eq!(enabled(&otlp.grpc).is_some(), expect_grpc, "{yaml}");
            config.check().unwrap();
        }

        let err = Config::parse(
            "source:\n  type: otlp\n  grpc: null\n  http: \"\"\nsink:\n  type: console\n",
        )
        .unwrap()
        .check()
        .expect_err("两个都关了应当报错");
        assert!(err.to_string().contains("至少要开一个"), "{err}");
    }

    #[test]
    fn rejects_bad_listen_address() {
        let err =
            Config::parse("source:\n  type: otlp\n  grpc: \"4317\"\nsink:\n  type: console\n")
                .unwrap()
                .check()
                .expect_err("没有 host 的地址应当报错");
        assert!(err.to_string().contains("source.grpc"), "{err}");
    }

    #[test]
    fn stdin_source_and_static_fields() {
        let config = Config::parse(
            r#"
source:
  type: stdin
sink:
  type: clickhouse
  endpoint: http://clickhouse:8123
  database: logs
  table: otel_trace
  timezone: Asia/Shanghai
fields:
  cluster: bj-prod
  replica: 3
"#,
        )
        .unwrap();
        assert!(matches!(config.source, SourceConfig::Stdin(_)));
        let ddl = config.ddl().unwrap();
        assert!(
            ddl.contains("`cluster`             LowCardinality(String)"),
            "{ddl}"
        );
        assert!(ddl.contains("`replica`             Int64"), "{ddl}");
        assert!(ddl.contains("DateTime64(9, 'Asia/Shanghai')"), "{ddl}");
        config.build().unwrap();
    }

    #[test]
    fn rejects_bad_timezone() {
        let err = Config::parse(
            "source:\n  type: otlp\nsink:\n  type: clickhouse\n  endpoint: http://x:8123\n  database: t\n  table: t\n  timezone: Asia/Beijing\n",
        )
        .unwrap()
        .check()
        .expect_err("Asia/Beijing 不是 IANA 时区名");
        assert!(err.to_string().contains("sink.timezone"), "{err}");
    }

    #[test]
    fn names_the_section_and_hints_type_mismatch() {
        let err = Config::parse(
            r#"
source:
  type: otlp
sink:
  type: console
  endpoint: http://127.0.0.1:8123
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`sink` 配置有问题"), "{err}");
        assert!(err.contains("unknown field `endpoint`"), "{err}");
        assert!(err.contains("sink.type"), "{err}");

        let err = Config::parse("source:\n  type: kafka\nsink:\n  type: console\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("`source` 配置有问题"), "{err}");
    }

    #[test]
    fn rejects_unknown_top_level_section() {
        let err = Config::parse("source:\n  type: otlp\nsink:\n  type: console\nparser: {}\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("未知的配置项 `parser`"), "{err}");
    }

    #[test]
    fn reports_missing_section() {
        let err = Config::parse("source:\n  type: otlp\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("缺少 `sink` 配置"), "{err}");
    }
}
