//! ClickHouse 入库：HTTP 接口 + `JSONEachRow` 批量插入。
//!
//! 建表语句见 [`ClickhouseSink::create_table_ddl`]，列与 [`SpanEvent`] 一一对应，
//! 布局参考 OTel collector 的 clickhouse exporter（`otel_traces`），列名改成 snake_case
//! 好和 logpipe 的日志表一起查。

use std::io::Write;
use std::time::Duration;

use async_trait::async_trait;
use chrono_tz::Tz;

use crate::error::{Error, Result};
use crate::event::{SpanEvent, WithZone};
use crate::sink::Sink;

/// `timestamp` 之后、`events.*` 之前的固定列。两个时间戳列（`timestamp` /
/// `events.timestamp`）的类型跟着 `timezone` 走，不在这里。
const COLUMNS_HEAD: [(&str, &str); 14] = [
    ("trace_id", "String"),
    ("span_id", "String"),
    ("parent_span_id", "String"),
    ("trace_state", "String"),
    ("span_name", "LowCardinality(String)"),
    ("span_kind", "LowCardinality(String)"),
    ("service_name", "LowCardinality(String)"),
    ("duration_ns", "UInt64"),
    ("status_code", "LowCardinality(String)"),
    ("status_message", "String"),
    ("scope_name", "LowCardinality(String)"),
    ("scope_version", "LowCardinality(String)"),
    ("resource_attributes", "Map(LowCardinality(String), String)"),
    ("span_attributes", "Map(LowCardinality(String), String)"),
];

/// `events.timestamp` 之后的固定列。events / links 用 `Nested` 的平铺写法：几个等长
/// 数组，列名带点。
const COLUMNS_TAIL: [(&str, &str); 6] = [
    ("events.name", "Array(LowCardinality(String))"),
    (
        "events.attributes",
        "Array(Map(LowCardinality(String), String))",
    ),
    ("links.trace_id", "Array(String)"),
    ("links.span_id", "Array(String)"),
    ("links.trace_state", "Array(String)"),
    (
        "links.attributes",
        "Array(Map(LowCardinality(String), String))",
    ),
];

/// 跳数索引。按 trace id 反查（Jaeger / 日志表拿到一个 id 过来）不带时间范围，排序键
/// 帮不上忙，bloom filter 让这种查询跳过绝大多数 granule；属性的 key / value 索引是
/// 「找带某个 tag 的 span」用的，和 OTel exporter 建的一样。
const INDEXES: [(&str, &str); 6] = [
    ("idx_trace_id", "`trace_id` TYPE bloom_filter GRANULARITY 4"),
    (
        "idx_res_attr_key",
        "mapKeys(`resource_attributes`) TYPE bloom_filter(0.01) GRANULARITY 1",
    ),
    (
        "idx_res_attr_value",
        "mapValues(`resource_attributes`) TYPE bloom_filter(0.01) GRANULARITY 1",
    ),
    (
        "idx_span_attr_key",
        "mapKeys(`span_attributes`) TYPE bloom_filter(0.01) GRANULARITY 1",
    ),
    (
        "idx_span_attr_value",
        "mapValues(`span_attributes`) TYPE bloom_filter(0.01) GRANULARITY 1",
    ),
    ("idx_duration", "`duration_ns` TYPE minmax GRANULARITY 1"),
];

pub struct ClickhouseSink {
    client: reqwest::Client,
    endpoint: String,
    database: String,
    table: String,
    cluster: Option<String>,
    timezone: Option<Tz>,
    /// 固定列之外还要有的列（配置里的静态字段），建表和启动校验都用。
    extra_columns: Vec<(String, String)>,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
    async_insert: bool,
    compress: bool,
}

impl ClickhouseSink {
    /// `endpoint` 形如 `http://127.0.0.1:8123`。
    pub fn new(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            database: database.into(),
            table: table.into(),
            cluster: None,
            timezone: None,
            extra_columns: Vec::new(),
            user: None,
            password: None,
            timeout: Duration::from_secs(30),
            async_insert: false,
            compress: true,
        }
    }

    /// ClickHouse 集群名（`system.clusters` 里的那个，不是 k8s 集群）。
    ///
    /// 配了之后建表语句变成两张表：`<table>_local` 是 `ReplicatedMergeTree`，带
    /// `ON CLUSTER` 一次性下发到所有节点；`<table>` 是它上面的 `Distributed`，
    /// 也就是 sink 实际写入的那张。写入路径本身不受影响 —— 还是往 `table` 里 INSERT。
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    /// 时间戳列的显示时区，比如 `Asia/Shanghai`。
    ///
    /// span 的时间是绝对时刻（UNIX 纳秒），INSERT 时总是带着偏移写出去
    /// （`2026-09-07 11:04:08.914293456+08:00`），存进去的时刻不依赖列的时区。这个
    /// 选项只影响 `--ddl`：时间戳列建成 `DateTime64(9, 'Asia/Shanghai')`，查出来显示的
    /// 是北京时间而不是服务端时区。要和 logpipe 的日志表（同样配了 timezone）按同一个
    /// 墙上时间对照着看就配上。
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.timezone = Some(timezone);
        self
    }

    /// 固定列之外的列：配置里的静态字段。`--ddl` 建出来，healthcheck 时校验表里确实有。
    pub fn extra_columns(mut self, columns: Vec<(String, String)>) -> Self {
        self.extra_columns = columns;
        self
    }

    pub fn auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 打开后由 ClickHouse 服务端再攒一层批，适合多实例小批量写入的场景。
    pub fn async_insert(mut self, enabled: bool) -> Self {
        self.async_insert = enabled;
        self
    }

    /// 是否 gzip 压缩 INSERT 的请求体，默认开。
    ///
    /// span 的 JSON 里属性 key 大量重复，压得动。关掉它一般只有一个理由：中间的
    /// 代理/网关不能正确转发压缩过的 body。
    pub fn compress(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    fn timestamp_type(&self) -> String {
        match &self.timezone {
            Some(tz) => format!("DateTime64(9, '{}')", tz.name()),
            None => "DateTime64(9)".to_owned(),
        }
    }

    /// 全部固定列，按建表顺序。
    fn base_columns(&self) -> Vec<(String, String)> {
        let ts = self.timestamp_type();
        let mut columns = vec![("timestamp".to_owned(), ts.clone())];
        columns.extend(
            COLUMNS_HEAD
                .iter()
                .map(|(name, ty)| ((*name).to_owned(), (*ty).to_owned())),
        );
        columns.push(("events.timestamp".to_owned(), format!("Array({ts})")));
        columns.extend(
            COLUMNS_TAIL
                .iter()
                .map(|(name, ty)| ((*name).to_owned(), (*ty).to_owned())),
        );
        columns
    }

    /// 建表 + 补表的语句，直接拿去执行即可，重复执行也没事。
    ///
    /// 第一段 `CREATE TABLE IF NOT EXISTS` 管新表；后面的 `ALTER TABLE` 全是
    /// `ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS` / `MODIFY COLUMN`
    /// 这类幂等操作，管老表：配置里新加了 `fields`、后来配了 `timezone`，重跑一次就把
    /// 差异补齐，不用人手对着表结构写 ALTER。集群模式下 `_local` 表和 `Distributed`
    /// 表各补一遍，`Distributed` 不会自动跟着本地表变列，而且它不支持跳数索引。
    ///
    /// 不碰的：排序键、分区键改不了；TTL 能改但 `MODIFY TTL` 会触发重算；已有列的类型
    /// 变了（静态字段从整数改成字符串）`IF NOT EXISTS` 会跳过 —— 这几种本来就该人看
    /// 一眼再动。
    pub fn create_table_ddl(&self) -> String {
        self.create_table_ddl_with(&self.extra_columns)
    }

    /// 同上，额外追加几列而不用 [`Self::extra_columns`]。
    pub fn create_table_ddl_with(&self, extra: &[(String, String)]) -> String {
        let timestamp_type = self.timestamp_type();
        let mut columns = self.base_columns();
        for (name, ty) in extra {
            if !columns.iter().any(|(existing, _)| existing == name) {
                columns.push((name.clone(), ty.clone()));
            }
        }

        let width = columns
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0);
        let pad = |name: &str| " ".repeat(width - name.len());
        let mut body: Vec<String> = columns
            .iter()
            .map(|(name, ty)| format!("    `{name}`{} {ty}", pad(name)))
            .collect();
        body.extend(
            INDEXES
                .iter()
                .map(|(name, expr)| format!("    INDEX `{name}` {expr}")),
        );
        let body = body.join(",\n");

        let layout = "PARTITION BY toDate(`timestamp`)\n\
             ORDER BY (`service_name`, `span_name`, toDateTime(`timestamp`))\n\
             TTL toDateTime(`timestamp`) + INTERVAL 30 DAY";
        let db = &self.database;
        let table = &self.table;
        let modify_timestamps: Vec<String> = match self.timezone {
            Some(_) => vec![
                format!("MODIFY COLUMN `timestamp` {timestamp_type}"),
                format!("MODIFY COLUMN `events.timestamp` Array({timestamp_type})"),
            ],
            None => Vec::new(),
        };

        let alter = |target: &str, on_cluster: &str, with_index: bool| {
            // timestamp 排第一、在排序键里，一定存在，不 ADD
            let mut actions: Vec<String> = columns
                .iter()
                .filter(|(name, _)| name != "timestamp")
                .map(|(name, ty)| format!("ADD COLUMN IF NOT EXISTS `{name}` {ty}"))
                .collect();
            if with_index {
                actions.extend(
                    INDEXES
                        .iter()
                        .map(|(name, expr)| format!("ADD INDEX IF NOT EXISTS `{name}` {expr}")),
                );
            }
            actions.extend(modify_timestamps.iter().cloned());
            format!(
                "ALTER TABLE `{db}`.`{target}`{on_cluster}\n    {}",
                actions.join(",\n    ")
            )
        };

        let Some(cluster) = &self.cluster else {
            return format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.`{table}`\n\
                 (\n{body}\n)\n\
                 ENGINE = MergeTree\n{layout};\n\n{}",
                alter(table, "", true)
            );
        };

        // 集群：本地表存数据，Distributed 表负责分发，全部 ON CLUSTER 一次下发。
        // `{shard}` / `{replica}` 是 ClickHouse 自己的宏，由各节点的 macros 配置展开。
        // 补列先补本地表再补 Distributed 表：反过来的话中间那一瞬间往 Distributed 表
        // 插新列会因为本地表没有而失败。分片键用 trace_id 的 hash：同一条 trace 的
        // span 落同一个分片，按 trace id 查不用跨分片。
        let local = self.local_table();
        let on_cluster = format!(" ON CLUSTER `{cluster}`");
        format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{local}`{on_cluster}\n\
             (\n{body}\n)\n\
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{db}/{local}', '{{replica}}')\n\
             {layout};\n\n\
             CREATE TABLE IF NOT EXISTS `{db}`.`{table}`{on_cluster}\n\
             AS `{db}`.`{local}`\n\
             ENGINE = Distributed(`{cluster}`, `{db}`, `{local}`, cityHash64(`trace_id`));\n\n\
             {};\n\n{}",
            alter(&local, &on_cluster, true),
            alter(table, &on_cluster, false)
        )
    }

    /// 表里必须有的列名：固定列 + 额外列。
    fn expected_columns(&self) -> Vec<String> {
        self.base_columns()
            .into_iter()
            .map(|(name, _)| name)
            .chain(self.extra_columns.iter().map(|(name, _)| name.clone()))
            .collect()
    }

    /// 集群模式下真正存数据的本地表名：`<table>_local`。
    pub fn local_table(&self) -> String {
        format!("{}_local", self.table)
    }

    /// 执行任意 SQL（建表、查询都可以）。
    pub async fn execute(&self, sql: &str) -> Result<String> {
        self.request(sql, Vec::new()).await
    }

    async fn request(&self, sql: &str, body: Vec<u8>) -> Result<String> {
        let mut settings: Vec<(&str, &str)> = vec![
            ("query", sql),
            // 时间戳按 `2026-09-07 03:04:08.914293456+00:00` 发送，要开宽松解析
            ("date_time_input_format", "best_effort"),
            // 事件里的自定义字段可能没有对应列，跳过而不是整批失败。
            ("input_format_skip_unknown_fields", "1"),
        ];
        if self.async_insert {
            settings.push(("async_insert", "1"));
            settings.push(("wait_for_async_insert", "1"));
        }

        // 空 body（`SELECT 1`、`EXISTS TABLE` 这些健康检查）不压：gzip 一个空串反而
        // 会多出十几个字节的头，而这里正是 411 那个坑所在，保持原样最稳。
        let compressed = self.compress && !body.is_empty();
        let body = if compressed { gzip(&body)? } else { body };

        // Content-Length 必须自己写。body 为空时 hyper 认为流已经结束，既不发
        // Content-Length 也不用 chunked，而 ClickHouse 见到这样的 POST 直接回
        // 411 Length Required。
        let mut request = self
            .client
            .post(&self.endpoint)
            .query(&settings)
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body);

        if compressed {
            request = request.header(reqwest::header::CONTENT_ENCODING, "gzip");
        }

        if let (Some(user), Some(password)) = (&self.user, &self.password) {
            request = request
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password);
        }

        let response = request.send().await.map_err(Error::sink)?;
        let status = response.status();
        let text = response.text().await.map_err(Error::sink)?;

        if !status.is_success() {
            return Err(Error::Sink(
                format!("ClickHouse 返回 {status}: {}", text.trim()).into(),
            ));
        }
        Ok(text)
    }
}

/// SQL 字符串字面量转义：库名表名来自配置，反引号标识符走的是另一套规则，这里只管
/// `WHERE database = '...'` 里的单引号串。
fn escape_literal(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

/// 压缩请求体。ClickHouse 见到 `Content-Encoding: gzip` 会自己解开，服务端不用开
/// 任何设置 —— `enable_http_compression` 管的是响应方向，跟这里无关。
///
/// 压缩级别取最快的那一档：多压那百分之十几的体积要多花几倍 CPU，不划算。
fn gzip(body: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(body.len() / 8),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(body)
        .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err))?;
    encoder
        .finish()
        .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err))
}

#[async_trait]
impl Sink for ClickhouseSink {
    async fn write(&mut self, events: &[SpanEvent]) -> Result<()> {
        // 时间戳一律带偏移；没配时区就按 UTC 换算
        let tz = self.timezone.unwrap_or(Tz::UTC);
        let mut body: Vec<u8> = Vec::with_capacity(events.len() * 512);
        for event in events {
            serde_json::to_writer(&mut body, &WithZone { event, tz })?;
            body.push(b'\n');
        }

        let sql = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
            self.database, self.table
        );
        self.request(&sql, body).await?;

        tracing::debug!(count = events.len(), table = %self.table, "已写入 ClickHouse");
        Ok(())
    }

    async fn healthcheck(&self) -> Result<()> {
        self.execute("SELECT 1").await?;

        let exists = self
            .execute(&format!(
                "EXISTS TABLE `{}`.`{}`",
                self.database, self.table
            ))
            .await?;
        if exists.trim() != "1" {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 不存在，执行 `tracepipe --ddl` 输出的语句建表",
                    self.database, self.table
                )
                .into(),
            ));
        }

        // 列齐不齐也要查。INSERT 带着 input_format_skip_unknown_fields=1，表里没有的
        // 字段不报错、整批也不失败，只是那个字段悄悄没了。启动时对一遍，缺了直接说清楚。
        let present = self
            .execute(&format!(
                "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT TSV",
                escape_literal(&self.database),
                escape_literal(&self.table)
            ))
            .await?;
        let present: std::collections::HashSet<&str> = present
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        let missing: Vec<String> = self
            .expected_columns()
            .into_iter()
            .filter(|name| !present.contains(name.as_str()))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 缺列 {}：表结构没跟上配置，这些字段插入时会被静默丢掉。\
                     重跑 `tracepipe --ddl` 输出的语句（ddl Job）即可补齐",
                    self.database,
                    self.table,
                    missing.join(", ")
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_lists_every_column_and_index() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_trace");
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.starts_with("CREATE TABLE IF NOT EXISTS `logs`.`otel_trace`"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`timestamp`           DateTime64(9),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`events.timestamp`    Array(DateTime64(9)),"),
            "{ddl}"
        );
        for (name, _) in COLUMNS_HEAD.iter().chain(COLUMNS_TAIL.iter()) {
            assert!(
                ddl.contains(&format!("`{name}`")),
                "DDL 少了 {name}:\n{ddl}"
            );
        }
        for (name, _) in INDEXES {
            assert!(ddl.contains(&format!("INDEX `{name}`")), "{ddl}");
            assert!(
                ddl.contains(&format!("ADD INDEX IF NOT EXISTS `{name}`")),
                "{ddl}"
            );
        }
        assert!(ddl.contains("ORDER BY (`service_name`, `span_name`, toDateTime(`timestamp`))"));
        assert!(
            !ddl.contains("ADD COLUMN IF NOT EXISTS `timestamp`"),
            "{ddl}"
        );
        assert!(
            ddl.contains("ADD COLUMN IF NOT EXISTS `events.timestamp`"),
            "{ddl}"
        );
        assert!(!ddl.contains("MODIFY COLUMN"), "没配时区别去动列: {ddl}");
        // main 会在末尾补分号，这里不能自带
        assert!(!ddl.trim_end().ends_with(';'), "{ddl}");
    }

    #[test]
    fn timezone_changes_both_timestamp_columns() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_trace")
            .timezone(chrono_tz::Asia::Shanghai);
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.contains("`timestamp`           DateTime64(9, 'Asia/Shanghai'),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`events.timestamp`    Array(DateTime64(9, 'Asia/Shanghai')),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("MODIFY COLUMN `timestamp` DateTime64(9, 'Asia/Shanghai')"),
            "{ddl}"
        );
        assert!(
            ddl.contains("MODIFY COLUMN `events.timestamp` Array(DateTime64(9, 'Asia/Shanghai'))"),
            "{ddl}"
        );
    }

    #[test]
    fn cluster_ddl_is_replicated_plus_distributed() {
        let sink = ClickhouseSink::new("http://ck-lb:8123", "logs", "otel_trace")
            .cluster("bj_ck")
            .extra_columns(vec![(
                "cluster".to_owned(),
                "LowCardinality(String)".to_owned(),
            )]);
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.contains("`logs`.`otel_trace_local` ON CLUSTER `bj_ck`"),
            "{ddl}"
        );
        assert!(ddl.contains("ENGINE = ReplicatedMergeTree"), "{ddl}");
        assert!(
            ddl.contains(
                "Distributed(`bj_ck`, `logs`, `otel_trace_local`, cityHash64(`trace_id`))"
            ),
            "{ddl}"
        );
        assert_eq!(ddl.matches("CREATE TABLE").count(), 2, "{ddl}");
        assert_eq!(ddl.matches("ALTER TABLE").count(), 2, "{ddl}");
        assert_eq!(ddl.matches(";\n").count(), 3, "{ddl}");
        // 索引只加在本地表上，Distributed 不支持跳数索引
        assert_eq!(
            ddl.matches("ADD INDEX IF NOT EXISTS `idx_trace_id`")
                .count(),
            1,
            "{ddl}"
        );
        assert_eq!(
            ddl.matches("ADD COLUMN IF NOT EXISTS `cluster`").count(),
            2,
            "{ddl}"
        );
        let local_alter = ddl.find("ALTER TABLE `logs`.`otel_trace_local`").unwrap();
        let dist_alter = ddl.find("ALTER TABLE `logs`.`otel_trace` ON").unwrap();
        assert!(
            local_alter < dist_alter,
            "先补本地表再补 Distributed 表:\n{ddl}"
        );
        assert!(
            ddl.contains("{shard}") && ddl.contains("{replica}"),
            "{ddl}"
        );
    }
}
