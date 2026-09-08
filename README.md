# tracepipe

一个精简的 OTLP trace 采集入库框架，和 [logpipe](../log) 同一套骨架（source → pipeline → sink），
只保留我们需要的部分：**接收应用上报的 OpenTelemetry trace，拍平成一行一个 span，批量写进 ClickHouse。**

```text
  OTel SDK / agent        ┌────────────┐        ┌──────────────────────┐        ┌────────────┐
  ─ OTLP/gRPC :4317 ─────▶│   Source   │ Batch  │       Pipeline       │ 批量写  │    Sink    │
  ─ OTLP/HTTP :4318 ─────▶│ OTLP 接收端  ├───────▶│ 攒批 / 重试 / 背压     ├───────▶│ ClickHouse │
                          │ stdin      │        │ 优雅退出              │  ack   │ Console    │
                          └────────────┘        └──────────────────────┘◀───────└────────────┘
                                ▲                                         wait_for_write 时
                                └── 队列满回 UNAVAILABLE / 503，SDK 自己重发      落库后才回成功
```

和 logpipe 的分工：logpipe 采日志、tracepipe 采 trace，两边落的 `trace_id` 是同一个写法
（32 位小写 hex），一张表里拿到 id 到另一张表直接 `where trace_id = ...`。

## 数据模型

OTLP 里一个导出请求是 `ResourceSpans → ScopeSpans → Span` 三层，落库时拍平：每个 span 一行，
resource / scope 的信息复制到每一行上。列的布局参考 OTel collector 的
[clickhouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
（`otel_traces` 表），列名改成 snake_case 好和 logpipe 的 `app_log` 一起查。

| 列 | 来源 | 说明 |
| --- | --- | --- |
| `timestamp` | `start_time_unix_nano` | span 开始时间，纳秒精度 |
| `trace_id` / `span_id` / `parent_span_id` | 原始字节 | 小写 hex：32 / 16 / 16 位，根 span 的 parent 为空串 |
| `trace_state` | `trace_state` | W3C tracestate 原文 |
| `span_name` | `name` | `GET /orders/{id}`、`SELECT orders` |
| `span_kind` | `kind` | `Server` / `Client` / `Internal` / `Producer` / `Consumer` / `Unspecified` |
| `service_name` | resource 的 `service.name` | 没有则 `unknown_service` |
| `duration_ns` | `end - start` | 纳秒；时钟回拨造成 end < start 时记 0 |
| `status_code` / `status_message` | `status` | `Unset` / `Ok` / `Error` |
| `scope_name` / `scope_version` | scope | 比如 `io.opentelemetry.tomcat-10.0` / `2.9.0` |
| `resource_attributes` | resource | `JSON`，每个 key 一个带类型的子列，见下面「属性列」 |
| `span_attributes` | span | `JSON` |
| `events.*` | span events | `Nested`：`timestamp` / `name` / `attributes` 三个等长数组，异常栈在这里 |
| `links.*` | span links | `Nested`：`trace_id` / `span_id` / `trace_state` / `attributes` |

属性保留 OTLP 里的类型：字符串、整数、小数、布尔原样，bytes 转 base64 串，数组和嵌套对象就是
JSON 数组 / 对象，没有值的属性记 `null`。`span_kind` / `status_code` 的取值沿用 exporter 的写法，
给 `otel_traces` 写的 Grafana 面板和查询改个列名就能套。

### 属性列

四个属性列（`resource_attributes` / `span_attributes` / `events.attributes` / `links.attributes`）
是 ClickHouse 的 `JSON` 类型，**需要 ClickHouse 25.3+**（JSON 类型在 25.3 转正）。v0.1 用的是
`Map(String, String)`，换成 JSON 的原因：Map 查一个 key 要把整个 map 列解压出来再取值，属性多的
span 一行几 KB，扫一小时就是几十 GB 的解压；JSON 列里每个 key 是独立的子列，查
`span_attributes.http.route` 只读这一个子列，而且值带类型，`status_code` 就是整数。

写法要点：

* **key 里的点就是路径分隔**：`http.response.status_code` 存成 `http` → `response` → `status_code`，
  查询写 `span_attributes.http.response.status_code`，查出来整列显示也是嵌套的。这是 ClickHouse
  JSON 类型的规则，和 OTel exporter 的 JSON 表一样。
* **同一个 key 类型不一致时按类型取**：Java agent 给 `http.response.status_code` 发整数，某个自己
  埋点的服务发字符串 `"200"`，直接 `= 500` 会报 `NO_COMMON_TYPE`。写
  `span_attributes.http.response.status_code.:Int64 = 500` 只取整数那部分（字符串的行当 NULL），
  或者 `toString(...) = '500'` 两边都要。
* **跳数索引建在带类型的子列上**：`--ddl` 不预建属性索引（Map 时代的 `mapKeys` / `mapValues`
  bloom filter 没有了），哪些 key 值得建由查询决定，自己加：

  ```sql
  ALTER TABLE logs.otel_trace
      ADD INDEX idx_http_route span_attributes.http.route.:String TYPE bloom_filter(0.01) GRANULARITY 1,
      ADD INDEX idx_http_status span_attributes.http.response.status_code.:Int64 TYPE minmax GRANULARITY 1;
  ```

  直接在 `span_attributes.http.route` 上建会被拒（`Unexpected type Dynamic of bloom filter index`），
  必须带 `.:类型`。历史 part 要 `MATERIALIZE INDEX` 才有。
* **路径数上限**：JSON 列默认最多 1024 个不同路径，超过的进「共享数据」区，还能查但退化成整列
  解析。span 属性 key 是 SDK 语义约定的，一般几百个；确认要更多就改 `--ddl` 输出里的类型：
  `` `span_attributes` JSON(max_dynamic_paths = 4096) ``，healthcheck 认带参数的写法。别把用户
  id 这种无限多的值当 key 用。
* **要 Map 的地方**（Grafana 的 trace 视图 tags 列）用下面这段拍平，key 保持带点的原样，值转成
  字符串，支持四层以内的路径：

  ```sql
  mapFromArrays(JSONAllPaths(span_attributes), arrayMap(p -> (
      splitByChar('.', p) AS k, toJSONString(span_attributes) AS s,
      multiIf(length(k) = 1, JSONExtractRaw(s, k[1]),
              length(k) = 2, JSONExtractRaw(s, k[1], k[2]),
              length(k) = 3, JSONExtractRaw(s, k[1], k[2], k[3]),
              JSONExtractRaw(s, k[1], k[2], k[3], k[4])) AS raw,
      if(startsWith(raw, '"'), JSONExtractString(raw), raw)).4, JSONAllPaths(span_attributes))) AS tags
  ```

**从 v0.1 的 Map 表迁过来**：`ADD COLUMN IF NOT EXISTS` 见列名已存在会跳过，`--ddl` 补不了类型，
启动时 healthcheck 会点名哪几列还是 Map。首选直接 `DROP TABLE logs.otel_trace`（集群模式连
`otel_trace_local` 一起，都带 `ON CLUSTER`）再重跑 `--ddl`：trace 是 30 天 TTL 的运维数据，老表里
的东西一个月后本来就没了，为它多养一套 Map 写法的查询和面板不值。切换期间 collector 还在双写
Jaeger 的话，那边也还能查。确实要留老数据就 `RENAME TABLE logs.otel_trace TO logs.otel_trace_v1`
再建新表，老表按老写法查到过期。不建议 `MODIFY COLUMN ... JSON` 原地转：重写整列的 mutation
线上风险最大，转完值还是字符串，类型的好处也没拿到。

**时间戳一律带偏移写入**（`2026-09-07 11:04:08.914293456+08:00`）：span 的时间本来就是绝对时刻，
存进去的时刻不依赖列有没有标时区。`sink.timezone` 只影响 `--ddl` 建出来的列类型（`DateTime64(9,
'Asia/Shanghai')`），也就是查出来显示成几点。和 logpipe 的日志表配成同一个时区，两张表按
`trace_id` 对照时才是同一口径的时间。

## 启动

有两种用法：直接跑二进制（读 YAML 配置），或者当库用（拓扑写在代码里）。

```bash
cp tracepipe.yaml /etc/tracepipe.yaml       # 仓库根目录有带注释的示例配置
cargo run --release -- --ddl /etc/tracepipe.yaml | clickhouse-client   # 建表
cargo run --release -- --check /etc/tracepipe.yaml                     # 只校验配置
cargo run --release -- /etc/tracepipe.yaml                             # 启动
RUST_LOG=debug cargo run -- /etc/tracepipe.yaml                        # 看每批收发
```

不带参数时读当前目录的 `tracepipe.yaml`。`Ctrl-C` / SIGTERM 是优雅退出：停止收新请求、
手上的数据先写完再退。

最小配置（先用 console sink 看收到的 span，不用连库）：

```yaml
source:
  type: otlp        # gRPC 0.0.0.0:4317 + HTTP 0.0.0.0:4318，都是 SDK 的默认端口

sink:
  type: console
  encoding: json    # 或 text：一行一个 span 的摘要
```

然后把应用指过来，Java agent 的话：

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://<tracepipe>:4317 \
OTEL_SERVICE_NAME=order-service \
java -javaagent:opentelemetry-javaagent.jar -jar app.jar
```

换成入库只要改 `sink`：

```yaml
sink:
  type: clickhouse
  endpoint: http://127.0.0.1:8123
  database: logs
  table: otel_trace
  timezone: Asia/Shanghai
  user: default
  password: ""

fields:            # 附加到每条 span 的静态字段
  cluster: bj-prod
  env: prod
```

完整可配项（`source` / `sink` / `batch` / `retry` / `pipeline` / `fields`）见 `tracepipe.yaml`
里的注释；写错的键会在启动时直接报错，不会静默忽略。

### 接收端的行为

| 项 | 默认 | 说明 |
| --- | --- | --- |
| `grpc` / `http` | `0.0.0.0:4317` / `0.0.0.0:4318` | 写 `null` 或空串关掉其中一个；两个都关启动报错 |
| 编码 | protobuf + JSON | HTTP 按 `Content-Type` 分派（`application/x-protobuf` / `application/json`）；gRPC 只有 protobuf |
| 压缩 | gzip | 两个入口都收 `gzip`（SDK 的 `OTEL_EXPORTER_OTLP_COMPRESSION=gzip`），解压后仍按大小上限卡 |
| `max_request_bytes` | 16 MiB | 单个请求（解压后）的上限，超过回 `PAYLOAD_TOO_LARGE` |
| `enqueue_timeout_secs` | 5 | 下游队列满时最多等多久，超时回 gRPC `UNAVAILABLE` / HTTP `503 + Retry-After` |
| `wait_for_write` | `false` | 见下面「投递语义」 |

拒收一律回「可重试」的状态码，这是 OTLP 规定的应答方式：SDK 自带指数退避重发，比把数据堆在
接收端内存里稳。坏请求（解析不了、不认的 Content-Type）回 400 / 415，SDK 不会重发。

## 当库用

```rust
use tracepipe::{Pipeline, sink::ClickhouseSink, source::OtlpSource};

#[tokio::main]
async fn main() -> tracepipe::Result<()> {
    Pipeline::builder()
        .source(OtlpSource::new().wait_for_write(true))
        .sink(
            ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_trace")
                .timezone(chrono_tz::Asia::Shanghai),
        )
        .require_healthy(true)
        .build()?
        .run()          // 跑到 Ctrl-C；手上的数据会先写完再退出
        .await
}
```

`examples/` 下有两个直接跑的例子：

```bash
cargo run --example otlp_to_console -- text
cargo run --example otlp_to_clickhouse
```

## 表结构

字段由程序定义（上面那些，外加 `fields` 里的静态字段），**建表由你自己执行**，采集进程不执行
任何 DDL —— 分区键、排序键、TTL、引擎这些线上细节留在你手里。启动时只做校验：
`require_healthy: true` 的情况下会 `SELECT 1` + `EXISTS TABLE` + 对一遍 `system.columns`，
表不存在或者**缺列就直接报错退出**，报错里写清缺哪几列、怎么补。

```bash
cargo run -- --ddl tracepipe.yaml | clickhouse-client
```

```sql
CREATE TABLE IF NOT EXISTS `logs`.`otel_trace`
(
    `timestamp`           DateTime64(9, 'Asia/Shanghai'),
    `trace_id`            String,
    `span_id`             String,
    `parent_span_id`      String,
    `trace_state`         String,
    `span_name`           LowCardinality(String),
    `span_kind`           LowCardinality(String),
    `service_name`        LowCardinality(String),
    `duration_ns`         UInt64,
    `status_code`         LowCardinality(String),
    `status_message`      String,
    `scope_name`          LowCardinality(String),
    `scope_version`       LowCardinality(String),
    `resource_attributes` JSON,
    `span_attributes`     JSON,
    `events.timestamp`    Array(DateTime64(9, 'Asia/Shanghai')),
    `events.name`         Array(LowCardinality(String)),
    `events.attributes`   Array(JSON),
    `links.trace_id`      Array(String),
    `links.span_id`       Array(String),
    `links.trace_state`   Array(String),
    `links.attributes`    Array(JSON),
    `cluster`             LowCardinality(String),
    INDEX `idx_trace_id` `trace_id` TYPE bloom_filter GRANULARITY 4,
    INDEX `idx_duration` `duration_ns` TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(`timestamp`)
ORDER BY (`service_name`, `span_name`, toDateTime(`timestamp`))
TTL toDateTime(`timestamp`) + INTERVAL 30 DAY;

ALTER TABLE `logs`.`otel_trace`
    ADD COLUMN IF NOT EXISTS `trace_id` String,
    ...
    ADD INDEX IF NOT EXISTS `idx_trace_id` `trace_id` TYPE bloom_filter GRANULARITY 4,
    ...
    MODIFY COLUMN `events.timestamp` Array(DateTime64(9, 'Asia/Shanghai'));
```

两段：`CREATE TABLE IF NOT EXISTS` 管新表，后面的 `ALTER TABLE` 管老表 —— 全是 `IF NOT EXISTS`
这类幂等操作，新表上跑是空转，老表上跑就把差异补齐。所以**表结构变了重跑一遍就行**。

唯一补不了的是 `timestamp` 列的时区：它在排序键和分区键里，ClickHouse 不允许 ALTER 键列
（`ALTER_OF_COLUMN_IS_FORBIDDEN`），所以 ALTER 段只 MODIFY `events.timestamp`。建表时没配
`timezone` 的老表，`timestamp` 就一直是裸 `DateTime64(9)`：存的时刻不受影响（INSERT 一律带
偏移），只是查出来按服务端时区显示。要换显示时区只能重建表，或者查询时 `toTimeZone(timestamp,
'Asia/Shanghai')`。

要点：

* 排序键是 `(service_name, span_name, toDateTime(timestamp))`，和 exporter 一样：「某个服务的
  某个接口最近的请求」这类查询走排序键；「拿一个 trace id 查整条链路」不带时间范围，靠
  `idx_trace_id` 跳过没这个 id 的 granule。
* `events.*` / `links.*` 是 ClickHouse 的 `Nested` 平铺写法，插入时按几个等长数组给，不用开
  `flatten_nested` 以外的任何设置。
* `fields` 里的静态字段类型按值推断：字符串 → `LowCardinality(String)`、整数 → `Int64`、
  小数 → `Float64`、布尔 → `UInt8`。**给线上配置新加了 `fields`，重跑一次 `--ddl` 的输出
  （ddl Job）**，忘了的话启动时 healthcheck 会点名缺哪几列。
* 集群写法和 logpipe 一样：`sink.cluster` 填 ClickHouse 集群名，`--ddl` 生成
  `ReplicatedMergeTree` 本地表 `otel_trace_local` + `Distributed` 表 `otel_trace`，全部
  `ON CLUSTER`。分片键是 `cityHash64(trace_id)`：同一条 trace 的 span 落同一个分片，
  按 trace id 查不用跨分片。

### 常用查询

```sql
-- 一条链路
select timestamp, service_name, span_name, span_kind, duration_ns / 1e6 as ms, status_code, span_id, parent_span_id
from logs.otel_trace
where trace_id = 'e89a476882236ce0f1186d1522c8f59f'
order by timestamp;

-- 某个接口最近的慢请求
select timestamp, trace_id, duration_ns / 1e6 as ms
from logs.otel_trace
where service_name = 'order-service' and span_name = 'GET /orders/{id}'
  and timestamp > now() - interval 1 hour
order by duration_ns desc limit 20;

-- 带某个 tag 的 span：JSON 列按路径取子列，只读这一个 key；类型不统一的 key 带 .:类型 取
select timestamp, trace_id, span_attributes.http.route
from logs.otel_trace
where span_attributes.http.response.status_code.:Int64 = 500 and timestamp > now() - interval 1 hour;

-- 某个服务出现过哪些属性 key（不解值，只读路径）
select arrayJoin(JSONAllPaths(span_attributes)) as k, count()
from logs.otel_trace
where service_name = 'order-service' and timestamp > now() - interval 1 hour
group by k order by count() desc;

-- trace 关联日志（logpipe 的表）
select timestamp, level, logger, message
from logs.app_log
where trace_id = 'e89a476882236ce0f1186d1522c8f59f'
order by timestamp;
```

### 接 Grafana

ClickHouse 数据源自带 trace 视图，query 的列名对上就行。tags 要 Map，用上面「属性列」里的拍平
表达式（新版插件能直接认 JSON 列并在前端拍平，那就把 `span_attributes` 原样给它）：

```sql
select trace_id as traceID, span_id as spanID, parent_span_id as parentSpanID,
       service_name as serviceName, span_name as operationName,
       timestamp as startTime, duration_ns / 1e6 as duration,
       <拍平表达式 span_attributes> as tags, <拍平表达式 resource_attributes> as serviceTags
from logs.otel_trace
where trace_id = '${traceId}'
order by timestamp
```

trace → 日志：Trace to logs 选 logpipe 的 ClickHouse 数据源，查询写 `trace_id = '${__span.traceId}'`。

## 投递语义

* **默认（`wait_for_write: false`）**：请求进了队列就给 SDK 回成功，和 OTel collector 的默认行为
  一样。ClickHouse 写失败按指数退避重试（默认 5 次），仍失败时 `on_error: stop` 停机、
  `drop` 丢掉继续 —— 都会丢这一批。
* **`wait_for_write: true`**：等数据**真正写进存储**再回成功。写失败会反映成 SDK 那边的导出失败，
  SDK 自己重发（Java agent 默认最多 5 次、每次退避翻倍），等于没有磁盘缓冲也有「至少一次」；
  重启、`on_error: stop` 时手上没写完的批次同样让 SDK 重发。代价是每个导出请求多等一个攒批
  周期（`batch.timeout_secs`）加一次写入的时间，SDK 的导出超时（默认 10s）要比这个长。
* 背压：source 与 sink 之间是有界队列（`pipeline.buffer`），存储慢下来时新请求会在队列口等
  `enqueue_timeout_secs`，等不到就回「稍后重试」，不会把内存吃光。
* 写入可能被重试，所以同一批 span 可能重复入库（表是 MergeTree，重复行不会合并）。要去重就
  换成 `ReplacingMergeTree` 并把 `span_id` 加进排序键 —— 改 `--ddl` 输出的 SQL 就行，程序不关心。

## 部署（k8s）

push 模型：应用主动发过来，所以是 Deployment + Service，不是 DaemonSet；不读宿主机文件，
不需要 root，也不需要 RBAC。`deploy/tracepipe-deployment.yaml` 可以直接 apply。

**顺序是先建表、再起 Deployment** —— 配了 `require_healthy: true`，表不存在时 healthcheck 直接
失败退出，Pod 会 CrashLoopBackOff：

```bash
# 1. namespace + ConfigMap + Deployment + Service
kubectl apply -f deploy/tracepipe-deployment.yaml

# 2. 建库建表（挂的是同一个 ConfigMap，列不会和采集端对不上）
kubectl apply -f deploy/tracepipe-ddl-job.yaml
kubectl -n tracing wait --for=condition=complete job/tracepipe-ddl --timeout=180s

# 3. 让第 1 步已经起来的 Pod 立刻重试，不用等 CrashLoop 退避
kubectl -n tracing rollout restart deployment/tracepipe
```

然后应用的 `OTEL_EXPORTER_OTLP_ENDPOINT` 指到 `http://tracepipe.tracing.svc.cluster.local:4317`。

Job 里 `apply-ddl` 容器的 `CH_HOST` / `CH_DATABASE` / `CH_CLUSTER` / `CH_USER` / `CH_PASSWORD`
要和 ConfigMap 里 sink 的对上。**改了配置里的 `fields` 或 `timezone` 就重跑一次 Job**。

不想在集群里跑 Job 的话，`--ddl` 不连库，本地也能渲染：

```bash
kubectl -n tracing get cm tracepipe-config -o jsonpath='{.data.tracepipe\.yaml}' > /tmp/tracepipe.yaml
docker run --rm -v /tmp/tracepipe.yaml:/etc/tracepipe/tracepipe.yaml:ro \
  ghcr.io/easayliu/trace:v0.2.0 --ddl /etc/tracepipe/tracepipe.yaml
```

要点：多副本各自小批量写，`async_insert: true` 让 ClickHouse 服务端再攒一层；
`terminationGracePeriodSeconds` 留够（滚动更新时正在等 `wait_for_write` 的请求要写完）；
Service 前面走 gRPC 的话注意 k8s Service 是按连接负载均衡的，一个 SDK 的长连接只打到一个副本 ——
副本数按「够用」配，不是按均摊算。

### 发布

镜像由 CI 构建推送，打 tag 就发版（tag 必须单独推，和 logpipe 一样）：

```bash
# 1. 先改 Cargo.toml 的 version，CI 会校验它和 tag 一致
git commit -am "release v0.2.0"
git push origin main

# 2. tag 单独推，不能和分支挤在同一条 git push 里，否则不触发构建
git tag v0.2.0
git push origin v0.2.0
```

产出 `ghcr.io/easayliu/trace:v0.2.0`，同时把 `:latest` 指过去。`.github/workflows/ci.yml` 在
push / PR 上跑 `fmt --check` + `clippy -D warnings` + `cargo test`；`docker.yml` 构建前复用它
作为闸门。

## 调试：回放 OTLP/JSON

`type: stdin` 读 OTLP/JSON，一行一个导出请求，正是 OTel collector `file` exporter 写出来的格式：

```yaml
source:
  type: stdin
sink:
  type: console
  encoding: text
```

```bash
cat traces.jsonl | tracepipe stdin.yaml
```

## 加自己的组件

只有两个 trait，都很短：

```rust
#[async_trait]
pub trait Source: Send + 'static {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()>;
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn write(&mut self, events: &[SpanEvent]) -> Result<()>;
    async fn healthcheck(&self) -> Result<()> { Ok(()) }
}
```

攒批、重试、ack、退出都在 pipeline 里。内置组件：

| 类型 | 组件 | 说明 |
| --- | --- | --- |
| source | `OtlpSource` | OTLP/gRPC + OTLP/HTTP 接收端，gzip、大小上限、队列背压 |
| source | `StdinSource` | 读 OTLP/JSON 行，调试 / 回放用 |
| sink | `ClickhouseSink` | HTTP `JSONEachRow` 批量插入，gzip 请求体 |
| sink | `ConsoleSink` | JSON / 摘要文本输出 |
| sink | `MemorySink` | 测试用 |

## 还没做

* 只收 trace：OTLP 的 logs / metrics 信号没有接（日志走 logpipe）
* 采样（tail-based sampling）：现在收到什么存什么，量大靠 SDK 侧的头采样和表的 TTL
* TLS / 鉴权：接收端是明文，只应暴露在集群内
* ClickHouse 多 endpoint 轮询 / 故障转移（现在只能填一个地址，靠外面的 LB）
* 磁盘缓冲（`wait_for_write` 让 SDK 兜底，代价是延迟）
* 采集指标（收发条数 / 拒收次数暂时只有 tracing 日志）
