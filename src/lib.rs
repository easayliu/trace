//! tracepipe —— 一个精简的 OTLP trace 采集入库框架，和 logpipe 同一套骨架。
//!
//! 数据流只有三段：
//!
//! ```text
//!   OTel SDK / agent ──OTLP──▶ Source ──(Batch)──▶ Pipeline ──(攒批 / 重试)──▶ Sink
//!                              gRPC 4317 / HTTP 4318   批处理与背压              入库
//! ```
//!
//! * [`Source`] 负责收 span（OTLP 接收端、读 stdin……），并在数据落库后收到 ack；
//! * [`Sink`] 负责把一批 span 写进存储（ClickHouse、控制台……）；
//! * [`Pipeline`] 负责攒批、重试、优雅退出，把两者串起来。
//!
//! ```no_run
//! use tracepipe::{Pipeline, sink::ClickhouseSink, source::OtlpSource};
//!
//! # async fn run() -> tracepipe::Result<()> {
//! Pipeline::builder()
//!     .source(OtlpSource::new())
//!     .sink(ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_trace"))
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```

pub mod batch;
pub mod config;
pub mod error;
pub mod event;
pub mod otlp;
pub mod pipeline;
pub mod shutdown;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
pub use event::{SpanEvent, SpanKind, SpanLink, StatusCode, TimedEvent};
pub use pipeline::{Pipeline, PipelineBuilder, RunningPipeline};
pub use shutdown::{Shutdown, ShutdownHandle};
pub use sink::Sink;
pub use source::{Batch, Source, SourceSender};
