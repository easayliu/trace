//! 入库端：把一批 span 写进存储。

pub mod clickhouse;
pub mod console;
pub mod memory;

pub use clickhouse::ClickhouseSink;
pub use console::ConsoleSink;
pub use memory::MemorySink;

use async_trait::async_trait;

use crate::error::Result;
use crate::event::SpanEvent;

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// 写入一批 span。返回 `Err` 时 pipeline 会按重试策略重发同一批。
    /// 因此实现必须能容忍重复写入（幂等，或业务上可接受重复）。
    async fn write(&mut self, events: &[SpanEvent]) -> Result<()>;

    /// 启动前的连通性检查。
    async fn healthcheck(&self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "sink"
    }
}
