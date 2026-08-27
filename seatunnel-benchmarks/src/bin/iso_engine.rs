// Layer-isolation micro-benchmark for the engine hot loop.
//
// Runs the same 200k-row stream through progressively more engine layers to
// pinpoint where per-row time goes:
//   1. raw    : source.poll_next() -> sink.write() direct calls (control)
//   2. single : TaskGroup -> one no-op sink writer
//   3. fanout : TaskGroup -> FanoutSinkWriter -> two no-op sink writers
//   4. kafka  : TaskGroup -> FanoutSinkWriter -> two real Kafka writers

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use seatunnel_api::row::{Field, Row};
use seatunnel_api::sink::sink_writer::SinkWriter;
use seatunnel_api::source::source_reader::{PollResult, SourceReader};

use seatunnel_engine_core::connector_factory::AnySplit;
use seatunnel_engine_core::fanout::{FanoutSinkWriter, SinkFailurePolicy};
use seatunnel_engine_core::task_group::{TaskContext, TaskGroup};

const ROWS: u64 = 200_000;

struct SeqSource {
    emitted: u64,
    total: u64,
}

impl SeqSource {
    fn row(&self) -> Row {
        let mut row = Row::new(seatunnel_api::RowKind::Insert, 3);
        row.set(0, Field::Int64(self.emitted as i64));
        row.set(1, Field::Int64(0)); // ts_ms slot
        row.set(2, Field::String("payload-iso".to_string()));
        row
    }
}

impl SourceReader for SeqSource {
    type Output = Row;
    type Split = AnySplit;
    fn open(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
    fn poll_next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<PollResult<Self::Output>>> + Send + '_>> {
        Box::pin(async move {
            if self.emitted >= self.total {
                return Ok(PollResult::EOF);
            }
            let row = self.row();
            self.emitted += 1;
            Ok(PollResult::Record(row))
        })
    }
    fn snapshot_state(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn add_splits(&mut self, _splits: Vec<Self::Split>) {}
    fn handle_no_more_splits(&mut self) {}
    fn close(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct NoopWriter {
    writes: u64,
}

impl SinkWriter for NoopWriter {
    type Input = Row;
    type WriterState = Vec<u8>;
    type CommitInfo = Vec<u8>;
    fn open(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn write(&mut self, _record: Self::Input) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        self.writes += 1;
        Box::pin(async { Ok(()) })
    }
    fn prepare_commit(
        &mut self,
    ) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<Self::CommitInfo>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn snapshot_state(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, Vec<u8>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn close(&mut self) -> seatunnel_api::sink::sink_writer::WriterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn boxed_sink(w: NoopWriter) -> seatunnel_engine_core::connector_factory::BoxedSinkWriter {
    Box::new(w)
}

fn boxed_source(total: u64) -> seatunnel_engine_core::connector_factory::BoxedSourceReader {
    Box::new(SeqSource { emitted: 0, total })
}

async fn scenario_raw() -> Duration {
    let mut source = SeqSource { emitted: 0, total: ROWS };
    let mut sink = NoopWriter::default();
    let start = Instant::now();
    source.open().await.unwrap();
    sink.open().await.unwrap();
    loop {
        match source.poll_next().await.unwrap() {
            PollResult::Record(row) => sink.write(row).await.unwrap(),
            PollResult::EOF => break,
            _ => tokio::time::sleep(Duration::from_millis(1)).await,
        }
    }
    sink.prepare_commit().await.unwrap();
    sink.close().await.unwrap();
    start.elapsed()
}

async fn scenario_taskgroup(fanout: bool) -> Duration {
    let ctx = TaskContext::new("iso", "iso-job", "p0", 0, 1);
    let sink: seatunnel_engine_core::connector_factory::BoxedSinkWriter = if fanout {
        let mux = FanoutSinkWriter::new(
            vec![
                ("a".to_string(), boxed_sink(NoopWriter::default())),
                ("b".to_string(), boxed_sink(NoopWriter::default())),
            ],
            SinkFailurePolicy::Fail,
        );
        Box::new(mux)
    } else {
        boxed_sink(NoopWriter::default())
    };
    let mut group = TaskGroup::new(ctx, boxed_source(ROWS), sink);
    let start = Instant::now();
    group.run().await.unwrap();
    start.elapsed()
}

#[tokio::main]
async fn main() {
    let d = scenario_raw().await;
    println!("raw    : {:>10?}  ({:>8.0} rows/s)", d, ROWS as f64 / d.as_secs_f64());

    let d = scenario_taskgroup(false).await;
    println!("single : {:>10?}  ({:>8.0} rows/s)", d, ROWS as f64 / d.as_secs_f64());

    let d = scenario_taskgroup(true).await;
    println!("fanout : {:>10?}  ({:>8.0} rows/s)", d, ROWS as f64 / d.as_secs_f64());
}
