//! Standalone probe driving MySqlCdcReader directly (no engine) with
//! startup.mode = timestamp, replicating the failing smoke test.
//!
//! Usage: cargo run -p seatunnel-connector-cdc-mysql --example reader_probe

use seatunnel_api::source::source_reader::{PollResult, SourceReader};
use seatunnel_connector_cdc_mysql::{MySqlCdcConfig, MySqlCdcReader, MySqlStartupMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let target_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;

    let config = MySqlCdcConfig {
        hostname: "127.0.0.1".into(),
        port: 13306,
        username: "root".into(),
        password: "root".into(),
        database_name: "seatunnel".into(),
        table_name: "users".into(),
        startup_mode: MySqlStartupMode::Timestamp {
            timestamp: target_ts,
        },
        subtask_index: 0,
        subtask_count: 1,
        ..Default::default()
    };
    let mut reader = MySqlCdcReader::new(config, None);
    reader.open().await?;
    println!("[open] done, target_ts={}", target_ts);

    // Warm-up drain for a moment.
    for _ in 0..40 {
        match reader.poll_next().await? {
            PollResult::Record(_) => println!("[warmup] unexpected record"),
            PollResult::SchemaChange(e) => {
                println!("[warmup] schema change: {:?}", e.changes.len())
            }
            PollResult::Empty => {}
            PollResult::EOF => {
                println!("[warmup] EOF");
                break;
            }
        }
    }
    println!("[warmup] drained; inserting probe row");

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(13306)
        .user(Some("root"))
        .pass(Some("root"));
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    use mysql_async::prelude::Queryable;
    let _: Vec<mysql_async::Row> = conn
        .exec(
            "INSERT INTO seatunnel.users(name, score) VALUES ('reader-probe', 42)",
            (),
        )
        .await?;
    println!("[insert] done; polling for live rows");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut records = 0u64;
    while std::time::Instant::now() < deadline {
        match reader.poll_next().await? {
            PollResult::Record(row) => {
                records += 1;
                println!("[live] record: {:?}", row.0.fields);
            }
            PollResult::SchemaChange(e) => {
                println!("[live] schema change on {}: {:?}", e.table, e.statement)
            }
            PollResult::Empty => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            PollResult::EOF => {
                println!("[live] EOF");
                break;
            }
        }
    }
    println!("[result] records={}", records);
    Ok(())
}
