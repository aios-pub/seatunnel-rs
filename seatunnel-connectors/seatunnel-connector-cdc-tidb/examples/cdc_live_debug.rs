//! Standalone EventFeedV2 diagnostic client.
//!
//! Opens one raw EventFeedV2 stream per region and prints a compact summary
//! of every received `ChangeDataEvent` (row types, resolved ts, errors) so
//! live PREWRITE/COMMIT delta delivery can be verified independently of the
//! engine / source reader pipeline.
//!
//! Usage:
//!   cargo run -p seatunnel-connector-cdc-tidb --example cdc_live_debug -- \
//!       --table-id 74 --duration 30 \
//!       [--span table|region|intersect] [--register none|set] \
//!       [--pd 127.0.0.1:2379] [--checkpoint-ago-secs 0] \
//!       [--rewrite host.docker.internal=127.0.0.1]
//!
//! While it runs, insert rows into the table from another terminal and watch
//! for `type=PREWRITE` / `type=COMMIT` rows on stdout.

use std::time::Duration;

use futures::StreamExt;
use prost::Message;
use tokio::time::sleep;

use seatunnel_connector_cdc_tidb::decoder::decode_record_key;
use seatunnel_connector_cdc_tidb::kvproto::cdcpb::change_data_client::ChangeDataClient;
use seatunnel_connector_cdc_tidb::kvproto::cdcpb::event::Event as CdcEvent;
use seatunnel_connector_cdc_tidb::kvproto::cdcpb::{ChangeDataRequest, Header};
use seatunnel_connector_cdc_tidb::kvproto::kvrpcpb::ExtraOp;
use seatunnel_connector_cdc_tidb::pd_client::PdClient;
use seatunnel_connector_cdc_tidb::table_key_range;

fn now() -> String {
    chrono::Local::now().format("%H:%M:%S%.3f").to_string()
}

/// MyRocks-style memcomparable chunked encoding (what official TiCDC's
/// `spanz.ToComparableKey` / PD region boundaries use): every 8-byte chunk
/// is emitted verbatim, padded with zeros to 8 bytes, and terminated with
/// `0xFF - pad_count`, so longer real data sorts higher.
fn encode_comparable(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + key.len() / 8 + 9);
    for chunk in key.chunks(8) {
        let pad = 8 - chunk.len();
        out.extend_from_slice(chunk);
        out.extend(std::iter::repeat_n(0u8, pad));
        out.push(0xFF - pad as u8);
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn log_type_name(t: i32) -> &'static str {
    match t {
        1 => "PREWRITE",
        2 => "COMMIT",
        3 => "ROLLBACK",
        4 => "COMMITTED",
        5 => "INITIALIZED",
        _ => "UNKNOWN",
    }
}

fn op_type_name(t: i32) -> &'static str {
    match t {
        0 => "PUT",
        1 => "DELETE",
        _ => "UNKNOWN",
    }
}

/// Per-region summary counters printed at exit.
#[derive(Default)]
struct Counters {
    messages: u64,
    rows: [u64; 6],
    resolved_ts_events: u64,
    batch_resolved_ts: u64,
    errors: u64,
    last_resolved_ts: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut pd_addr = "127.0.0.1:2379".to_string();
    let mut table_id: i64 = -1;
    let mut duration = 30u64;
    // Variant knobs — defaults mirror the current engine behavior.
    let mut span_mode = "table".to_string();
    let mut register_mode = "set".to_string();
    let mut checkpoint_ago = 0u64;
    let mut rewrite = ("host.docker.internal".to_string(), "127.0.0.1".to_string());

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| panic!("missing value for {arg}"))
        };
        match arg.as_str() {
            "--pd" => pd_addr = value(),
            "--table-id" => table_id = value().parse()?,
            "--duration" => duration = value().parse()?,
            "--span" => span_mode = value(),
            "--register" => register_mode = value(),
            "--checkpoint-ago-secs" => checkpoint_ago = value().parse()?,
            "--rewrite" => {
                let v = value();
                let (from, to) = v.split_once('=').expect("--rewrite expects from=to");
                rewrite = (from.to_string(), to.to_string());
            }
            other => anyhow::bail!("unknown argument {other}"),
        }
    }
    if table_id < 0 {
        anyhow::bail!("--table-id is required");
    }

    let mut pd = PdClient::connect(&pd_addr).await?;
    let cluster_id = pd.cluster_id();
    let tso = pd.get_tso().await?;
    // TSO layout: physical ms << 18 | logical; subtract from the physical part.
    let checkpoint = if checkpoint_ago > 0 {
        tso.saturating_sub((checkpoint_ago * 1000) << 18)
    } else {
        tso
    };
    println!(
        "[{}] cluster_id={} tso={} checkpoint={}",
        now(),
        cluster_id,
        tso,
        checkpoint
    );

    let (table_start, table_end) = table_key_range(table_id);
    println!(
        "[{}] table span (raw): [{}, {})",
        now(),
        hex(&table_start),
        hex(&table_end)
    );
    let enc_start = encode_comparable(&table_start);
    let enc_end = encode_comparable(&table_end);
    println!(
        "[{}] table span (encoded): [{}, {})",
        now(),
        hex(&enc_start),
        hex(&enc_end)
    );

    // Region discovery must run in the same key space as the PD region
    // boundaries: encoded (memcomparable) unless explicitly overridden.
    let (query_start, query_end) = if span_mode == "table" {
        (table_start.clone(), table_end.clone())
    } else {
        (enc_start.clone(), enc_end.clone())
    };
    let regions = pd.scan_regions(&query_start, &query_end).await?;
    println!("[{}] discovered {} region(s)", now(), regions.len());

    let mut tasks = Vec::new();
    for ri in &regions {
        let region_id = ri.region.id;
        let epoch = ri.region.region_epoch;
        // Resolve the leader store address (first voter peer's store).
        let leader = pd.leader_address(ri).await?.unwrap_or_default();
        let leader = if let Some((host, port)) = leader.rsplit_once(':') {
            let host = if host == rewrite.0 { &rewrite.1 } else { host };
            format!("{host}:{port}")
        } else {
            leader
        };

        // Span variant under test. `encoded` (official parity): intersection
        // of the memcomparable-encoded table span with the region bounds.
        let (sk, ek) = match span_mode.as_str() {
            "region" => (ri.region.start_key.clone(), ri.region.end_key.clone()),
            "table" => (table_start.clone(), table_end.clone()),
            _ => {
                let s = if ri.region.start_key > enc_start {
                    ri.region.start_key.clone()
                } else {
                    enc_start.clone()
                };
                let e = if ri.region.end_key.is_empty() || ri.region.end_key > enc_end {
                    enc_end.clone()
                } else {
                    ri.region.end_key.clone()
                };
                (s, e)
            }
        };

        let request = ChangeDataRequest {
            header: Some(Header {
                cluster_id,
                ticdc_version: "8.1.0".to_string(),
            }),
            region_id,
            region_epoch: epoch,
            checkpoint_ts: checkpoint,
            start_key: sk.clone(),
            end_key: ek.clone(),
            request_id: 1000 + region_id,
            extra_op: ExtraOp::ReadOldValue as i32,
            kv_api: 0,
            scan_priority: 0,
            filter_loop: false,
            request: if register_mode == "set" {
                Some(
                    seatunnel_connector_cdc_tidb::kvproto::cdcpb::change_data_request::Request::Register(Default::default()),
                )
            } else {
                None
            },
        };
        let wire = request.encode_to_vec();
        println!(
            "[{}] region={} leader={} span=[{}, {}) epoch={:?} req_wire={} bytes: {}",
            now(),
            region_id,
            leader,
            hex(&sk),
            hex(&ek),
            request.region_epoch,
            wire.len(),
            hex(&wire)
        );

        let dur = duration;
        tasks.push(tokio::spawn(async move {
            run_region_stream(leader, region_id, request, dur).await;
        }));
    }

    // Let the region tasks run for the requested duration, then hard-exit
    // (streams are diagnostics-only; no graceful drain needed).
    sleep(Duration::from_secs(duration)).await;
    println!("[{}] duration elapsed — exiting", now());
    std::process::exit(0);
}

async fn run_region_stream(
    addr: String,
    region_id: u64,
    request: ChangeDataRequest,
    duration: u64,
) {
    let uri = if addr.starts_with("http://") {
        addr.clone()
    } else {
        format!("http://{addr}")
    };
    let channel = match tonic::transport::Channel::from_shared(uri)
        .unwrap()
        .connect_timeout(Duration::from_secs(10))
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(8 * 1024 * 1024)
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{}] region {} connect failed: {}", now(), region_id, e);
            return;
        }
    };
    let mut client = ChangeDataClient::new(channel);
    // The send side must stay open: closing it makes TiKV tear the stream down.
    let req_stream = futures::stream::iter(vec![request.clone()]).chain(futures::stream::pending());
    let mut grpc_req = tonic::Request::new(req_stream);
    grpc_req
        .metadata_mut()
        .insert("features", "stream-multiplexing".parse().unwrap());
    let mut stream = match client.event_feed_v2(grpc_req).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            eprintln!("[{}] region {} EventFeedV2 failed: {}", now(), region_id, e);
            return;
        }
    };
    println!(
        "[{}] region {} stream OPEN (request_id={})",
        now(),
        region_id,
        request.request_id
    );

    let mut counters = Counters::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration + 5);
    loop {
        let msg = tokio::time::timeout_at(deadline, stream.message()).await;
        let event = match msg {
            Err(_) => break, // deadline
            Ok(Err(e)) => {
                eprintln!("[{}] region {} stream error: {}", now(), region_id, e);
                counters.errors += 1;
                break;
            }
            Ok(Ok(None)) => {
                eprintln!("[{}] region {} stream closed by server", now(), region_id);
                break;
            }
            Ok(Ok(Some(ev))) => ev,
        };
        counters.messages += 1;
        print_event(region_id, &event, &mut counters);
    }
    println!(
        "[{}] region {} SUMMARY messages={} rows(prewrite={},commit={},rollback={},committed={},initialized={}) rts_events={} batch_rts={} last_rts={} errors={}",
        now(),
        region_id,
        counters.messages,
        counters.rows[1],
        counters.rows[2],
        counters.rows[3],
        counters.rows[4],
        counters.rows[5],
        counters.resolved_ts_events,
        counters.batch_resolved_ts,
        counters.last_resolved_ts,
        counters.errors
    );
}

fn print_event(
    region_id: u64,
    event: &seatunnel_connector_cdc_tidb::kvproto::cdcpb::ChangeDataEvent,
    counters: &mut Counters,
) {
    for ev in &event.events {
        let tag = format!("region={} rid={}", ev.region_id, ev.request_id);
        match &ev.event {
            Some(CdcEvent::Entries(entries)) => {
                println!("[{}] {} ENTRIES rows={}", now(), tag, entries.entries.len());
                for row in &entries.entries {
                    let t = row.r#type.clamp(0, 5) as usize;
                    counters.rows[t] += 1;
                    let handle = decode_record_key(&row.key)
                        .map(|(_, h)| h.to_string())
                        .unwrap_or_else(|| "?".to_string());
                    println!(
                        "[{}]   type={} op={} start_ts={} commit_ts={} handle={} key={} vlen={} old_vlen={} value_hex={}",
                        now(),
                        log_type_name(row.r#type),
                        op_type_name(row.op_type),
                        row.start_ts,
                        row.commit_ts,
                        handle,
                        hex(&row.key),
                        row.value.len(),
                        row.old_value.len(),
                        hex(&row.value[..row.value.len().min(64)])
                    );
                }
            }
            Some(CdcEvent::ResolvedTs(ts)) => {
                counters.resolved_ts_events += 1;
                counters.last_resolved_ts = counters.last_resolved_ts.max(*ts);
                println!("[{}] {} RESOLVED_TS ts={}", now(), tag, ts);
            }
            Some(CdcEvent::Error(err)) => {
                counters.errors += 1;
                println!("[{}] {} ERROR {:?}", now(), tag, err);
            }
            Some(CdcEvent::Admin(admin)) => {
                println!("[{}] {} ADMIN {:?}", now(), tag, admin);
            }
            other => {
                println!("[{}] {} OTHER {:?}", now(), tag, other);
            }
        }
    }
    if let Some(rt) = &event.resolved_ts {
        counters.batch_resolved_ts += 1;
        counters.last_resolved_ts = counters.last_resolved_ts.max(rt.ts);
        println!(
            "[{}] region={} BATCH_RESOLVED_TS ts={} rid={} regions={:?} contains_self={}",
            now(),
            region_id,
            rt.ts,
            rt.request_id,
            rt.regions,
            rt.regions.contains(&region_id)
        );
    }
}
