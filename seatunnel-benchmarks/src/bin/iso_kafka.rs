// Micro-benchmark: rdkafka FutureProducer produce+delivery throughput with
// the exact configuration the engine's Kafka sink uses.

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let total = 10_000usize;
    let scenarios: &[(&str, &[(&str, &str)])] = &[
        ("default(ack1)", &[("acks", "1")]),
        ("linger0", &[("acks", "1"), ("linger.ms", "0")]),
        (
            "linger0+bnum",
            &[("acks", "1"), ("linger.ms", "0"), ("batch.num.messages", "10000")],
        ),
        (
            "inflight1",
            &[("acks", "1"), ("max.in.flight.requests.per.connection", "1")],
        ),
        (
            "debug-nagle",
            &[
                ("acks", "1"),
                ("linger.ms", "0"),
                ("socket.nagle.disable", "true"),
            ],
        ),
    ];
    for (name, overrides) in scenarios {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", "127.0.0.1:9092")
            .set("message.timeout.ms", "30000");
        for (k, v) in *overrides {
            cfg.set(*k, *v);
        }
        let producer: FutureProducer = cfg.create().unwrap();

        // Warm up metadata/topic.
        let m = FutureRecord::<str, str>::to("iso_fake1").payload("warmup");
        producer.send(m, Duration::from_secs(5)).await.unwrap();

        let start = Instant::now();
        let payloads: Vec<String> = (0..total)
            .map(|i| format!("[{},1724000000000,{},\"payload-iso\"]", i, i))
            .collect();
        let mut deliveries = Vec::with_capacity(total);
        for payload in &payloads {
            let record = FutureRecord::<str, str>::to("iso_fake1").payload(payload.as_str());
            deliveries.push(producer.send(record, Duration::from_secs(5)));
        }
        let enqueued = start.elapsed();
        let mut failures = 0;
        for d in deliveries {
            if d.await.is_err() {
                failures += 1;
            }
        }
        let elapsed = start.elapsed();
        println!(
            "{:>14}: enqueue={:>10?} total={:>10?} throughput={:>7.0} msg/s failures={}",
            name,
            enqueued,
            elapsed,
            total as f64 / elapsed.as_secs_f64(),
            failures
        );
    }
}
