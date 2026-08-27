# 单机压测报告：多 Source / 多 Sink / 500 张表（延迟核心指标）

- 日期：2026-08-27
- 引擎版本：seatunnel-rs v0.1.0（基线 commit `1c5a183` vs 优化后工作区代码）
- 结论先行：**优化前引擎无法满足任何延迟要求（增量阶段吞吐 ~5 行/秒，p50 延迟 197.9 秒，压测期间仅 0.43% 消息送达）；优化后在 2,000 行/秒持续负载下 p50 延迟 82ms、p99 328ms、100% 送达，整栈（MySQL + 引擎 + Kafka + 探针同机）持续容量上限约 3,400 行/秒。**

---

## 1. 测试环境

| 项 | 值 |
|---|---|
| 机器 | Apple M2, 8 核, 24 GB 内存, macOS 15 (darwin 25.5.0) |
| 引擎运行方式 | 单机本地模式（`seatunnel run -c job.yaml -m local`，进程内执行） |
| MySQL | docker `mysql:8.0.46`（binlog-format=row, binlog-row-image=full），宿主机端口 13306 |
| Kafka | docker `confluentinc/cp-kafka:7.6.0`，宿主机端口 9092，auto-create topics |
| 压测工具 | 本仓库自带（见 §5 复现方法），生成器/探针与引擎同机运行 |

## 2. 拓扑：2 源 × 500 表 × 4 sink

```
                        ┌─ Kafka topic *_a1 (json)
MySQL perf_a (250 表) ──┤ MySQL-CDC pipeline A（server-id 5401）
                        └─ Kafka topic *_a2 (json)

                        ┌─ Kafka topic *_b1 (json)
MySQL perf_b (250 表) ──┤ MySQL-CDC pipeline B（server-id 5402）
                        └─ Kafka topic *_b2 (json)
```

- 每张表：`id BIGINT UNSIGNED AUTO_INCREMENT PK, ts_ms BIGINT, seq BIGINT, payload VARCHAR(96)`，预置 100 行快照数据（`ts_ms=0`，不计入延迟统计）。
- 负载生成器按目标速率轮询写 500 张表，每行写入宿主机时钟 `ts_ms`；探针消费 4 个 topic，`延迟 = 消费时刻 - ts_ms`（两端同为宿主机时钟，无时钟偏差）。
- 快照吞吐 = `startup.mode=initial` 全量同步 5 万行的速率（每 topic 25,000 条）。
- 配置注意：MySQL-CDC 只配 `database-names` 时，legacy `table-name` 默认值 `users` 仍会作为精确表过滤，导致 **0 张表匹配**；需同时配置 `table-pattern: ".*"`。

## 3. 结果

### 3.1 端到端延迟（核心指标）

| 指标 | 基线 @1,941 行/秒 | 优化后 @1,495 行/秒 | 优化后 @3,841 行/秒 (batch=4) |
|---|---:|---:|---:|
| 送达比例 | **0.43%**（4,000 / 932,012 条） | **100%**（358,950 条） | **100%**（922,256 条） |
| p50 | 197,913 ms | **82 ms** | 6,509 ms |
| p90 | 199,966 ms | 111 ms | 17,599 ms |
| p95 | 200,220 ms | 158 ms | 19,163 ms |
| p99 | 200,431 ms | **328 ms** | 20,419 ms |
| p999 | 200,520 ms | 552 ms | 20,818 ms |
| max | 200,534 ms | 743 ms | 20,852 ms |

- 基线“送达 0.43%”的原因：本地模式无 checkpoint，Kafka sink 只在攒满 1,000 条时刷写，每个 sink 仅刷出第一个满批（4 sink × 1,000 条 = 4,000 条），其余 92.8 万条滞留缓冲区直至进程被杀。
- 3,841 行/秒一轮延迟持续爬升（每 10 秒 p99 约 +1.5s），说明已超过整栈持续容量（§3.3），但数据**零丢失**、最终全部送达。
- 优化后各 topic 延迟分布均匀（p50 80–84ms；p99 285–400ms @2,000 行/秒）。
- 生成器实际速率受单行 INSERT 经 docker 端口转发往返（~5ms/条 × 8 并发 ≈ 1,600 行/秒）限制；batch=4 后可达 3,841 行/秒（MySQL 侧 0 失败）。

### 3.2 快照（全量初始化）吞吐：50,000 行 / pipeline

| | 基线 | 优化后 | 提升 |
|---|---:|---:|---:|
| 完成时间 | ~240 s | **~13 s** | ~18x |
| 吞吐（行/秒/pipeline） | ~104 | ~1,900 | |

### 3.3 容量与分层微基准（定位过程）

| 实验 | 结果 |
|---|---:|
| 引擎裸循环 poll→write（无网络） | 726 万 行/秒 |
| TaskGroup + 单 sink | 600 万 行/秒 |
| TaskGroup + fanout(2 sink) | 190 万 行/秒 |
| 引擎全链路 Fake→2×Kafka（优化后） | 3,252 行/秒（20 万行 61.5s，CPU 32%） |
| 同上（优化前） | 158 行/秒 |
| rdkafka producer 宿主机默认配置 | **159 条/秒** |
| rdkafka `linger.ms=0` | 4,246 条/秒 |
| rdkafka `linger.ms=0` + `socket.nagle.disable=true` | 5,203 条/秒 |
| Kafka broker（容器内 perf-test，排除 broker 瓶颈） | 48,426 条/秒 |
| 整栈持续容量（2 pipeline × 2 sink，同机） | ~3,400 行/秒 |

> 关键定位链：引擎核心层百万级行/秒 → 瓶颈在 Kafka producer；容器内 broker 4.8 万条/秒 → 瓶颈在宿主机↔容器转发路径上 librdkafka 默认 `linger.ms=5` 的病态行为（每批 ~1 条 × ~6.3ms）。

## 4. 代码优化清单（压测前审查 + 压测中定位）

| # | 位置 | 问题 | 修复 |
|---|---|---|---|
| 1 | `seatunnel-connectors/seatunnel-connector-cdc-mysql/src/lib.rs` `poll_incremental` | **增量阶段每次 poll 只吸收 1 个 binlog 事件**；每行 INSERT 产生 ~4 个事件（BEGIN/TableMap/WriteRows/XID），非行事件返回 `Empty`，引擎空转睡眠使吞吐限死在 ~5 行/秒 | 循环排水直到解出行、到达 stop 边界或 250ms 超时 |
| 2 | `seatunnel-connectors/seatunnel-connector-kafka/src/lib.rs` producer 构建 | librdkafka 默认 `linger.ms=5` 经 docker 端口转发退化为 159 条/秒（31 倍损失） | 显式 `linger.ms=0` + `socket.nagle.disable=true` |
| 3 | 同上 `KafkaSinkWriter` | 无任何基于时间的刷写：本地模式（无 checkpoint）下不满 1,000 条**永不刷写**，延迟无上界 | 新增 `batch.timeout.ms`（默认 100ms，别名 `linger.ms`）定时刷写 + 空闲 `poll_flush` |
| 4 | 同上 `flush_batch` | 逐条 `send().await` 串行等待每条 delivery report | 先全部入队（librdkafka 内部成批）再统一 await |
| 5 | `seatunnel-engine/seatunnel-engine-core/src/task_group.rs` | **每条记录一次 `status.lock().await` 异步锁**；`Empty` 固定睡 20ms（流量恢复尾延迟高） | 状态发布节流至 200ms；自适应退避（先 yield，再 1/2/5/10/20ms） |
| 6 | `seatunnel-engine/seatunnel-engine-core/src/fanout.rs` `write()` | 每条记录分配一个 `Vec`（`alive()`）；每个 sink 深拷贝 Row（N 次） | 倒序扫描免分配；最后一个存活 sink 直接 move（N-1 次拷贝） |
| 7 | `seatunnel-connectors/seatunnel-connector-jdbc/src/source.rs` | `Vec::remove(0)` 每条 O(n) 移动 + `first().cloned()` 深拷贝 | 改用 `VecDeque::pop_front()`（O(1) 移动语义） |
| 8 | `seatunnel-api/src/sink/sink_writer.rs`（+ adapter/fanout 接线） | 无空闲刷写钩子，尾部记录要等下一条写或 checkpoint | 新增 `SinkWriter::poll_flush()` 默认空实现；fanout worker 100ms 空闲 tick 触发 |

全部改动通过所在 crate 单测（engine-core 32、kafka 48、cdc-mysql 13、api 11、jdbc 等）。
已知的**存量问题**（与本次改动无关）：`seatunnel-e2e/tests/e2e.rs` 引用旧版 `WorkerRegistration`（缺 `running_task_ids` 字段）无法编译，系 HEAD 上测试代码漂移。

## 5. 复现方法

```bash
# 1) 基础设施（docker-compose 里的 mysql + kafka 已启动）
# 2) 建 500 张表并预置快照数据
bash seatunnel-benchmarks/stress/setup_mysql.sh

# 3) 压测（label 会用于隔离 Kafka topic 与产物目录）
bash seatunnel-benchmarks/stress/run_bench.sh <label> <seatunnel二进制> <速率> <秒数> [批量]
# 例： bash seatunnel-benchmarks/stress/run_bench.sh opt2000 target/release/seatunnel 2000 120
# 运行时产物：/tmp/seatunnel-stress/<label>/{job.yaml, engine.log, gen.json, probe.json, probe.log}
# 本报告原始数据已归档：seatunnel-benchmarks/stress/results/{baseline2000,opt2000,opt4000}_*.{json,log}

# 分层定位（可选）
cargo run --release -p seatunnel-benchmarks --bin iso_engine   # 引擎各层吞吐
cargo run --release -p seatunnel-benchmarks --bin iso_kafka    # producer 配置矩阵
```

压测工具：`seatunnel-benchmarks/src/bin/stress_gen.rs`（负载生成）、`stress_probe.rs`（延迟探针，输出 p50–p999 与 10 秒分桶序列）、`iso_engine.rs` / `iso_kafka.rs`（隔离微基准）。

## 6. 后续建议

1. **延迟还想更低**：Kafka sink 已支持 `batch.timeout.ms`（默认 100ms，约占当前 p50 的大头），可按场景调到 10–50ms。
2. **吞吐还想更高**：单机瓶颈在宿主机↔容器转发（~5K 条/秒/producer）；Kafka 直连容器网络或部署在同网段可显著提升；引擎核心层（190 万行/秒）远未饱和。
3. **CDC 配置易用性**：建议在代码里让官方 `database-names` 存在时忽略 legacy `table-name` 默认值，避免“0 表匹配”的坑。
4. 引擎数据面仍存在每条记录两次 Box Future 分配（API trait 所限），如需进一步降低 CPU 开销可考虑泛型化热路径（收益预计小于本次修复的 1%）。
