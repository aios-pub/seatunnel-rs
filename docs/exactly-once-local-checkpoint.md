# 本地模式生产级 Checkpoint / Exactly-Once / 故障恢复实现与验证报告

日期:2026-08-27
范围:`seatunnel run -m local`(本地模式)全链路 checkpoint 协调、两阶段提交(2PC)、kill -9 故障恢复,以及配套的生产级验证。集群模式路径保持原行为不变。

---

## 1. 背景与目标

此前本地模式**完全没有 checkpoint**:

- `run_local` 从不挂 `CheckpointListener`、不传 checkpoint interval,restore state 永远为 `None`;
- job id 每次进程随机生成,重启后无从恢复;
- engine 中存在一套"Java 风格"但从未接线的脚手架(`CheckpointCoordinator`、`BarrierTracker` 等);
- 真实运行的路径(集群 worker)是每任务按自己的时钟自触发、per-task checkpoint id、`prepare_commit` 返回的 CommitInfo 直接丢弃 —— 2PC 第二阶段从未执行。

本次实现参照 Java Zeta 引擎机制(CheckpointCoordinator 全局触发 → barrier 切割 → 全任务 ACK → CompletedCheckpoint 落盘 → `notifyCheckpointComplete` 广播 → committer 二段提交 → 重启从最新 checkpoint 恢复并清算遗留事务),在本地模式落地,并以三层验证证明其正确性。

## 2. 与 Java Zeta 的机制对照

| Java Zeta(seatunnel-engine-server) | Rust 本地模式实现 | 位置 |
|---|---|---|
| `CheckpointManager` / `CheckpointCoordinator`(按 pipeline) | `LocalCheckpointDriver`(按 job,单进程内 pipeline 间无数据交换,"对齐"退化为"所有任务在各自安全点切割") | `engine-core/src/local_checkpoint.rs` |
| `CheckpointIDCounter` 自增全局 checkpoint id | driver 全局自增,恢复后从 `checkpoint_id + 1` 续编 | 同上 |
| `CheckpointBarrierTriggerOperation` 只发给 source 任务 | gate 触发每个存活 TaskGroup(链式任务内 source→sink 本就同任务,无需跨算子流动) | `CheckpointGate::trigger` |
| 任务 ACK(`TaskAcknowledgeOperation`)→ 全部 ACK 后 `completePendingCheckpoint` | mpsc 上报 `TaskCheckpointReport` → 齐全后聚合为 `CheckpointEnvelope` | `handle_message` / `complete_checkpoint` |
| ProtoStuff 序列化 + CheckpointStorage(localfile 插件)持久化 | JSON envelope,`tmp 写 + fsync + rename + 目录 fsync` 原子落盘,保留最近 N 个 | `LocalCheckpointStore` |
| `CheckpointFinishedOperation` → `notifyCheckpointComplete` → `SinkCommitter.commit` / 提交消费位点 | 广播 `Completed(id)` 事件 → TaskGroup 执行 committer.commit + `SourceReader::notify_checkpoint_complete`(Kafka source 位点改在此提交) | `TaskGroup::handle_checkpoint_event` |
| checkpoint 超时(`CHECKPOINT_EXPIRED`) | 30s 超时 → 广播 `Aborted` → committer 回滚,下轮重试 | `abort_checkpoint` |
| `PREPARE_CLOSE_BARRIER_ID` 最终 checkpoint | 任务 EOF/取消时的退出 barrier;SIGINT/SIGTERM → driver 终局 checkpoint(savepoint 语义)后再取消任务 | `FINAL_CHECKPOINT_ID` / `final_checkpoint` |
| 恢复:`getLatestCheckpointStateByType` → `restoreTaskState` → 读者/写者重建 | 启动时 `load_latest` → 读者走现有 `create_source(.., restore_state)`,写者走 `restore_writer`/`restore_from_state_bytes`,driver 续编 id | `LocalCheckpointPlan::restore_from_latest` |
| Kafka sink `prepareCommit(checkpointId)` + `KafkaInternalProducer` 反射 resume 事务 | `SinkWriter::prepare_commit(checkpoint_id)`;rdkafka 无 resumeTransaction,采用 **commit-at-prepare(1.5PC)+ 稳定 transactional.id fence 僵尸** | `connector-kafka/src/lib.rs` |
| `JdbcExactlyOnceSinkWriter` + XA(`XaFacade`) | `JdbcXa`(MySQL `XA START/END/PREPARE/COMMIT/RECOVER` 纯 SQL,真 2PC) | `connector-jdbc/src/xa_sink.rs` |
| `SinkAggregatedCommitterTask.restoreCommit` 清算遗留事务 | 写者 `open()` 时 `XA RECOVER` 清算:窗口 ≤ 已恢复窗口 → COMMIT,否则 ROLLBACK | `XaSinkWriter::recover_prepared` |

## 3. 实现架构

### 3.1 协议时序(每个 checkpoint N)

```
driver                              TaskGroup(source→transforms→sink)
  │ id = next++                            ▲
  ├─ trigger(N) ──────────────────────────►│ 循环顶部,在两次 poll 之间切割:
  │                                        │ 1. sink.prepare_commit(N)   ← 2PC 阶段一(刷写/XA PREPARE)
  │                                        │ 2. sink.snapshot_state()
  │                                        │ 3. reader.snapshot_state()  ← 源位点在刷写之后
  │◄───────── TaskCheckpointReport ────────┤
  │ (全部任务上报后)                        │
  ├─ envelope 原子落盘(fsync)              │
  ├─ Completed(N) ────────────────────────►│ committer.commit()          ← 2PC 阶段二
  │                                        │ reader.notify_checkpoint_complete()
  └─ (超时/失败 → Aborted(N) ──────────────►│ committer.abort())
```

### 3.2 崩溃窗口分析(诚实边界)

- **丢失窗口 = 0(Kafka 与 XA 皆然)**:sink 先刷写/prepare,reader 位点后取;恢复点之前的所有数据要么已可见,要么事务被 fence/回滚后由重放覆盖。
- **Kafka(rdkafka 约束)**:事务在 prepare 阶段即提交(1.5PC),重复窗口仅为 "Kafka commit 应答 → checkpoint 文件 fsync" 之间崩溃(毫秒级);实测 3 次 kill -9 仅 2–3 条重复,依赖消息 key 幂等收敛。rdkafka 无 `resumeTransaction`(Java 需反射 hack),跨进程严格 2PC 不可达 —— 如实记录。
- **JDBC XA**:阶段一 `XA PREPARE` 在 MySQL 内持久;崩溃后 `XA RECOVER` 清算,窗口 ≤ 已恢复窗口的 COMMIT、否则 ROLLBACK,配合 upsert 幂等 → **严格 exactly-once**(表级无重无丢)。
- **CDC 位点边界(本次发现并修复的真实缺陷)**:原实现 `snapshot_state` 返回的是 binlog 流位置,可能领先于"已发给 engine 的行"(缓冲区中还有已解码未发出的行),恢复会静默跳过这些行(实测丢 12 条)。现改为**事务边界位点**:最后一个"行已全部发出"的事务的 XID 之后位置;旋转 binlog 文件时边界失效自动回退。
- **僵尸会话(macOS/docker 端口转发实测)**:kill -9 后 TCP 半开连接让 MySQL 侧会话与 ACTIVE XA 事务长期存活,导致 `XAER_DUPID` 与行锁阻塞。处理:XID 内嵌进程世代号(`{prefix}-{pipeline}-{subtask}-r{epoch}-cp{window}`)+ 启动时清理目标库上持有事务的僵尸会话。

### 3.3 CLI 使用

```bash
# 默认开启 checkpoint(30s),状态目录 ./state(可用 --state-dir / SEATUNNEL_STATE_DIR 覆盖)
seatunnel run -c job.yaml -m local

# 生产建议:固定 job id,重启自动从最新 checkpoint 恢复(binlog 断点续传,不重跑快照)
seatunnel run -c job.yaml -m local --job-id my-cdc-job --state-dir /data/state

# env.checkpoint.interval: 0 关闭;支持嵌套与点号两种写法
# Kafka exactly-once:sink 配置 semantics: exactly-once(或 transactions.enabled: true)
# MySQL 严格 exactly-once:sink 插件 JdbcXa(目标表需预先建好,写入为 upsert)
```

优雅停机:SIGINT/SIGTERM → driver 终局 checkpoint(savepoint)→ 任务退出 barrier 刷尾 → 干净退出。

## 4. 验证结果

### 4.1 引擎层故障注入矩阵(最关键)

`seatunnel-engine-core/tests/local_checkpoint_recovery.rs`:以真实 driver + TaskGroup 驱动一个 XA 语义模型 sink(仅阶段二/恢复清算可见,水位幂等),跨会话共享同一状态目录,`JoinHandle::abort` 模拟 kill -9(无终局 checkpoint、无 close)。断言最终输出严格等于 `1..=watermark` 连续序列(无重无丢)。

| 故障点 | 结果 |
|---|---|
| 阶段一(prepare_commit)完成后立即崩溃(未落盘) | ✅ 无重无丢 |
| 新 envelope 落盘后立即崩溃(阶段二可能未执行) | ✅ 无重无丢 |
| 同一故障点连续崩溃 ×5 | ✅ 无重无丢 |
| 数据流中途随机崩溃 | ✅ 无重无丢 |
| 优雅停机(终局 checkpoint + 尾部清算) | ✅ 无重无丢 |

### 4.2 真实 kill -9 e2e(docker MySQL 8.0.46 + cp-kafka 7.6)

`seatunnel-e2e/tests/checkpoint_recovery.rs`:真实 `seatunnel run -m local` 子进程,写入流量中 **3 次 kill -9 + 1 次 SIGTERM 优雅停机**,同 `--job-id --state-dir` 重启恢复。

| 链路 | 断言 | 结果 |
|---|---|---|
| MySQL-CDC → Kafka(`semantics: exactly-once`,checkpoint 1s) | `read_committed` 消费:240/240 seq 全送达(0 丢失)、无半批可见、重复数有界 | ✅ 通过(243 条消息 / 240 distinct / 3 重复) |
| MySQL-CDC → MySQL `JdbcXa`(checkpoint 1s) | 目标表 `COUNT = DISTINCT = MAX = 240`(每 seq 恰一次,upsert 幂等吸收重放),结束时无遗留 prepared XID,优雅退出码 0 | ✅ 通过 |

辅助验证:重启后 CDC 直接从 binlog 位点续传(日志 `resuming from checkpoint offset`,**不重跑快照** —— ts=0 的种子行不重复出现)。

### 4.3 单元测试

- `local_checkpoint.rs`:store 原子落盘/最新读取/保留清理/job id 文件名安全、driver 聚合完成、恢复续编 id、超时 abort。
- `xa_sink.rs`:xid 组分净化、`XA RECOVER` 十六进制与裸格式解码、writer 状态往返、**跨连接 XA RECOVER 可见性实测**(连不上 MySQL 自动跳过)。
- 其余全仓 294 项测试全绿(`cargo test --workspace`),clippy 无新增告警。

### 4.4 exactly-once 开销压测(与既有多源多 sink 压测同机同法)

单 pipeline、MySQL-CDC → Kafka、1000 rows/s × 20s(19,980 行)、checkpoint 3s;对比"checkpoint 开 + 事务关"与"checkpoint 开 + 事务开(exactly-once)"。探针在运行结束后从 topic 头部消费,故绝对延迟含固定的事后消费偏移(两模式相同),有效对比量是送达数:

| 配置 | 送达 / 19,980 | 送达率 | 探针 p50(含事后偏移) |
|---|---|---|---|
| checkpoint 开,事务关 | 19,865 | 99.4% | 22,265 ms |
| checkpoint 开 + exactly-once | 19,785 | 99.0% | 22,222 ms |

结论:**事务(exactly-once)在本负载下无吞吐损耗**(送达差 0.4% 在探针空闲退避噪声内;p50 差 43 ms 为噪声)。语义差异是可见性:事务模式下消息在 checkpoint 提交时才对 `read_committed` 消费者可见,端到端可见延迟上界增加至多一个 checkpoint 间隔(此处 3s)——与 Java Zeta 行为一致,是 exactly-once 的语义性代价而非性能缺陷。原始数据归档于 `seatunnel-benchmarks/stress/results/eos_bench/`。

## 5. 代码变更清单

| 模块 | 变更 |
|---|---|
| `seatunnel-api` | `SinkWriter::prepare_commit(checkpoint_id)`;`SourceReader::notify_checkpoint_complete`;`CommitterFuture` 别名 |
| `engine-core/local_checkpoint.rs`(新) | gate / envelope / 原子 store / driver,全协议与单测 |
| `engine-core/task_group.rs` | gate 触发的 barrier 切割、完成/中止事件处理(阶段二)、退出 barrier、Done 上报;旧 listener 路径保留给集群 |
| `engine-core/connector_factory.rs` | `create_sink_with_restore` / `create_sink_pipeline`(首次接线 `Sink::create_committer`);修复 `snapshot_state` 双重序列化 bug |
| `engine-core/fanout.rs` | 结构化 per-sink commit info;`FanoutCommitter` |
| `connector-kafka` | 稳定 transactional.id + init fence、checkpoint 对齐事务、writer 状态恢复;source 位点改在 checkpoint 完成后提交 |
| `connector-jdbc/xa_sink.rs`(新) | MySQL XA 真 2PC writer/committer、`XA RECOVER` 清算、世代 xid、僵尸会话清理 |
| `connector-cdc-mysql` | checkpoint 位点改为"最后完全发出事务的 XID 后位置"(修复超前位点丢数据缺陷);binlog 旋转边界失效处理 |
| `seatunnel-cli` | `--job-id`/`--state-dir`、默认开启 checkpoint(30s)、恢复检测、信号优雅停机、tracing 初始化 |
| 文档/测试 | 本报告、README/engine-config 语义更新、故障注入矩阵、kill -9 e2e |

## 6. 已知限制与后续工作

1. **Kafka 严格跨进程 2PC 不可达**(rdkafka 缺 `resumeTransaction`):重复窗口为毫秒级,依赖 key 幂等;若未来 rdkafka 暴露该 API 可升级为真 2PC。
2. **JdbcXa 仅支持 MySQL 协议**(XA 语句纯 SQL 实现);PostgreSQL 两阶段提交(`PREPARE TRANSACTION`)可按同框架扩展。
3. **集群模式仍为 at-least-once**(per-task 时钟 checkpoint);本地模式的协调器协议可作为集群侧 CheckpointCoordinator 落地的参考实现。
4. `XaSinkWriter` 启动时清理目标库上持有事务的会话需要 PROCESS + CONNECTION_ADMIN 权限(专用同步账号标准配置),已记录在注释与文档。
5. 每 record 的 boxed future 等 API 层性能项与既压测报告结论一致,未在本次范围内。

## 7. 复现

```bash
cargo test -p seatunnel-engine-core --test local_checkpoint_recovery -- --nocapture   # 故障注入矩阵
cargo build -p seatunnel-cli
cargo test -p seatunnel-e2e --test checkpoint_recovery -- --nocapture                 # 真实 kill -9 e2e
docker compose up -d mysql kafka                                                      # 前置依赖
```
