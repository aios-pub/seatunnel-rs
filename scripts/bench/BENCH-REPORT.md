# canal-sync-consolidated 真实环境模拟压测报告

日期：2026-08-28 ｜ 环境：macOS (Apple Silicon)，Docker MySQL 8.0.46 (13306, ROW binlog + GTID + full row image)、Kafka 7.6 (9092)、RabbitMQ 3 (5672) ｜ 引擎：release 构建

压测对象：`jobs/configs/canal-sync-consolidated.yaml` 的同构本地版
（`scripts/bench/canal-sync-bench.yaml`，5 条 pipeline / 106 张表 / 19 条 topic-routes /
5 组跨 topic 复投 / checkpoint 5s），单机（hybrid 1 节点）与集群（3 节点 Raft）各跑一轮阶梯负载。

## 一、压测前自检优化（先优化后测试）

| # | 优化 | 位置 | 效果 |
|---|------|------|------|
| 1 | TopicRouter 每表路由缓存（`HashMap<String, Arc<Vec<String>>>`） | connector-kafka | 每条消息 19 次正则匹配 + 重复字符串渲染 → 首次解析后仅引用计数克隆 |
| 2 | `producer.*` 配置透传到 librdkafka | connector-kafka | linger/压缩/batch 等全部可在 job YAML 里调，无需改代码；默认行为不变 |
| 3 | 消除 fan-out/单投递路径 key+payload 多余 clone（末路 move 语义） | connector-kafka | 单 topic 投递（最常见）payload 零克隆 |

## 二、压测过程中发现并修复的缺陷（真实环境验证的价值）

| # | 现象 | 根因 | 修复 |
|---|------|------|------|
| 1 | P2/P3/P4（仅 `table-names`/`table-pattern`，无 `database-names`）零捕获，schema watcher 0 个 | `TableSelector::matches` 先做 database 门禁，而 databases 列表只从 `database-names` 填充；且 legacy 折叠把缺省库名 "seatunnel" 塞进门禁 | 表级选择（全限定 refs/patterns）自行生效，database 列表仅收窄；有官方 table 配置时不再折叠 legacy 默认库（cdc-base） |
| 2 | RabbitMQ sink 任务卡死在 open，0 消费 | `amqp_uri()` 把默认 vhost "/" 削成空串 → "vhost not found" | vhost 保持并编码为 `%2F`（connector-rabbitmq） |
| 3 | vhost 修好后 publish 404 杀死 channel | sink 从不声明 exchange/queue/binding | sink 连接时幂等声明 durable direct exchange + queue + bind；source 的 bind 前同样补声明 |
| 4 | web 控制台 /metrics 在引擎重启后冻结（定格旧值） | poller 的 gRPC 调用无超时，连接楔死后任务永久挂起 | 刷新周期加 interval 级超时 + `seatunnel_web_refresh_{cycles,failures}_total`/`last_ok_unix_ts` 存活指标，数据过期可观测 |

另修复指标观测点缺陷：投递 future 只在积压 >8192 或 checkpoint 边界被收割，
延迟 EMA 实测的是 checkpoint 周期而非真实投递延迟 → 新增 `reap_completed`（`now_or_never` 非阻塞扫描）。

## 三、单机版（hybrid 单节点）

阶梯：500 → 2000 → 5000 → 10000 rows/s（每档 60s），负载混合 70% INSERT / 25% UPDATE / 5% DELETE。

- 源端写入 886,000 行事件（762,450 / 114,100 / 9,450）；10k 档实际达成 **7,214 rows/s —— 瓶颈是负载生成器**（docker exec 单 mysql 管道），非引擎
- 引擎 4 条 Kafka pipeline 全部实时捕获（~974k 事件，含 UPDATE 逐行事件口径），`failed=0`，`in_flight` 有界（≤1.5k）
- Kafka 9 个 topic 实收 **1,654,329** 条消息（跨 topic 复投放大）；**`canal_sync_route_unmatched`=0 —— 106 张表全部命中路由，零漏配**
- P5 RabbitMQ 当轮因 vhost 缺陷卡死（上表 #2），修复后在集群轮验证通过

## 四、集群版（3 节点 Raft，5 条 pipeline 分布到 3 个节点）

- 源端 946,000 行事件；10k 档达成 **8,251 rows/s**（同受负载生成器限制）
- 引擎 5 条 pipeline 合计捕获 **1,101,966** 事件：p0=641,014 / p1=194,460 / p2=212,440 / p3=10,405 / p4(RabbitMQ)=43,647
- Kafka 本轮新增 **1,104,368** 条消息；RabbitMQ `queue_canal_sync_user` 积压 43,701 ≈ p4 处理数（1:1 投递）；`unmatched=0`、`failed=0`
- **吞吐与单机持平**：raft/心跳/gRPC 调度不构成数据路径开销；引擎进程 CPU 峰值 15.7%（leader+worker 节点）

## 五、producer 调优 A/B（5000 rows/s × 90s，集群）

`producer.linger.ms: 5` + `producer.compression.codec: lz4`（纯 YAML 透传，见 `canal-sync-bench-tuned.yaml`）：

| pipeline | 基线 ema_avg / ema_max (ms) | 调优后 ema_avg / ema_max (ms) |
|---|---|---|
| p0 (62 表 19 路由) | 187 / 648 | 287 / **331** |
| p1 (ailearn 19 表) | 284 / 1570 | 219 / **253** |
| p2 (recommand 22 表) | 601 / 4035 | **193 / 196** |

结论：**尾延迟塌缩一个数量级**（p2 max 4035→196ms，p1 1570→253ms），均值更平稳，在途更低。
本地延迟绝对值（~200-300ms）主要是 docker 端口转发 + acks=all 的往返，流水线化下不影响吞吐。

## 六、结论与生产建议

1. **链路健康**：全部表捕获、全路由命中、零投递失败、checkpoint 无塌陷；本机测试瓶颈始终在压测客户端/源端，不在引擎。
2. **高吞吐 CDC 建议开启** `producer.linger.ms: 5` + `producer.compression.codec: lz4`（生产配置加两行即可，见 `scripts/bench/canal-sync-bench-tuned.yaml`）。
3. 仅 `table-names`/`table-pattern` 的管道（P2/P3/P4 形态）在修复前完全静默不工作 —— 升级包含此修复前不要单独使用该形态。
4. 观测链路（web /metrics + 窗口化 sink 指标）在压测全程可用，且现在能自我诊断数据过期。

## 七、工具与数据（全部在 `scripts/bench/`，可复用复跑）

- `gen_schema.py` → `init_bench_schema.sql` + `bench_schema.json`（106 表确定性生成）
- `canal-sync-bench.yaml` / `canal-sync-bench-tuned.yaml`（基线/调优 job）
- `gen_load.py`（阶梯混合负载，毫秒基数 id 防冲突）、`collect_metrics.sh`（5s 采样引擎+Kafka offset）
- `cleanup_bench.sql`（收尾清理）、样本数据：`standalone.metrics` / `cluster.metrics` / `tuned_ab.metrics` / `*_load.csv`

## 八、压测后清理（已执行）

7 个压测库整体 DROP；`RESET MASTER` 收缩 binlog（MB 级 → 157 字节）；10 个压测 topic 删除；RabbitMQ 容器停止。mysql/kafka 容器保留运行。

## 回归

`cargo test --workspace`：**383 passed / 0 failed**（含新增：路由缓存、producer 透传、CDC 选择器 table-names-only/table-pattern-only、RabbitMQ vhost 编码用例）。
