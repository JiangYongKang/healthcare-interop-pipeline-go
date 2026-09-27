# 本地临床数据互操作管道（Go）

用本地样例消息文件模拟上游临床系统，把**不同格式与版本**的消息经过
**解析 → 归一化 → 去标识 → 落盘**四个阶段，产出稳定、可追溯、可复现的结果。
在此之上提供**事件一致性管理**：同一临床事件跨格式/跨版本归入同一事件链，
内容冲突进入待复核并由本地复核结论确定性裁决，支持延迟到达处理与一致性回放，
全程可解释、可复核、可回放。

仅使用 Go 标准库，无外部服务依赖；输出为本地 JSONL 文件。

## 1. 支持的格式与版本范围

| 格式 | 线格式 | 支持版本 | 支持事件 |
|---|---|---|---|
| HL7 v2（简化管道编码方言） | 段以 `\r`（兼容 `\r\n`/`\n`）分隔，分隔符固定为 `\| ^ ~ \\ &` | **2.3**、**2.5.1** | `ORU^R01` → Observation，`PPR^PC1` → Condition，`ADT^A04` → Encounter |
| FHIR（简化 JSON 方言） | UTF-8（容忍 BOM）；裸资源 `{"resourceType":...}` 或信封 `{"fhirVersion":...,"resource":{...}}` | **STU3 (3.0.x)**、**R4 (4.0.x)**（缺省按 R4 处理） | Observation、Condition、Encounter |

必备段/字段：HL7 必须含 MSH（首段）、PID、OBX；FHIR 必须含 `resourceType`、
`id`、`subject.reference=Patient/<mrn>`、代码、业务时间。

未知段（如 `ZZZ`）与未知 JSON 字段（含 `extension`）**不会导致解析失败或错列**：
其原始叶子值被采集，随后在去标识阶段替换为令牌；已识别结构严格按字段名映射。

### 解析失败不会被静默跳过，每条都有可区分的拒绝原因

拒绝原因（稳定机器码，写入决策日志）：

- 结构/编码类（对外映射 `FORMAT_ERROR`）：`EMPTY_PAYLOAD`、`ENCODING_INVALID`
  （非 UTF-8、非法转义、错误的 MSH-2 分隔符）、`STRUCTURE_TRUNCATED`
  （缺段、首段非 MSH、JSON 截断）、`UNSUPPORTED_FORMAT`、`UNSUPPORTED_VERSION`、
  `JSON_INVALID`（语法错、重复键、尾随数据）、`EMPTY_ENVELOPE`、`INVALID_COMPONENT`、`INVALID_TIME`
- 业务校验类（对外映射 `VALIDATION_ERROR`）：`MISSING_EVENT_ID`、
  `MISSING_PATIENT_ID`、`MISSING_EVENT_TYPE`、`UNSUPPORTED_EVENT_TYPE`、
  `MISSING_CODE`、`MISSING_EVENT_TIME`、`INVALID_STATUS`

## 2. 版本兼容与缺省语义（确定且稳定）

| 场景 | 旧版本（2.3 / STU3） | 新版本（2.5.1 / R4） |
|---|---|---|
| HL7 OBX-11 状态为空 | `preliminary` | `final` |
| HL7 OBX-3.3 代码系统为空 | `L`（本地码） | `LN`（LOINC） |
| HL7 OBX-6 单位为空 | `unspecified` | `unspecified` |
| FHIR Observation 状态为空 | `preliminary` | `preliminary` |
| FHIR Condition `inactive` | `resolved`（STU3 兼容映射） | `resolved` |
| FHIR Quantity 单位为空 | `1`（STU3 单位缺省语义） | `unspecified` |
| 生效时间缺失 | 回退为事件时间（两格式一致） | 同左 |

时间统一归一为 RFC3339 UTC（HL7 `YYYYMMDD[HH[MM[SS]]]`、FHIR RFC3339/`YYYY-MM-DD`）。
状态枚举跨格式归一到：`final / preliminary / amended / active / resolved / entered-in-error`。

## 3. 去标识规则

- 算法：`HMAC-SHA256(salt, 命名空间 + 真实值)`，取 12 位十六进制摘要；
  占位形如 `PAT-a1b2c3d4e5f6`（患者）与 `TOK-…`（其它敏感值）。
- **一致性**：同一真实 MRN 在任何消息、任何格式（HL7 PID-3 与 FHIR
  `Patient/<mrn>`）、任何时间、任何 goroutine 下都映射到同一个占位值，
  因此**跨记录的患者纵向关联保持可用**；不同 MRN 以压倒性概率得到不同占位值。
- 映射是**纯函数、无共享可变映射表**，从根本上消除并发下"同一真实值被分配
  多个占位值"的竞态窗口。盐固定（配置项 `deidSalt`），故占位值可复现；
  更换盐即得到一套不可关联的新占位值。
- 患者姓名（HL7 PID-5）、FHIR `subject.display`、未知/扩展字段等一切
  非结构化叶子值替换为字段级令牌；**对外的 `records.jsonl` 中不出现任何原始 PHI**。
- 临床事实（编码、代码系统、检验值、单位、时间、状态）原样保留以维持可用性。

## 4. 幂等、乱序与确定性裁决规则

- **幂等键**：`eventId`（HL7 MSH-10 / FHIR `id`）。业务内容指纹为除到达元数据外
  全部业务字段（含未知字段）的 SHA-256。完全相同的重复投递判定为
  `DUPLICATE`，不再产生记录、不占容量，无论投递多少次、跨不跨重启。
- **修订裁决规则（只看内容，与到达顺序和来源格式无关）**：
  生效时间 → 状态秩（`amended > final/active/resolved > preliminary > entered-in-error`）。
  来源格式/版本**不参与**排序，任何格式都不会"天然压过"另一种格式。
  - 较新修订后到：`ACCEPTED` 并记录"取代先前版本"；
  - **较旧修订后到（乱序）：绝不静默覆盖较新数据**，标记 `OUT_OF_ORDER`，
    原因中给出双方时间与状态；旧修订仍以只追加方式留在 `revisions.jsonl` 可追溯；
  - 同秩且临床事实一致（仅表示层差异：显示文本、扩展字段等）：保留不应用，
    标记 `OUT_OF_ORDER`，**不视为冲突**；
  - 同秩且临床事实不一致：**不按到达先后或来源格式静默覆盖**，见 §5。
- 无论乱序消息先后如何到达，最终胜出记录集合完全一致（可复现，见
  `TestArrivalOrderIndependence`）。
- 快照按 `(eventTime, eventId)` 排序输出，乱序消息因此在结果视图中归位。

## 5. 事件一致性视图（事件链）

同一临床事件（同一 `eventId`）无论来自哪种格式或版本，都归入**同一条事件链**。
`Engine.Chain(ctx, eventId)` / `Engine.Chains(ctx)` 返回链视图：

- `status`：`EFFECTIVE`（有效版本无未决冲突）或 `PENDING_REVIEW`（待复核）；
- `current` + `currentDigest`：当前有效版本（已去标识）及其内容指纹；
- `deliveries[]`：每次非重复投递的到达序号、来源格式/版本、生效时间、判定结果，
  以及相对当前有效版本的关系：`CURRENT / SUPERSEDED / STALE / CONFLICT_PENDING / REVIEW_PINNED`；
- `reviews[]`：已应用的复核结论（见 §6）；
- `patientRef`：患者假名（`PAT-…`），跨链纵向关联保持可用。

**视图中只有去标识数据**：患者假名与字段令牌可关联、不可还原，原始 MRN、
姓名等敏感值不会出现在视图、复核或回放流程中（`TestEngineCrossFormatSameEventChain`
断言视图 JSON 中不含任何原始 PHI）。

## 6. 冲突复核与确定性裁决

同秩投递的**临床事实**不一致时，系统给出稳定、可区分的冲突原因并进入待复核：

| 冲突原因 | 触发条件 |
|---|---|
| `CONFLICT_PATIENT` | 同一事件 id 对应不同患者假名 |
| `CONFLICT_EVENT_TYPE` | 事件类型不一致 |
| `CONFLICT_CODE` | 检验/诊断代码或代码系统不一致 |
| `CONFLICT_VALUE` | 同秩但结果值/单位不一致 |
| `CONFLICT_STATUS` | 同秩但状态不一致 |

- 冲突投递被**保留**在事件链中（`CONFLICT_PENDING`），判定为 `CONFLICT`，
  **当前有效版本原地保留**，对外快照不变；
- 复核结论通过 `Engine.Review(ctx, ReviewRequest)` 提交：
  `USE_DELIVERY`（指定链中某次投递为有效版本）或 `KEEP_CURRENT`（确认当前版本）；
- 结论以 `reviews.jsonl` + `REVIEW_APPLIED` 决策**本地落盘并驱动后续处理**：
  链状态、对外快照与派生数据在同一临界区内原子更新；
- **幂等**：`reviewId` 相同的重复结论返回原记录并标记 `duplicate`，
  不产生重复状态或重复日志；同一 `reviewId` 携带不同结论被拒绝（`REVIEW_ID_CONFLICT`）；
- 复核目标必须是链中保留的投递（按到达序号），否则 `UNKNOWN_DELIVERY`。

## 7. 延迟到达与一致性回放

- 较旧/较低优先级的消息晚到：`OUT_OF_ORDER` 保留可追溯，**不改变当前有效结果**；
- 可接受的新修订（更新生效时间或 `amended`）或复核结论到达后：事件链、
  对外结果与派生数据（`records.jsonl`，tmp+rename 原子重写）在**同一临界区内**
  原子更新，崩溃重开时由日志重放重新发布派生数据，不留半完成状态；
- **一致性回放** `Engine.SubmitReplay(ctx)`：从只追加日志重建全部链状态，
  与增量维护的状态逐链比对（`ReplayReport.mismatches` 应为 0），然后原子替换。
  从历史重新处理与在现有状态上增量处理得到**相同的有效结果**
  （`TestReplayEqualsIncremental`、`TestRestartReplayRestoresEverything`）；
- 回放任务进入**有界队列**逐个执行；回放幂等，重复执行效果相同。

## 8. 异常链路的对外错误类型

对外只有四类稳定错误，底层解析文本不出现在对外 message 中（仅保留在本地
决策日志的 `detail` 字段）：

- `FORMAT_ERROR`：上游格式错误（畸形、截断、非法编码、不支持的格式/版本）
- `VALIDATION_ERROR`：业务校验失败（必填缺失、非法枚举等）
- `STORAGE_CONFLICT`：重复、乱序陈旧、容量上限
- `UNAVAILABLE`：管道关闭、持久化 IO 故障、等待并发槽时上下文取消

## 9. 并发与资源上限

- `maxInFlight`：并发处理消息的硬上限。批量目录摄入采用**并行解析/去标识、
  按输入顺序串行提交**：既限制内存与 CPU 占用，又保证对固定有序输入的
  重复/乱序判定标签可复现；单条 `Ingest` 调用则使用有界信号量，在分配解析
  状态前获取槽位，上下文取消立即返回 `UNAVAILABLE`。
- `maxRecords`：主库接受的**去重后不同事件**数上限（0=不限）；重复与乱序
  投递不占容量。超限策略 `-policy`：
  - `reject`（默认）：返回 `STORAGE_CONFLICT` 并记 `REJECTED`；
  - `degrade`：写入 `degraded.jsonl` 降级日志并记 `DEGRADED`，主库不受影响。
- `maxPendingReviews`：同时处于 PENDING_REVIEW 的事件链上限（0=不限）。
  超限同样按 `-policy` 拒绝（`REJECTED` + `STORAGE_CONFLICT`）或降级
  （`DEGRADED` 写入降级日志），两种路径都留在决策日志中可查询，
  不会留下"半条"冲突投递。
- `maxReplayTasks`：一致性回放任务队列上限（0 视为 1）。队列满时按 `-policy`
  拒绝（`REPLAY_BACKLOG_FULL`）或降级跳过，均写入决策日志；已入队任务
  必定执行完成，回放本身幂等。
- 存储层所有判定在单把互斥锁内串行完成；修订/复核与决策写盘各自单条 JSONL +
  `fsync`，并以"先追加事实、再追加决策、决策为事实源"的方式提交。
  进程在两次写之间崩溃时，重放只采纳有对应决策的条目，**截断/孤立行会在重开时
  自动截断修复，不会残留部分写入状态**（见 `TestTornJournalHealing`）。

## 10. 输出文件（`-out` 目录）

| 文件 | 内容 |
|---|---|
| `records.jsonl` | 派生数据：去标识后的当前有效版本，按 `(eventTime, eventId)` 排序；每次改变有效集合的提交（新修订/复核/回放）后以 tmp+rename **原子重写**，重开时由日志重放重新发布 |
| `revisions.jsonl` | 所有非重复修订（含乱序旧修订与冲突投递），只追加，用于审计与重放 |
| `decisions.jsonl` | 每条消息/系统事件的判定：`ACCEPTED/REJECTED/DUPLICATE/OUT_OF_ORDER/CONFLICT/DEGRADED/REVIEW_APPLIED/REPLAYED` + 原因/错误类型/格式/版本 |
| `reviews.jsonl` | 已应用的复核结论（reviewId 幂等键），只追加 |
| `degraded.jsonl` | 容量/积压超限降级模式下的记录 |

不加 `-reset` 重开时会重放上述日志，去重记忆、待复核状态与复核结论跨重启保持，
天然支持断点续跑。

## 11. 本地验证

```bash
# 构建
go build ./...

# 跑全部样例（testdata/samples 下 25 个文件：合法/畸形/重复/乱序/跨版本/未知字段）
go run ./cmd/ingest -dir testdata/samples -out output -reset

# 常用参数
#   -max-records N    去重后事件上限（0 不限）
#   -policy reject|degrade
#   -max-inflight N   并发上限
#   -max-pending-reviews N  待复核链上限（0 不限）
#   -max-replay-tasks N     回放任务队列上限
#   -replay            摄入后执行一次一致性回放并打印报告
#   -reset            运行前清空输出目录

# 一致性语料（跨格式同事件归类 / 冲突待复核 / 延迟到达 / amended 取代）
go run ./cmd/ingest -dir testdata/consistency -out output-cons -reset -replay

# 测试（含竞态检测；日志中打印消息标识 eventId 与判定依据）
go test -race ./...
go test ./... -v     # 查看每条用例的 eventId 与 basis= 判定依据
```

样例语料覆盖：HL7 2.3/2.5.1 与 FHIR STU3/R4 合法消息；同一事件重复投递（含
跨格式同 id）；旧版本乱序重投；缺 MRN/缺时间/非法时间/非法状态/非法编码字符/
结构截断/非法 UTF-8/重复 JSON 键/未知版本；以及带未知扩展字段的前向兼容消息。
`testdata/consistency` 另覆盖：跨格式/跨版本同一事件归类、同时间冲突值、
患者不匹配冲突、旧修订晚到与 `amended` 修订取代（见 `docs/SAMPLE_CATALOG.md`）。

预期判定分布（顺序摄入排序后的语料）：9 接受、2 重复、1 乱序、13 拒绝；
无论并发调度如何，最终均为 9 条去标识记录，且同一患者跨格式占位值一致。
一致性语料（9 条）：5 接受、2 乱序、2 冲突（待复核），回放报告 `mismatches=0`。
