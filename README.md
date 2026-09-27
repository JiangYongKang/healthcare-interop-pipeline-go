# 本地临床数据互操作管道（Go）

用本地样例消息文件模拟上游临床系统，把**不同格式与版本**的消息经过
**解析 → 归一化 → 去标识 → 落盘**四个阶段，产出稳定、可追溯、可复现的结果。

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

## 4. 事件链、幂等与乱序规则

- **事件链**：同一临床事件由事件 id（HL7 MSH-10 / FHIR `id`，上游系统对同一事件
  跨格式/跨版本保持不变）归组为一条链 `CHAIN-<hash>`。同一事件无论来自 HL7 2.3/2.5.1
  还是 FHIR STU3/R4，都进入同一条链；链内保留每次投递的来源（格式/版本）、**各自的
  患者占位**、生效时间、到达序号与最终状态，以及当前有效版本。链视图只含去标识数据
  （患者为 `PAT-` 占位值），患者纵向关联通过占位值保持可用。
  - **链状态**：`EFFECTIVE`（有当前有效版本且无未决冲突）/ `PENDING_REVIEW`
    （存在未决内容或患者冲突，最后有效版本继续作为对外结果）。
  - 同一事件 id 上出现不同患者时，**链的患者关联与有效版本都不被改写**；冲突投递
    仍保留在链内，其患者占位可在投递视图中区分，等待复核裁决。
- **幂等键**：`eventId`。系统维护两类指纹：
  - **业务指纹** `businessDigest`：除到达元数据外全部业务字段（含来源格式/版本、
    代码系统、未知字段）的 SHA-256，用于**精确重复投递**判定——逐字节等价的重投
    判定为 `DUPLICATE`，不再产生记录、不占容量，无论投递多少次、跨不跨重启。
  - **临床语义指纹** `clinicalDigest`：仅事件类型、归一状态、代码、结果值、单位
    （单位大小写不敏感），与格式/版本无关，用于同一生效时间下的内容一致性判定
    （见第 5 节）。跨格式但临床等价的投递不是重复、也不是冲突，按修订规则归位。
- **修订裁决规则（与到达顺序无关，纯确定性）**
  - 先比较**临床生效时间**：生效时间更晚的修订取代更早的修订；生效时间更早
    的修订晚到则标记 `OUT_OF_ORDER`，绝不覆盖。
  - **同一临床生效时间**下先比较**临床内容语义指纹** `clinicalDigest`
    （事件类型、归一状态、代码、结果值、单位，单位大小写不敏感；**不含**来源格式/版本、
    代码系统与显示名拼写、未知字段）。语义一致才进入版本序比较
    （`2.3 < 2.5.1 < STU3 < R4`）：高版本以 `ACCEPTED` 取代，同序则保留等价结果。
    语义不同即属于内容冲突（见第 5 节），**版本序再高也绝不静默覆盖**——因此
    较旧/较低优先级消息晚到虽不会改变有效结果，但"内容确实不一致"的判定不会因为
    格式/版本排序被跳过。
  - **跨格式/跨版本等价投递**：HL7 `cel` 与 FHIR `Cel`、代码系统缺省差异
    （`L`/`LN`/`http://loinc.org`）等格式方言差异不计入语义分歧。
  - 无论消息谁先到谁后到，有效结果与冲突判定集合完全一致（可复现）。
- 快照按 `(eventTime, eventId)` 排序输出，乱序消息因此在结果视图中归位。

## 5. 冲突复核与确定性裁决

系统区分两类**稳定、可机器区分**的冲突原因；两种情形都只追加保留投递、
**当前有效版本与链状态保持不变**、链进入 `PENDING_REVIEW`：

| 稳定原因码 | 判定条件 | 保留/不改写的内容 |
|---|---|---|
| `CONTENT_CONFLICT_SAME_REVISION` | 同一事件 id、**同一患者**、**同一临床生效时间**，但临床语义指纹不同（归一状态/结果值/代码/单位对不上）。与来源格式、版本序无关：高版本不会静默覆盖。 | 当前有效版本 |
| `PATIENT_MISMATCH_SAME_EVENT` | 同一事件 id 的投递引用了**另一个患者**（患者占位不同）。优先于内容冲突判定。 | 链已有的患者关联与当前有效版本 |

- **判定依据留痕**：决策日志 `reason` 以稳定原因码开头，并附双方版本/格式、版本序、
  生效时间、双方业务指纹（患者不一致时另附双方患者占位），不暴露任何原始 PHI。
  链视图的每次投递都带各自的去标识患者占位，患者不一致可逐投递审计。
- **与到达顺序无关**：冲突 id 为 `CON-<hash(eventId, 双方业务指纹)>`，指纹对按字典序
  排序，同一对内容无论谁先到都得到同一个 id；两条消息以任意顺序到达，有效结果与冲突
  集合一致。较旧/较低优先级消息晚到不会改变有效结果，但同生效时间的内容不一致不会被
  版本序排序跳过（旧版本晚到同样判内容冲突）。
- **冲突生命周期**：冲突写入 `conflicts.jsonl` 并可通过 `PendingConflicts` 查询；
  重复检测到同一对内容不会产生重复冲突记录。
- **复核结论**：`ResolveConflict(conflictId, KEEP_CURRENT|ACCEPT_INCOMING, reviewer)`
  - `KEEP_CURRENT`：保留当前有效版本；`ACCEPT_INCOMING`：把冲突投递提升为有效版本
    （含患者不一致冲突：复核人可据此把链的有效版本切换到另一患者的投递）。
  - 结论先写 `reviews.jsonl`（提交点）并 fsync，再在锁内原子更新链状态与对外结果，
    然后才写信息性 `REVIEW_APPLIED` 决策，不会留下只完成一半的复核。
  - **幂等**：同一结论重复应用返回既有结论、不写新日志、不改状态、不产生重复记录；
    对已结冲突给出不同结论则拒绝（`CONFLICT_ALREADY_RESOLVED`）。未知冲突与非法结论
    分别返回 `UNKNOWN_CONFLICT` / `INVALID_RESOLUTION`。
- **待复核积压上限**：`maxPendingReviews`（0=不限，默认 1000），对两类冲突同等生效。
  超限时按 `-policy` 处理：`reject` → 该投递 `REJECTED`（`STORAGE_CONFLICT`）；
  `degrade` → 写入 `degraded.jsonl` 并记 `DEGRADED`。两种策略下积压都可查询，且无
  部分写入（容量计数只在新冲突真正登记时增长）。
- **重启与回放**：冲突原因在重放时由"双方患者占位 + 临床内容"重新派生（患者不一致
  优先），因此重启重建与运行时一致性回放得到完全相同的冲突原因、状态与裁决；
  `Replay()` 逐链比对有效版本指纹、冲突原因、状态与裁决结论。

## 6. 一致性回放

- **重开即回放**：不加 `-reset` 重开时，存储层从 `revisions.jsonl` + `decisions.jsonl`
  + `reviews.jsonl` 按序号顺序重建链状态（修订与其提交决策按事件配对，复核结论按
  序号归并），**从历史重新处理与增量处理得到相同的有效结果**。
- **运行时校验**：`Replay()` 用同一重建逻辑在全新内存模型上重放历史，并与当前状态
  逐链比对（有效版本指纹、冲突状态），输出 `ReplayReport`（链数/投递数/冲突数/是否
  一致/差异明细），每次回放（含被拒绝的）都写入 `replay.jsonl` 可查询。
- **回放并发上限**：`maxReplayTasks`（0=不限，默认 2）。超限的回放请求被拒绝
  （`UNAVAILABLE` / `REPLAY_LIMIT`）并记入 `replay.jsonl`；槽位释放后即可正常回放。
- **重复回放**：重启后重投全部历史消息，结果全部为 `DUPLICATE`，有效状态不变。

## 7. 异常链路的对外错误类型

对外只有四类稳定错误，底层解析文本不出现在对外 message 中（仅保留在本地
决策日志的 `detail` 字段）：

- `FORMAT_ERROR`：上游格式错误（畸形、截断、非法编码、不支持的格式/版本）
- `VALIDATION_ERROR`：业务校验失败（必填缺失、非法枚举等）
- `STORAGE_CONFLICT`：重复、乱序陈旧、内容冲突（待复核）、容量/积压上限
- `UNAVAILABLE`：管道关闭、持久化 IO 故障、等待并发槽时上下文取消

## 8. 并发与资源上限

- `maxInFlight`：并发处理消息的硬上限。批量目录摄入采用**并行解析/去标识、
  按输入顺序串行提交**：既限制内存与 CPU 占用，又保证对固定有序输入的
  重复/乱序判定标签可复现；单条 `Ingest` 调用则使用有界信号量，在分配解析
  状态前获取槽位，上下文取消立即返回 `UNAVAILABLE`。
- `maxRecords`：主库接受的**去重后不同事件**数上限（0=不限）；重复与乱序
  投递不占容量。超限策略 `-policy`：
  - `reject`（默认）：返回 `STORAGE_CONFLICT` 并记 `REJECTED`；
  - `degrade`：写入 `degraded.jsonl` 降级日志并记 `DEGRADED`，主库不受影响。
- `maxPendingReviews` / `maxReplayTasks`：待复核冲突与并发回放上限，见第 5、6 节。
- 存储层所有判定在单把互斥锁内串行完成；修订与决策写盘各自单条 JSONL +
  `fsync`，并以"先追加修订、再追加决策、决策为事实源"的方式提交（复核结论以
  `reviews.jsonl` 为提交点）。
  进程在两次写之间崩溃时，重放只采纳有对应决策的修订，**截断/孤立行会在重开时
  自动截断修复，不会残留部分写入状态**（见 `TestTornJournalHealing`）。

## 9. 输出文件（`-out` 目录）

| 文件 | 内容 |
|---|---|
| `records.jsonl` | 去标识后的最终规范记录，按 `(eventTime, eventId)` 排序（每次运行由快照重写） |
| `revisions.jsonl` | 所有非重复修订（含乱序旧修订），只追加，用于审计与重放 |
| `decisions.jsonl` | 每条消息的判定：`ACCEPTED/REJECTED/DUPLICATE/OUT_OF_ORDER/CONFLICT/DEGRADED/REVIEW_APPLIED` + 原因/错误类型/格式/版本/链与冲突 id |
| `degraded.jsonl` | 容量/积压超限降级模式下的记录 |
| `conflicts.jsonl` | 检测到的冲突（id、稳定原因 `CONTENT_CONFLICT_SAME_REVISION`/`PATIENT_MISMATCH_SAME_EVENT`、双方内容指纹、双方患者占位、状态与裁决），审计用 |
| `reviews.jsonl` | 复核结论（提交点）：冲突 id、裁决、胜出指纹、复核人、序号 |
| `replay.jsonl` | 每次一致性回放的报告（含被积压上限拒绝的回放） |

不加 `-reset` 重开时会重放上述日志，去重记忆跨重启保持，天然支持断点续跑。

## 10. 本地验证

```bash
# 构建
go build ./...

# 跑全部样例（testdata/samples 下 27 个文件：合法/畸形/重复/乱序/跨版本/跨格式同事件/冲突/未知字段）
go run ./cmd/ingest -dir testdata/samples -out output -reset

# 常用参数
#   -max-records N    去重后事件上限（0 不限）
#   -policy reject|degrade
#   -max-inflight N   并发上限
#   -reset            运行前清空输出目录

# 测试（含竞态检测；日志中打印消息标识 eventId 与判定依据）
go test -race ./...
go test ./... -v     # 查看每条用例的 eventId 与 basis= 判定依据

# 只跑事件一致性/冲突复核相关用例
go test ./pipeline/ -run 'TestChain|TestConflict|TestResolve|TestHigherVersion|TestCrossVersion|TestPatientMismatch|TestLateLowerVersion|TestStatusDisagreement|TestConcurrentMixed' -v

# 复现"高版本静默覆盖"缺陷修复：同一事件同临床时间、2.3 先到、R4 携带不同值，
# 结果必须是 CONFLICT/CONTENT_CONFLICT_SAME_REVISION 且有效版本仍是 2.3
go test ./pipeline/ -run TestHigherVersionDoesNotSilentlyOverwrite -v

# 复现跨格式/跨版本内容冲突的两种到达顺序（冲突 id 相同、首个到达者保持有效）
go test ./pipeline/ -run 'TestChainCrossFormatContentConflictBothOrders|TestCrossVersionContentConflictOrderIndependent' -v

# 复现患者不一致（两种到达顺序、积压策略、复核原子性与重启一致性）
go test ./pipeline/ -run 'TestChainPatientMismatchBothOrders|TestPatientMismatch' -v
```

样例语料覆盖：HL7 2.3/2.5.1 与 FHIR STU3/R4 合法消息；同一事件重复投递（含
跨格式同 id）；跨格式同一事件归链（EVT-0001 的 HL7 2.3 与 FHIR R4）；旧版本乱序
重投；同坐标不同内容的冲突投递（EVT-0004，进入待复核）；缺 MRN/缺时间/非法时间/
非法状态/非法编码字符/结构截断/非法 UTF-8/重复 JSON 键/未知版本；以及带未知扩展
字段的前向兼容消息。

预期判定分布（顺序摄入排序后的语料）：9 接受、2 重复、2 乱序、1 冲突、13 拒绝；
无论并发调度如何，最终均为 9 条去标识记录、9 条事件链（EVT-0004 为
PENDING_REVIEW），且同一患者跨格式占位值一致。运行结束打印每条链的
`[chain]` 视图与 `[pending-review]` 待复核清单。
