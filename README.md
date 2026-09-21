# 本地临床数据互操作管道（Go）

用本地样例消息文件模拟上游系统，将不同**格式与版本**的临床消息依次经过
**解析 → 归一化 → 去标识 → 幂等/乱序落盘**，产出稳定、可追溯、可复现的结果。
所有敏感真实值在对外结果中只以确定性占位值出现，全链路有审计流水。

## 支持的格式与版本

| 格式族 | 支持版本 | 说明 |
---|---|---
| HL7v2（`|`/`^` 管道编码） | `2.3`、`2.5.1` | 识别 MSH / PID / EVN 与自定义 ZSN（源序列号）；其余段（如 ZPD/ZZZ）作为**未知段保留**而非丢弃 |
| FHIR JSON | `DSTU2`（向后兼容）、`R4`（主力）、`R5`（前向兼容） | 未知/新增 JSON 属性**保留不报错**（如 `event.futureScore`、`experimentalExtension`）；DSTU2 的 `family` 数组等旧形态可解析 |

- 版本不在上表 → `FORMAT_ERROR / UNSUPPORTED_VERSION`，不做猜测式解析。
- `fhirVersion` 支持代号（`R4`/`R5`/`DSTU2`）与数字前缀（`4.x`/`5.x`/`1.x`）。

### 解析阶段可区分的拒绝原因（不静默跳过）

| Code | 触发条件 |
---|---
| `MSG_TRUNCATED` | 空载荷；MSH 不完整（早于 MSH-12）；缺少 PID；JSON 文档截断；缺少 `fhirVersion` |
| `ILLEGAL_ENCODING` | 非 UTF-8；含 Tab/CR/LF 以外的 C0 控制字节 |
| `MALFORMED_JSON` | JSON 语法错误；文档后存在多余拼接内容 |
| `UNSUPPORTED_VERSION` | 版本号不在支持范围 |
| `UNKNOWN_FORMAT` | 既不是 MSH 开头的 HL7，也不是 JSON 对象 |
| `MISSING_REQUIRED_FIELD` | 业务必需项缺失：事件 ID、患者 MRN、事件类型、生效时间 |
| `INVALID_VALUE` | 时间/日期非法、性别码无法识别、ZSN 序列号非整数、结构类型不符 |

底层解析细节（偏移量、段字段号、异常栈）只写入**审计的内部字段**，
对外错误消息固定、无 PHI、无解析器细节（见 `internal/apperr`）。

## 归一化与旧版本缺省语义

不同格式被归一成同一个规范模型 `CanonicalEvent`：

- 时间统一为 `YYYY-MM-DDTHH:MM:SS`（HL7 DTM 支持
  `YYYY[MM[DD[HH[MM[SS]]]]]` 精度，本地样例按 UTC 解释）；
  FHIR 接受 RFC3339。
- 性别统一为 `M/F/O/U`；**旧版本缺失时稳定缺省为 `UNK`**。
- 患者标识体系缺失时稳定缺省为 `LOCAL-MPI`；姓名/联系方式/出生日期缺失稳定缺省为空。
- 旧版本没有源序列号时，排序键由生效时间派生，并在记录 `defaults` 中显式登记。
- 每个应用的缺省都落在输出记录的 `defaults` 字段，例如
  `["gender:UNK","mrn-system:LOCAL-MPI","contact:empty","birth-date:empty",...]`，
  使“缺省”与“真实空值”可区分、可复现。
- 未知段/未知字段进入 `unknowns`（位置 + 种类 + 长度，**不含原值**），不导致失败、不错列。

## 幂等与乱序规则

- **幂等键**：业务事件 ID（HL7 `MSH-10`；FHIR `identifier[use=event-id].value`，
  回退首个 identifier / 顶层 `id`）。
- **业务指纹**：对归一化后的业务字段做 SHA-256，**刻意排除**格式、版本、
  生命周期字段与传输层序列号。因此同一业务事件经 HL7 或 FHIR 重复投递，
  指纹一致 → 判为重复，不产生第二条记录（`DUPLICATE_EVENT`）。
- **排序键（乱序判定）**：统一取归一化生效时间，跨格式可比：
  - 同 ID、同排序键、不同指纹 → `STORAGE_CLASH`，**绝不静默覆盖**当前值；
  - 同 ID、更旧排序键晚到 → `STALE_VERSION`，旧副本保留为
    `status="out-of-order"` 并以 `supersededBy` 指向当前行，当前值不变；
  - 同 ID、更新排序键到达 → 接受为新当前值，旧当前行标记
    `status="stale"`（journal 追加 `supersede` 标记）。
- 所有决定都在审计流水中留下一条记录，含消息标识与判定依据。

## 去标识规则

- 基于 **HMAC-SHA256（可配置密钥）** 的确定性伪名：同一真实值在任意消息、
  任意格式、任意并发下都映射到**同一个**占位值；不同值不会碰撞（16 hex 截断）。
- 占位值带作用域前缀：纵向患者关联 `PT-…`（由 MRN 体系 + MRN 派生，
  姓名/联系方式变化不影响纵向关联）；`MRN-…`、`NM-…`、`CT-…`。
- 空真实值稳定映射为空字符串（占位不伪造数据）。
- 对外/落盘记录（`OutputRecord`）不含任何真实 PHI；指纹不含真实值之外的
  可逆信息，且原始值绝不进入输出。密钥不同则占位值整体不同。

## 并发、容量与一致性

- `Ingest` 并发安全：去标识无状态确定（同一真实值不会被分配两个占位值）；
  存储在单一互斥下完成“容量判定 + journal fsync + 内存索引更新”，
  无半更新状态。
- `max-inflight`：在途并发上限，满载立即返回 `LIMIT_EXCEEDED / CAPACITY_EXCEEDED`
  （非阻塞、不堆积）。
- `max-records`：内存当前记录上限：
  - `policy=reject`（默认）：超限直接拒绝，不写任何状态；
  - `policy=degrade`（需有落盘目录）：仍 fsync 追加到 journal，但从内存索引
    淘汰以限制内存，需要时可从 journal 恢复，审计标记 `DEGRADED`。
- 落盘采用 **append-only JSONL + fsync + 重启重放**；journal 损坏时启动即报错
  （fail-closed），不静默丢数据。

## 目录结构

```
internal/model      规范模型 / 对外输出记录 / 版本表
internal/apperr     四类对外错误与稳定 Code（内部细节不透出）
internal/deid       HMAC 确定性去标识引擎
internal/parser     HL7v2 与 FHIR JSON 结构解析（可区分拒绝原因）
internal/normalize  业务校验、时间/性别归一、旧版本缺省语义、业务指纹
internal/audit      append-only 审计流水（内存 + 可选 JSONL，内部细节不落盘）
internal/store      幂等/乱序/冲突判定与 append-only journal
internal/pipeline   并发编排、容量闸门、错误映射
cmd/pipe            读取样例目录并发摄入并打印每条判定的命令行
testdata/samples    20 个覆盖正常/畸形/重复/乱序/跨版本/未知字段的样例
```

## 本地验证

要求 Go 1.25+。

```bash
# 全量单元测试（含 -race；日志打印每条消息标识与判定依据）
go test -race ./...

# 用样例目录端到端跑一遍（顺序、可复现）
rm -rf .local-run
go run ./cmd/pipe -in testdata/samples -out .local-run

# 高并发摄入
go run ./cmd/pipe -in testdata/samples -out /tmp/run -concurrent 16 -max-inflight 32

# 容量上限：拒绝策略 / 降级策略
go run ./cmd/pipe -in testdata/samples -out /tmp/r1 -max-records 3 -policy reject
go run ./cmd/pipe -in testdata/samples -out /tmp/r2 -max-records 3 -policy degrade
```

产物：

- `<out>/records.jsonl`：append-only 记录与 supersede 标记（全部为去标识后数据）；
- `<out>/audit.jsonl`：逐条判定流水（`seq/messageId/decision/code/reason/at`，
  **不含**内部解析细节与 PHI）。

### 样例集（`testdata/samples/`，文件名前缀即摄入顺序）

`00` 空载荷、`01` HL7 2.3 入院、`02` 与 01 同业务的 FHIR R4（跨格式重复）、
`03` HL7 原样重投、`04` FHIR R4 事件、`05` R5 含未知字段、`06` DSTU2 缺字段（缺省）、
`07` HL7 截断、`08` JSON 截断、`09` JSON 语法错、`10` 非法编码、
`11/12` 版本不支持、`13` 未知格式、`14` 缺 MRN、`15` 非法时间、
`16` 同序不同体（存储冲突）、`17` 旧版本晚到（乱序归位）、
`18` E-001 更新版本（取代并标记旧行 stale）、`19` 含未知 Z 段。

## 对外错误类型（四类）

| Kind | 含义 | 包含 Code（举例） |
---|---|---
| `FORMAT_ERROR` | 上游格式/编码错误 | TRUNCATED / ILLEGAL_ENCODING / MALFORMED_JSON / UNSUPPORTED_VERSION / UNKNOWN_FORMAT |
| `VALIDATION_ERROR` | 结构可解析但违反业务规则 | MISSING_REQUIRED_FIELD / INVALID_VALUE |
| `CONFLICT` | 幂等/乱序/存储冲突 | DUPLICATE_EVENT / STALE_VERSION / STORAGE_CLASH |
| `LIMIT_EXCEEDED` | 容量/并发上限 | CAPACITY_EXCEEDED |
