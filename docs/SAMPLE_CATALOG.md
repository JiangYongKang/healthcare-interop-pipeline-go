# 样例消息目录（testdata/samples）

所有文件都被 `cmd/ingest` 与集成测试 `TestEngineMixedSampleDirectory` 使用。

## 合法消息
| 文件 | 格式/版本 | 说明 |
|---|---|---|
| hl7_obs_v23_ok.hl7 | HL7 2.3 Observation | 体温，OBX-11=F |
| hl7_obs_v251_ok.hl7 | HL7 2.5.1 Observation | 体重 |
| hl7_cond_v23_ok.hl7 | HL7 2.3 Condition | OBX-11 为空 → 归一为 preliminary |
| hl7_enc_v251_ok.hl7 | HL7 2.5.1 Encounter | ADT^A04 |
| fhir_obs_r4_ok.json | FHIR R4 Observation | 裸资源，未知 `note` 字段 |
| fhir_cond_stu3_ok.json | FHIR STU3 Condition | 信封形态，inactive → resolved |
| fhir_enc_r4_ok.json | FHIR R4 Encounter | in-progress → active |
| fhir_obs_r4_other.json | FHIR R4 Observation | 同患者 MRN-1001，验证纵向关联 |
| fhir_unknown_fields_ok.json | FHIR R4 Observation | 未知 extension/display 必须被接受并令牌化 |

| fhir_obs_r4_evt0001.json | FHIR R4 Observation | 与 hl7_obs_v23_ok 同一事件 EVT-0001：临床事实一致（同时间、同值 36.8，`cel`/`Cel` 与 LN/LOINC URL 归一等价），跨格式归链且 R4 正常取代 |

## 重复 / 乱序 / 冲突
| 文件 | 预期 |
|---|---|
| hl7_dup_0001.hl7 | 与 hl7_obs_v23_ok 完全相同 → DUPLICATE |
| fhir_dup_1001.json | 与 fhir_obs_r4_ok 完全相同 → DUPLICATE |
| hl7_stale_0002.hl7 | 同 id 的 2.3/更早临床时间修订；新 2.5.1 已在位时 → OUT_OF_ORDER |
| hl7_obs_v23_ok.hl7 | EVT-0001 的 FHIR R4 已在位（字典序先摄入）：临床一致但版本序更旧 → OUT_OF_ORDER |
| hl7_zconflict_0004.hl7 | 与 hl7_enc_v251_ok 同版本同临床时间、值不同（`{visit}` vs `{visit-corrected}`）→ CONFLICT / `CONTENT_CONFLICT_SAME_REVISION`（待复核，差异字段 value） |

> 说明：跨格式/跨版本"内容确实不一致"（同临床时间但值、状态等对不上）以及"患者被换成
> 另一个人"的场景没有放在样例语料中（以保持 9/2/2/1/13 的固定分布），由自动化用例
> 稳定覆盖，见 README 第 10 节：
> `CONTENT_CONFLICT_CROSS_REVISION`（两种到达顺序）与 `PATIENT_MISMATCH` 均断言
> 原有效版本保留、链患者关联不改写、原因稳定可查、复核原子且幂等、重启与回放一致。

## 畸形 / 校验失败（拒绝原因可区分）
| 文件 | 拒绝原因 | 对外类型 |
|---|---|---|
| empty.msg | EMPTY_PAYLOAD | FORMAT_ERROR |
| hl7_bad_encoding.hl7 | ENCODING_INVALID（MSH-2 分隔符错） | FORMAT_ERROR |
| hl7_truncated.hl7 | STRUCTURE_TRUNCATED（缺 PID/OBX） | FORMAT_ERROR |
| hl7_bad_time.hl7 | INVALID_TIME（OBX-14 非法） | FORMAT_ERROR |
| hl7_bad_version.hl7 | UNSUPPORTED_VERSION（2.1） | FORMAT_ERROR |
| hl7_missing_mrn.hl7 | MISSING_PATIENT_ID | VALIDATION_ERROR |
| hl7_bad_status.hl7 | INVALID_STATUS（OBX-11=Z，归一阶段拒绝） | VALIDATION_ERROR |
| fhir_truncated.json | STRUCTURE_TRUNCATED | FORMAT_ERROR |
| fhir_dup_key.json | JSON_INVALID（重复键） | FORMAT_ERROR |
| fhir_missing_subject.json | MISSING_PATIENT_ID | VALIDATION_ERROR |
| fhir_bad_status.json | INVALID_STATUS（"purple"） | VALIDATION_ERROR |
| fhir_bad_version.json | UNSUPPORTED_VERSION（DSTU2） | FORMAT_ERROR |
| fhir_bad_time.json | INVALID_TIME | FORMAT_ERROR |
