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

| fhir_obs_r4_evt0001.json | FHIR R4 Observation | 与 hl7_obs_v23_ok 同一事件 EVT-0001（跨格式归链；临床语义等价——同一生效时间、同状态/代码/值，单位 cel/Cel 大小写不敏感，代码系统方言差异忽略——R4 胜出） |

## 重复 / 乱序 / 冲突
| 文件 | 预期 |
|---|---|
| hl7_dup_0001.hl7 | 与 hl7_obs_v23_ok 完全相同 → DUPLICATE |
| fhir_dup_1001.json | 与 fhir_obs_r4_ok 完全相同 → DUPLICATE |
| hl7_stale_0002.hl7 | 同 id 的 2.3/更早时间修订；新 2.5.1 已在位时 → OUT_OF_ORDER |
| hl7_obs_v23_ok.hl7 | EVT-0001 的 FHIR R4 版本已在位（字典序先摄入）→ OUT_OF_ORDER |
| hl7_zconflict_0004.hl7 | 与 hl7_enc_v251_ok 同患者、同版本、同生效时间但内容不同 → CONFLICT，原因 `CONTENT_CONFLICT_SAME_REVISION`（待复核；有效版本保留） |

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
