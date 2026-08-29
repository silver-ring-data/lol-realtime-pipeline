# 설계 결정 기록 (ADR)

이 디렉터리의 문서는 2026-08-29 에 노션 데이터베이스 "[ADR] 설계 기록" 에서 마이그레이션했다. 형식은 `dev-notes/templates/adr.md` 를 따른다 (상태·날짜 / 현상 / 결정 / 이유 / 트레이드오프). 본문은 노션 원문을 그대로 옮겼으며, 각 문서 상단의 `원본:` 링크가 노션 원문을 가리킨다.

## 번호 재정렬 안내

노션에서 "ADR-048" 번호가 두 문서에 중복 부여되어 있었다. 마이그레이션 시 생성 일시가 빠른 문서를 0048, 늦은 문서를 0049 로 정하고, 그 뒤 문서를 하나씩 밀었다.

| 노션 번호 | 마이그레이션 번호 |
|---|---|
| ADR-048 (DAG·SQL 분리, 05-04) | 0048 |
| ADR-048 (Naming Convention 통일, 05-06) | 0049 |
| ADR-049 | 0050 |
| ADR-050 | 0051 |
| ADR-051 | 0052 |
| ADR-052 | 0053 |

상태 매핑: 노션 `승인` → 채택, `폐기` → 폐기, `보류` → 보류. 날짜는 노션 생성 일시의 KST 기준이다.

## 목록

| 번호 | 제목 | 상태 | 날짜 |
|---|---|---|---|
| [0001](0001-data-dieting.md) | 데이터 수집 범위를 핵심 지표로 제한한다 (Data Dieting) | 폐기 | 2026-04-13 |
| [0002](0002-replay-simulation.md) | 프로 경기 데이터를 리플레이 시뮬레이션으로 수집한다 | 채택 | 2026-04-13 |
| [0003](0003-staged-validation-static-vs-dramatic.md) | 변동성에 따라 파이프라인을 단계별로 검증한다 | 폐기 | 2026-04-13 |
| [0004](0004-postgresql-storage.md) | 데이터 저장소로 PostgreSQL 을 선정한다 | 폐기 | 2026-04-13 |
| [0005](0005-parallel-ingestion.md) | 병렬 데이터 수집 아키텍처를 채택한다 | 채택 | 2026-04-14 |
| [0006](0006-add-chat-data-source.md) | 채팅 데이터 소스를 추가한다 | 채택 | 2026-04-14 |
| [0007](0007-domain-weighted-scoring.md) | 도메인 기반 하이라이트 가중치 산정 로직을 채택한다 | 채택 | 2026-04-14 |
| [0008](0008-hybrid-fact-sentiment-detection.md) | 하이브리드(Fact + Sentiment) 하이라이트 탐지 아키텍처 | 채택 | 2026-04-14 |
| [0009](0009-temporal-window-join.md) | 윈도우 기반 조인(Temporal Join)을 적용한다 | 채택 | 2026-04-14 |
| [0010](0010-defer-threshold-tuning.md) | 데이터 기반 임계치·윈도우 최적화를 보류한다 | 보류 | 2026-04-14 |
| [0011](0011-mock-data-layer.md) | Mock 데이터 레이어를 도입한다 | 채택 | 2026-04-15 |
| [0012](0012-env-config-strategy.md) | 환경 변수 관리 전략을 수립한다 | 채택 | 2026-04-15 |
| [0013](0013-flink-streaming.md) | Flink 기반 초저지연 실시간 스트리밍 아키텍처를 도입한다 | 채택 | 2026-04-16 |
| [0014](0014-dataset-selection.md) | 분석 데이터셋과 수집 전략을 확정한다 (2024 Worlds 결승) | 채택 | 2026-04-16 |
| [0015](0015-schema-change.md) | 스키마를 변경한다 | 채택 | 2026-04-16 |
| [0016](0016-postgresql-direct-load.md) | Flink 에서 PostgreSQL 로 직접 적재한다 | 폐기 | 2026-04-20 |
| [0017](0017-phased-validation-and-live-api.md) | 단계별 검증 및 실전 API 확장 전략 | 보류 | 2026-04-21 |
| [0018](0018-athena-analysis-layer.md) | Amazon Athena 분석 레이어를 채택한다 | 채택 | 2026-04-21 |
| [0019](0019-smart-sampling.md) | 대시보드 채팅 시각화에 스마트 샘플링을 도입한다 | 채택 | 2026-04-22 |
| [0020](0020-preserve-raw-data.md) | S3 Bronze 에 무필터링 원본을 100% 보존한다 | 채택 | 2026-04-22 |
| [0021](0021-pivot-digital-twin-stadium.md) | '디지털 트윈 응원석'으로 피벗한다 | 채택 | 2026-04-22 |
| [0022](0022-viewer-count-mvp-scope.md) | 시청자 수 연동을 제외하고 1차 MVP 스코프를 조정한다 | 폐기 | 2026-04-22 |
| [0023](0023-project-naming-convention.md) | 프로젝트 명명 규칙을 적용한다 (lol-highlighter) | 폐기 | 2026-04-24 |
| [0024](0024-table-naming-convention.md) | 파이프라인 내부 테이블 명명 규칙을 정한다 | 채택 | 2026-04-24 |
| [0025](0025-silver-layer-data-diet.md) | 실버 레이어 데이터 다이어트 및 정제 전략 | 채택 | 2026-04-27 |
| [0026](0026-preprocessing-in-flink.md) | 인게임 데이터 전처리 계층을 Flink 로 정한다 | 채택 | 2026-04-27 |
| [0027](0027-window-and-weight-parameters.md) | 하이라이트 탐지 윈도우·가중치 파라미터 최적화 | 보류 | 2026-04-27 |
| [0028](0028-flink-integrated-processing-vs-lambda.md) | 실시간 정제·집계를 Flink 로 통합한다 (vs Lambda) | 폐기 | 2026-04-28 |
| [0029](0029-remove-rds-opensearch-only.md) | RDS 를 제거하고 OpenSearch 단일 서빙을 채택한다 | 채택 | 2026-04-28 |
| [0030](0030-remove-intermediate-kinesis-streams.md) | 중간 단계 Kinesis 스트림 제거 및 Flink 인메모리 처리 | 채택 | 2026-04-28 |
| [0031](0031-chat-filtering-tiers.md) | 디지털 트윈 응원석 채팅 정제 및 노출 기준 | 채택 | 2026-04-29 |
| [0032](0032-airflow-for-batch-orchestration.md) | 배치 오케스트레이션 도구로 Airflow 를 선정한다 | 채택 | 2026-04-30 |
| [0033](0033-infra-naming-and-s3-partitioning.md) | 인프라 네이밍 컨벤션과 S3 파티셔닝 전략 | 채택 | 2026-04-30 |
| [0034](0034-nexus-destroyed-shutdown-trigger.md) | 넥서스 파괴 기준 스트림 처리 자동 종료 및 동기화 | 채택 | 2026-04-30 |
| [0035](0035-hive-style-s3-partitioning.md) | S3 경로에 Hive 스타일 파티셔닝을 채택한다 | 채택 | 2026-05-01 |
| [0036](0036-heterogeneous-stream-time-sync.md) | 이종 스트림 시간을 Producer 단에서 동기화한다 | 채택 | 2026-05-02 |
| [0037](0037-serving-chat-keywords.md) | 서빙용 채팅 선별 키워드 | 채택 | 2026-05-02 |
| [0038](0038-opensearch-index-template-and-lambda-tuning.md) | OpenSearch 인덱스 템플릿 도입 및 Lambda 전송 안정화 | 채택 | 2026-05-03 |
| [0039](0039-logical-medallion-no-physical-silver.md) | 물리적 Silver 저장소 제거 및 논리적 Medallion 명명 | 채택 | 2026-05-03 |
| [0040](0040-lambda-proxy-to-opensearch.md) | Lambda Proxy 기반 OpenSearch 적재로 전환한다 | 채택 | 2026-05-03 |
| [0041](0041-virtual-viewer-and-avatar-state.md) | 가상 시청자 수 시뮬레이션 및 아바타 상태 동기화 | 보류 | 2026-05-03 |
| [0042](0042-dual-time-sync.md) | 듀얼 타임(Base Time & Real Time) 동기화 전략 | 채택 | 2026-05-04 |
| [0043](0043-serverless-serving.md) | 완전 서버리스 서빙 아키텍처를 채택한다 | 채택 | 2026-05-04 |
| [0044](0044-firehose-dynamic-partitioning.md) | Firehose 동적 파티셔닝을 도입한다 | 채택 | 2026-05-04 |
| [0045](0045-lambda-sink-vs-firehose.md) | Gold 데이터 서빙에 Lambda Sink 를 채택한다 (vs Firehose) | 채택 | 2026-05-04 |
| [0046](0046-airflow-s3-signaling-async-batch.md) | Airflow 와 S3 시그널링으로 비동기 배치 분석 자동화 | 채택 | 2026-05-04 |
| [0047](0047-glue-spark-engine.md) | 배치 레이어 엔진으로 Glue Spark 를 채택한다 | 채택 | 2026-05-04 |
| [0048](0048-separate-dag-and-sql.md) | Airflow DAG 와 비즈니스 SQL 로직을 분리한다 | 채택 | 2026-05-04 |
| [0049](0049-layer-table-naming-unification.md) | 레이어·테이블 Naming Convention 통일 (노션 ADR-048) | 채택 | 2026-05-06 |
| [0050](0050-multi-dag-by-lifecycle.md) | 역할·생명주기 기반 Airflow 다중 DAG 분리 (노션 ADR-049) | 채택 | 2026-05-07 |
| [0051](0051-pre-task-isolation-for-sync.md) | 스트림 조인 싱크 보장을 위한 사전 작업 분리 (노션 ADR-050) | 채택 | 2026-05-07 |
| [0052](0052-athena-data-loss-troubleshooting.md) | Athena 데이터 유실 문제 점검 절차 (노션 ADR-051) | 채택 | 2026-05-07 |
| [0053](0053-inject-match-id-at-producer.md) | Producer 에서 match_id 메타데이터 주입 (노션 ADR-052) | 채택 | 2026-05-07 |

## 마이그레이션 메모

- 본문의 현상·결정·이유는 노션의 "Context (현상)", "Decision (결정)", "Rationale (이유)" 블록을 그대로 옮겼다. 트레이드오프 항목은 원문 중 단점·리스크·결과 및 영향에 해당하는 부분만 옮겼고, 그런 내용이 없는 문서는 "(원문에 없음)" 으로 표시했다.
- 노션 블록이 비어 있는 항목은 "(원문 없음)" 으로 표시했다. (예: 0015 는 현상 한 줄만 있고, 0024·0037 은 결정만 있다.)
- 0052 는 노션 원문이 결정 형식이 아닌 트러블슈팅 점검 목록이라 본문을 그대로 옮겼다.
- 노션 원문의 개인 식별 정보(공용 계정 사용자 ID, 버킷 이름, 팀원 이름)는 `{user_id}`, `{bucket}` 등 자리 표시자로 바꾸거나 제거했다.
- 새 ADR 은 `dev-notes/templates/adr.md` 형식을 따라 0054 부터 추가한다.
