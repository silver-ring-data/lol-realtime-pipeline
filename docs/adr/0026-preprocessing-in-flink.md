# 0026. 인게임 데이터 전처리(Pre-processing) 계층 선정: Edge(Producer) vs Flink

> 원본: https://app.notion.com/p/34f1198df8d98016a4d0c03194a48b66

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-04-27 |

## 현상

- 실시간 하이라이트 점수 계산을 위해서는 인게임 원본 데이터(JSON)에서 불필요한 필드(예: `minions_killed`)를 제거하고, 복잡한 어시스트 배열(`assisting_participant_ids`)을 정수형 길이(`assist_count`)로 변환하는 '데이터 다이어트'가 필수적임.
- 이를 처리할 위치로 두 가지 선택지가 존재함.
  - **Option A (Edge 전처리):** Python Producer(데이터 전송 봇) 단에서 미리 데이터를 가공하여 Kinesis로 전송. (네트워크 페이로드 감소 및 Flink 연산 부하 최소화)
  - **Option B (Flink 전처리):** Producer는 100% 원본을 전송하고, Flink(Silver Layer 진입점)에서 `CARDINALITY` 함수 등을 사용해 가공.

## 결정

- **Option B(Flink 전처리)를 채택**하여, 파이썬 봇은 가공 없이 100% 원본 데이터를 전송하고, **Apache Flink가 데이터 파싱, 스키마 축소, 윈도우 조인 등의 무거운 연산(Heavy Lifting)을 전담**하도록 설계함.

## 이유

- **메달리온 아키텍처 정석 구현 (데이터 무결성):** S3에 쌓이는 Bronze Layer에는 어떠한 데이터 손실이나 가공도 없는 '단일 진실 공급원(Immutable Raw Data)'을 보존해야 함. 이를 통해 추후 새로운 분석 요건(예: 미니언 처치 수 기반 분석)이 생겨도 유연하게 대응(Backfill)할 수 있음.
- **비용(Cost) 대비 최적의 아키텍처:** Kinesis는 25KB 단위로 과금되며, Flink는 최소 1 KPU(Kinesis Processing Unit)를 기본으로 할당함. 본 프로젝트의 초당 수백 건(수백 Byte 수준) 트래픽 환경에서는 Option A와 B의 클라우드 인프라 청구 비용이 동일하므로, 아키텍처의 정합성을 지키는 Option B가 훨씬 가치 있는 선택임.
- **스트림 엔진 역량 증명:** 단순 통로 역할이 아닌, Flink의 강력한 In-memory 연산 능력(배열 파싱 및 실시간 조인)을 한계까지 활용하여 분산 데이터 스트리밍 시스템의 도입 타당성을 스스로 입증함.

## 트레이드오프

(원문에 없음)
