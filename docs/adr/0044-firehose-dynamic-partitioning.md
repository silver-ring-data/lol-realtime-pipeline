# 0044. S3 데이터 레이크 비용 최적화를 위해 Firehose 동적 파티셔닝을 도입한다

> 원본: https://app.notion.com/p/3561198df8d980f69fa0f6be09cc36bc

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-05-04 |
| 결정자 | silver-ring-data |
| 관련 | 0035 |

## 배경

Kinesis Data Firehose 로 봇의 원본(Bronze) 데이터를 S3 에 적재하고 있다. 데이터가 누적되면 특정 경기(`match_id`)만 분석하고 싶어도 Athena 가 S3 전체를 스캔해야 하는 구조이고, Athena 는 스캔량 과금이므로 비용이 기하급수적으로 늘 위험이 있다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Firehose 동적 파티셔닝(JQ 로 `match_id` 추출) | 가공 코드 없이 설정만으로 경기별 폴더 분류, Partition Pruning | Firehose 동적 파티셔닝 추가 비용 |
| B. 단일 경로 적재 후 사후 정리 | 설정 단순 | 전체 스캔, 별도 정리 배치 필요 |

## 결정

**A 를 채택한다.** 입력 JSON 에서 JQ 쿼리로 `match_id` 를 추출하고, S3 경로를 `bronze/chat/match_id=!{partitionKeyFromQuery:match_id}/` 형태로 자동 분류한다.

## 결과

- 좋아지는 것: Athena 스캔량을 90% 이상 줄여 쿼리 비용을 절감하고, 수백만 건 중 필요한 경기 데이터만 즉시 조회한다.
- 감수하는 것: Firehose 의 미세한 추가 비용. 절감 효과가 훨씬 크다.
- 다시 볼 조건: 파티션 키가 추가되어 경로 규칙을 바꿔야 할 때.
