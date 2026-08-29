# 0044. S3 데이터 레이크 비용 최적화를 위한 Firehose 동적 파티셔닝 도입

> 원본: https://app.notion.com/p/3561198df8d980f69fa0f6be09cc36bc

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-05-04 |

## 현상

- Kinesis Data Firehose를 통해 봇의 원본(Bronze) 데이터를 S3에 적재하고 있음.
- 데이터가 누적됨에 따라, 특정 경기(`match_id`)의 데이터만 분석하고 싶어도 Athena가 S3 전체 데이터를 스캔해야 하는 구조임.
- Athena는 '데이터 스캔량'에 따라 과금되므로, 데이터가 쌓일수록 분석 비용이 기하급수적으로 증가할 위험이 있음.

## 결정

- Kinesis Data Firehose의 **'동적 파티셔닝(Dynamic Partitioning)'** 기능을 활성화함.
- 입력되는 JSON 데이터에서 JQ 쿼리를 사용하여 `match_id`를 추출하고, S3 경로를 `bronze/chat/match_id=!{partitionKeyFromQuery:match_id}/` 형태로 자동 분류함.

## 이유

- **비용 효율성:** Firehose에서 발생하는 미세한 동적 파티셔닝 추가 비용보다, Athena에서 데이터 스캔량을 90% 이상 줄임으로써 얻는 **쿼리 비용 절감 효과**가 훨씬 크다고 판단함.
- **분석 성능:** 폴더 구조가 경기별로 나뉘어 있어, 수백만 건의 데이터 중 필요한 경기 데이터만 즉시 조회할 수 있는 'Partition Pruning'이 가능해짐.
- **운영 편의성:** 별도의 가공 코드 없이 Firehose 설정만으로 데이터 레이크의 구조를 체계적으로 관리할 수 있음.

## 트레이드오프

(원문에 없음)
