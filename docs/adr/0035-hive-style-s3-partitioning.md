# 0035. 데이터 레이크 S3 경로에 Hive 스타일 파티셔닝을 채택한다

> 원본: https://app.notion.com/p/3531198df8d980129444e117db4afdb8

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-05-01 |
| 결정자 | silver-ring-data |
| 관련 | 0033, 0044 |

## 배경

메달리온 아키텍처를 구현하며 S3 저장 경로 설계에서 특정 경기(`match_id`)를 최상위 폴더로 둘지, 정제 단계(Layer)와 테이블명을 최상위로 둘지가 핵심 쟁점이었다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Data-centric/Hive 표준: `layer/table/partition_key=value/` (예: `gold/highlight_scores/match_id=worlds_2024_final/`) | Partition Pruning 으로 스캔량·비용 절감, 레이어별 Lifecycle·IAM 정책 적용 용이, Athena/Glue Crawler 자동 파티션 인식 | 콘솔에서 한 경기의 전 단계를 한 번에 보기 번거로움 |
| B. Project-centric: `match_id/layer/table/` (예: `worlds_2024_final/gold/highlight_scores/`) | 경기 단위 탐색 편리 | `WHERE match_id` 처리 시 모든 폴더 순회, 비효율 |

## 결정

**A 를 채택한다.** 대규모 분석 환경(Athena, Spark)에서 비용을 최적화하고 성능을 극대화하기 위한 전략적 선택이다.

## 결과

- 좋아지는 것: 분석 효율 극대화, 인프라 비용 절감, 아키텍처 확장성, 메타데이터 관리 오버헤드 감소.
- 감수하는 것: 수동 탐색의 번거로움. 분석 도구로 해결 가능하다.
- 다시 볼 조건: 파티션 키가 늘어나 경로 깊이가 과도해질 때.
