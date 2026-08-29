# 0045. Gold 데이터 실시간 서빙에 Lambda Sink 를 채택한다 (vs Firehose)

> 원본: https://app.notion.com/p/3561198df8d9800d959ef860fa6a846a

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-05-04 |
| 결정자 | silver-ring-data |
| 관련 | 0038, 0040 |

## 배경

Flink 연산이 끝난 Gold 데이터(하이라이트 점수, 정제 채팅)를 OpenSearch 로 전달해야 한다. 기존 실습 방식인 Firehose 는 S3 적재에 최적화되어 있으나 실시간 대시보드 서빙에는 한계가 있다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Kinesis Data Streams(Gold) → Lambda → OpenSearch | Batch window 1초로 즉시 반영, 인덱스 라우팅 로직 구현 용이, `opensearch-py` HTTP Basic Auth 로 안정적 인덱싱, 유휴 비용 0 | Lambda 관리 |
| B. Firehose → OpenSearch | 설정형 | 최소 60초 또는 1MB 버퍼링 |

## 결정

**A 를 채택한다.** 하나의 Lambda 가 점수 데이터와 채팅 데이터를 동시에 처리하여 각각 다른 인덱스(`score-index`, `chat-index`)로 분기(Routing)한다. Flink-OpenSearch 직접 연결의 복잡한 IAM·버전 의존성 문제를 피한다.

## 결과

- 좋아지는 것: 데이터 생성 즉시 대시보드에 반영, 스마트 라우팅, 서버리스 비용 구조.
- 감수하는 것: Lambda 타임아웃·메모리 튜닝(0038).
- 다시 볼 조건: 초당 처리량이 Lambda 동시성 한계에 도달할 때.
