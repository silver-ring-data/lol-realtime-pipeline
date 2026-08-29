# 0045. Gold 데이터 실시간 서빙을 위한 Lambda Sink 채택 (vs Firehose)

> 원본: https://app.notion.com/p/3561198df8d9800d959ef860fa6a846a

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-05-04 |

## 현상

- Flink에서 연산이 완료된 Gold 데이터(하이라이트 점수, 정제된 채팅)를 최종 목적지인 OpenSearch로 전달해야 함.
- 기존 실습 방식인 Firehose는 S3 적재에는 최적화되어 있으나, 실시간 대시보드 서빙에는 한계가 있음.

## 결정

- Kinesis Data Streams (Gold)와 OpenSearch 사이의 연결 고리로 **AWS Lambda**를 채택함.

## 이유

- **초저지연(Ultra-Low Latency):** Firehose는 최소 60초 혹은 1MB의 버퍼링 시간이 필요하지만, Lambda는 **Batch window를 1초**로 설정하여 데이터 생성 즉시 대시보드에 반영할 수 있음.
- **스마트 라우팅:** 하나의 람다 함수가 점수 데이터와 채팅 데이터를 동시에 처리하여 각각 OpenSearch의 다른 인덱스(`score-index`, `chat-index`)로 분기 처리(Routing)하는 로직을 코드로 간단히 구현 가능함.
- **인증 및 보안:** Flink-OpenSearch 직접 연결 시 발생하는 복잡한 IAM 및 버전 의존성 문제를 피하고, 람다 내부에서 `opensearch-py` 라이브러리를 통해 HTTP Basic Auth 방식을 사용하여 안정적으로 데이터를 인덱싱함.
- **비용 최적화:** 서버를 상시 가동하는 방식이 아닌, 데이터가 들어올 때만 실행되는 서버리스 구조를 선택하여 유휴 시간 비용을 0으로 유지함.

## 트레이드오프

(원문에 없음)
