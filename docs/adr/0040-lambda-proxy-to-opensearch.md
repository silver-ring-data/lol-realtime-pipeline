# 0040. OpenSearch 커넥터 이슈를 피해 Lambda Proxy 기반 적재로 전환한다

> 원본: https://app.notion.com/p/3541198df8d9807d95bcf2e20e36c75c

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-05-03 |
| 결정자 | silver-ring-data |
| 관련 | 0029, 0038, 0045 |

## 배경

초기에는 Flink SQL 의 `opensearch` 커넥터로 Managed Flink 에서 OpenSearch 로 직접 전송하려 했다. 그러나 커넥터 JAR 의 버전 의존성 충돌, 특정 버전의 `NullPointerException` 등 런타임 에러가 지속되어 신뢰성을 확보하기 어려웠고, 커스텀 JAR 패키징·디버깅에 과도한 시간이 소요되었다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Flink → Kinesis Data Streams(Gold) → Lambda(`opensearch-py`) → OpenSearch | 가벼운 Python 라이브러리 관리, CloudWatch 연동, 내장 재시도, 역할 분리 | 리소스 추가로 수백 ms 지연 |
| B. Flink Direct Sink 유지 | 홉 최소화 | 커넥터 버전 충돌, 디버깅 비용 |

## 결정

**A 를 채택한다.** Direct Sink 를 제거하고, Flink 결과물을 표준 커넥터인 Kinesis Data Streams 로 송출한 뒤 Lambda 가 이를 구독하여 `opensearch-py` 로 인덱싱한다. Flink 는 복잡한 스트림 연산에, Lambda 는 데이터 배달에 집중한다.

## 결과

- 좋아지는 것: 커넥터 설정 대신 비즈니스 로직 개발에 집중하고, 파이프라인 가시성이 확보되며, 일시적 OpenSearch 부하 시에도 재시도로 유실을 방지한다.
- 감수하는 것: 수백 ms 단위 지연. 경기 중계 특성상 서비스 품질에 영향이 없는 수준이다.
- 다시 볼 조건: Managed Flink 의 OpenSearch 커넥터가 안정화될 때.
