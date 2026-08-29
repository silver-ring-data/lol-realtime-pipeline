# 0028. 실시간 정제·집계를 Lambda 분리 없이 Flink 로 통합한다

> 원본: https://app.notion.com/p/3501198df8d98006add6fb70da1faa3d

| | |
|---|---|
| 상태 | **폐기** |
| 날짜 | 2026-04-28 |
| 결정자 | silver-ring-data |
| 관련 | 0013, 0030, 0040 |

## 배경

현재 파이프라인은 Kinesis Data Streams 로 들어오는 대량의 비정형 채팅과 정형 게임 데이터를 처리한다. 초기에는 Flink 부하를 줄이기 위해 AWS Lambda 로 앞단에서 미리 정제(Flattening, Filtering)하는 방식을 검토했다. 실시간성, 인프라 관리 복잡도, 제한된 예산(월 6만 원) 내 효율적 운영을 다시 검토할 필요가 생겼다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Flink 통합 처리: `Kinesis → Flink (정제 + 집계)` | 지연 최소화, 아키텍처 단순화, 1 KPU 내 처리 가능 | Flink SQL 이 길어짐 |
| B. Lambda 사전 정제: `Kinesis → Lambda → Kinesis → Flink` | Flink 부하 분산 | 네트워크 홉 추가로 지연, Lambda 호출 비용 |

## 결정

**A 를 채택한다.** Managed Flink 는 최소 1 KPU 고정 비용이 발생하며 현재 트래픽(초당 수백 건)은 1 KPU 로 정제와 집계를 모두 처리하기에 충분하다. Lambda 를 추가하면 불필요한 과금과 지연만 늘어난다. Flink SQL 내에서 `JSON_VALUE` 와 필터링 로직을 통합 관리한다. 트래픽이 급증해 KPU 임계치에 도달하면 그때 Lambda 를 분리하는 Scale-out 전략을 취한다.

## 결과

- 좋아지는 것: 월 예산 내 안정적 운영, 단순한 아키텍처로 트러블슈팅 용이.
- 감수하는 것: Flink SQL 이 길어질 수 있으나 VIEW 와 모듈화된 쿼리로 극복한다.
- 다시 볼 조건: KPU 사용량이 임계치에 도달할 때. (이후 OpenSearch 적재 경로에는 Lambda 가 도입되었다. 0040 참고.)
