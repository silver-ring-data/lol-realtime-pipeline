# 0030. 중간 단계(Silver, Gold) Kinesis 스트림을 제거하고 Flink 인메모리 처리로 전환한다

> 원본: https://app.notion.com/p/3501198df8d9809e8b55ef081c789103

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-04-28 |
| 결정자 | silver-ring-data |
| 관련 | 0013, 0039 |

## 배경

초기 설계에서는 메달리온 각 단계(Bronze → Silver → Gold)마다 물리적 통로인 Kinesis Data Streams 를 두려 했다. 단계별로 영구 저장할 수 있어 안정적이지만, Kinesis 는 샤드 수에 따라 시간당 비용이 발생해 예산(6만 원)을 초과할 위험이 크고, 매번 스트림에 쓰고 읽는 과정에서 네트워크 오버헤드가 발생해 초저지연 성능을 저해한다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Silver 전용 스트림 미생성, Flink 인메모리 임시 테이블로 정제·조인 후 Gold 만 전송 | 월 수십 달러 절감, ms 단위 지연, 관리 포인트 감소 | Job 중단 시 Silver 데이터 복구 불가 |
| B. 단계별 Kinesis 스트림 | 단계별 영구 저장 | 비용 폭발, 지연 증가 |

## 결정

**A 를 채택한다.** Flink 내부에서 실시간 정제와 조인을 한 번에 수행하고, 최종 결과물(Gold)만 최종 목적지(OpenSearch)로 바로 전송한다. Flink SQL 은 `INSERT INTO Silver_Stream → SELECT FROM Silver_Stream` 구조에서 하나의 View 또는 연속 연산(Chaining) 구조로 바뀐다.

## 결과

- 좋아지는 것: 비용 효율, 초저지연, 아키텍처 단순화.
- 감수하는 것: Flink Job 이 멈추면 Silver 단계 데이터를 스트림에서 복구할 수 없다. Checkpointing 을 활성화하여 Flink 내부 상태를 S3 에 저장함으로써 보완한다.
- 다시 볼 조건: 중간 단계 데이터를 다른 소비자가 구독해야 할 때.
