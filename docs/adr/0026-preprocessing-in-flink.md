# 0026. 인게임 데이터 전처리 계층을 Flink 로 정한다 (Edge vs Flink)

> 원본: https://app.notion.com/p/34f1198df8d98016a4d0c03194a48b66

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-04-27 |
| 결정자 | silver-ring-data |
| 관련 | 0020, 0025 |

## 배경

실시간 하이라이트 점수 계산을 위해 인게임 원본 JSON 에서 불필요한 필드(`minions_killed`)를 제거하고 어시스트 배열(`assisting_participant_ids`)을 정수 길이(`assist_count`)로 변환하는 '데이터 다이어트'가 필요하다. 이를 처리할 위치로 두 가지 선택지가 있다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Flink 전처리: Producer 는 100% 원본 전송, Flink 에서 `CARDINALITY` 등으로 가공 | Bronze 에 무손실 원본 보존(Backfill 가능), 스트림 엔진 역량 증명 | Flink 연산 부하 |
| B. Edge 전처리: Python Producer 단에서 가공 후 Kinesis 전송 | 네트워크 페이로드 감소, Flink 부하 최소화 | Bronze 원본 훼손 |

## 결정

**A 를 채택한다.** 파이썬 봇은 가공 없이 원본을 전송하고, Flink 가 파싱·스키마 축소·윈도우 조인 등 무거운 연산을 전담한다. Kinesis 는 25KB 단위 과금이고 Flink 는 최소 1 KPU 를 기본 할당하므로, 초당 수백 건(수백 Byte) 수준의 트래픽에서는 두 옵션의 인프라 비용이 동일하다. 따라서 아키텍처 정합성을 지키는 A 가 더 가치 있다.

## 결과

- 좋아지는 것: 메달리온 아키텍처의 정석(Immutable Raw Data)을 구현하고, 새 분석 요건이 생겨도 유연하게 대응한다.
- 감수하는 것: Flink 연산 부하. 현재 트래픽에서는 무시할 수준이다.
- 다시 볼 조건: 트래픽이 늘어 Kinesis 과금 단위나 KPU 사용량이 실제 비용 차이를 만들 때.
