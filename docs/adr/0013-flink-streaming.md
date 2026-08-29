# 0013. Flink 기반 초저지연 실시간 스트리밍 아키텍처를 도입한다

> 원본: https://app.notion.com/p/3441198df8d98071bfc8d13995b1d339

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-04-16 |
| 결정자 | silver-ring-data |
| 관련 | 0009, 0018, 0030 |

## 배경

기존 설계(v0.2)는 Airflow 로 주기적으로 수집해 PostgreSQL 에 적재한 뒤 SQL 로 분석하는 방식이었다. '적재 후 분석'의 특성상 최소 수 초 이상의 지연이 발생하여, 실시간 하이라이트 탐지라는 핵심 가치를 극대화하기에 한계가 있었다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. Apache Flink (In-motion 스트리밍) | 1초 미만 지연, 메모리 레벨 연산, 내장 Window API, DB 병목 해소 | 패키징·배포 복잡도, 학습 비용 |
| B. PostgreSQL SQL (Post-processing) | 익숙함 | 수 초 이상 지연, 디스크 I/O, 복잡한 INTERVAL 조인 |

## 결정

**A 를 채택한다.** 데이터 흐름은 `Source(Kinesis) → Operator(Flink Logic) → Sink(Kinesis/Firehose)` 다.

- 데이터 인입: Python 수집 모듈이 Amazon Kinesis Data Streams 로 송신한다.
- 실시간 연산: Flink 가 Kinesis 스트림을 구독하여 경기 데이터와 채팅 데이터를 조인·분석한다. Sliding/Tumbling Window 로 "15~30초 구간의 데이터 폭증"을 메모리에서 즉시 계산한다.
- 결과 적재: 분석 결과는 Kinesis 를 거쳐 Amazon Data Firehose 로 S3(Data Lake)에 영구 저장한다.
- 언어·환경: Java, Scala 또는 PyFlink 를 사용하며 `.zip` 으로 패키징하여 배포한다.

## 결과

- 좋아지는 것: 사건 발생 직후 1초 이내 하이라이트 판별, DB 병목 해소, 최신 스트리밍 스택 구현 경험.
- 감수하는 것: 배포·의존성 관리 복잡도.
- 다시 볼 조건: 실시간 요구가 사라지거나 Flink 고정 비용(KPU)이 예산을 초과할 때.
