# 0025. 실버 레이어(Silver Layer) 데이터 다이어트 및 정제 전략 확정

> 원본: https://app.notion.com/p/34f1198df8d9804782d6e9c5ca474632

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-04-27 |

## 현상

- 실시간 하이라이트 탐지 시스템은 **초저지연(Ultra-Low Latency)** 처리가 핵심임.
- 원본(Bronze) 스트림에는 시스템 메시지(Nightbot)나, 하이라이트 판별과 무관한 지속 성장 지표(CS 등), 복잡한 파싱을 요구하는 배열 데이터(Array)가 포함되어 있음.
- 이를 그대로 Flink로 넘겨 연산(Window Join)할 경우, 파이프라인의 메모리를 불필요하게 차지하고 분석 속도를 저하시키는 병목(Bottleneck)이 될 수 있음.

## 결정

- **A. 채팅 데이터 정제 (Noise Filtering)**
  - 시스템/봇 메시지는 하이라이트 화력(민심) 계산을 왜곡하므로 완벽히 차단함.
  - *적용:* Flink SQL의 `WHERE` 절을 통해 `nickname = '@nightbot'` 및 특정 경고 패턴(`[warning]`)을 필터링하여 순수 유저 채팅만 실버 스트림에 적재.
- **B. 인게임 데이터 다이어트 (Drop Columns)**
  - 하이라이트(교전)와 직접적 연관이 없는 파밍 지표는 과감히 제외함.
  - *적용:* `minions_killed` 필드 드롭. (격차 계산을 위한 `total_gold`만 유지).
- **C. 연산 최적화를 위한 타입 변환 (Array to Integer)**
  - '한타(교전)의 규모'를 파악하기 위해 어시스트 정보는 필수적이나, Flink 내에서 배열(Array)을 뜯어서(Explode) 분석하는 것은 연산 비용이 높음.
  - *적용:* Python Producer(데이터 발생기) 단에서 `assisting_participant_ids` (배열) 데이터를 읽어 배열의 길이인 `assist_count` (정수)로 변환하여 Flink로 전송.

## 이유

- **성능 향상:** Flink는 복잡한 배열 파싱 없이 `kill_score + (assist_count * 가중치)` 형태의 단순 사칙연산만 수행하게 되어 연산 속도가 극대화됨.
- **비용 절감:** Kinesis와 Firehose를 타고 흐르는 데이터의 페이로드(Payload) 크기가 줄어들어 네트워크 및 스토리지(S3) 비용이 절감됨.
- **데이터 품질 확보:** 노이즈(봇 채팅)가 제거된 순도 높은 데이터만 Gold Layer(대시보드)로 전달되어 분석의 신뢰도가 향상됨.

## 트레이드오프

(원문에 없음)
