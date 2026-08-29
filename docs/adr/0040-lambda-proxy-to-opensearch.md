# 0040. OpenSearch 커넥터 이슈 해결을 위한 Lambda Proxy 기반 적재 아키텍처 전환

> 원본: https://app.notion.com/p/3541198df8d9807d95bcf2e20e36c75c

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-05-03 |

## 현상

- **초기 설계:** Flink SQL의 `opensearch` 커넥터를 사용하여 Managed Flink에서 OpenSearch로 데이터를 직접(Direct) 전송하려고 시도함.
- **기술적 난관:**
  - Managed Flink 환경에서 OpenSearch 커넥터(JAR)의 버전 의존성 충돌 발생.
  - 특정 버전에서 `NullPointerException` 등 런타임 에러가 지속되어 데이터 전송의 신뢰성을 확보하기 어려움.
  - 클라우드 환경 특성상 커스텀 JAR 패키징 및 디버깅에 과도한 시간 소요됨.

## 결정

- **Direct Sink 제거:** Flink에서 OpenSearch로 직접 쏘는 설정을 제거함.
- **KDS-Lambda 징검다리 도입:**
  - Flink의 결과물을 표준 커넥터인 Kinesis Data Streams (Gold)로 송출.
  - 해당 스트림을 **AWS Lambda**가 구독(Trigger)하게 함.
  - Lambda 내부에서 `opensearch-py` 라이브러리를 사용하여 데이터를 OpenSearch로 인덱싱함.

## 이유

- **라이브러리 관리의 용이성:** Flink의 무거운 JAR 방식 대신, Lambda에서 가벼운 Python 라이브러리를 사용함으로써 버전 관리와 보안 설정을 훨씬 직관적으로 제어할 수 있음.
- **디버깅 및 모니터링:** Lambda는 CloudWatch와 완벽하게 연동되어 전송 실패 시 로그 추적이 매우 쉬움 (우리가 타임아웃 범인을 잡은 것처럼!).
- **재시도 로직 (Retry Logic):** KDS와 Lambda의 조합은 전송 실패 시 자동으로 재시도하는 로직이 내장되어 있어, 일시적인 OpenSearch 부하 시에도 데이터 유실을 방지함.
- **역할의 분리:** Flink는 '복잡한 스트림 연산'에만 집중하고, Lambda는 '데이터 배달'에만 집중하여 시스템 구성 요소 간의 결합도를 낮춤.

## 트레이드오프

- **긍정적 효과:**
  - 커넥터 설정 삽질(?)을 멈추고 비즈니스 로직(하이라이트 점수 계산) 개발에 집중할 수 있게 됨.
  - 파이프라인의 가시성(Observability)이 확보되어 문제 발생 시 즉각 대응 가능.
- **부정적 효과 (주의점):**
  - 중간에 리소스(KDS, Lambda)가 추가되어 아주 미세한 지연(Latency)이 발생할 수 있으나, 롤 경기 중계 특성상 수백 밀리초 단위의 지연은 서비스 품질에 영향이 없는 수준임.
