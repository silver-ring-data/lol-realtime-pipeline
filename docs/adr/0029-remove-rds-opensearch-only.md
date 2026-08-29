# 0029. RDS 를 제거하고 OpenSearch 단일 서빙을 채택한다

> 원본: https://app.notion.com/p/3501198df8d980149c5fce27488f4f67

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-04-28 |
| 결정자 | silver-ring-data |
| 관련 | 0004, 0016, 0038, 0040 |

## 배경

초기 메달리온 설계에서는 실시간 서빙 레이어를 둘로 나누었다. Silver(비정형 채팅)는 텍스트 검색에 특화된 OpenSearch, Gold(시계열 점수)는 수치 조회에 특화된 RDS(PostgreSQL)였다. 그러나 인프라 세팅 중 현재 MVP 에는 회원가입·결제 같은 트랜잭션(ACID)이 필요한 관계형 데이터가 없음을 확인했다. 단순 시계열 조회를 위해 RDS 를 유지하는 것은 오버 엔지니어링이다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. OpenSearch 를 단일 통합 서빙 DB 로 격상 | RDS 인스턴스·VPC 비용 절감, 역색인 기반 ms 단위 조회, 리소스 재투자 | SQL 대신 OpenSearch DSL 로 조회 로직 작성 |
| B. RDS + OpenSearch 이중 서빙 | 용도별 최적화 | 비용·관리 복잡도 상승 |

## 결정

**A 를 채택한다.** PostgreSQL(RDS) 도입을 전면 취소하고, Flink 가 산출한 Silver(정제 채팅)와 Gold(하이라이트 점수)를 모두 OpenSearch 인덱스에 적재한다. 파이프라인은 `Kinesis → Flink → OpenSearch + S3` 로 간결해진다.

## 결과

- 좋아지는 것: 클라우드 비용 절감, 시계열 그래프 서빙 성능 충족, 절약한 시간을 AWS CDK 자동화·OpenSearch Dashboards 고도화·Bedrock 연동에 투자.
- 감수하는 것: API 서버(FastAPI)에서 SQL 대신 OpenSearch DSL 쿼리를 작성한다.
- 다시 볼 조건: 트랜잭션이 필요한 관계형 데이터(회원, 결제 등)가 생길 때.
