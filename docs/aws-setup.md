# AWS 리소스 구성 가이드

이 프로젝트의 AWS 리소스는 **관리 콘솔에서 수동으로 구성**했습니다.
아래는 동일한 환경을 재현하기 위한 설정값 기록입니다.

> **한계와 향후 과제**
> 수동 구성은 재현성이 떨어지고 변경 이력이 남지 않습니다.
> 리소스 이름과 설정값을 [`config/config.yaml`](../config/config.yaml)에 모아
> 코드가 하드코딩된 이름에 의존하지 않도록 했지만, 리소스 생성 자체는
> 여전히 수동입니다. IaC(CDK/Terraform) 전환이 다음 개선 과제입니다.

리소스 명명 규칙: `{prefix}-{project}-{env}-{region_abbr}-{service}-{tier}-{data}`
리전은 모두 `ap-northeast-2`를 사용합니다.

---

## 1. Kinesis Data Streams

Bronze 3개, Gold 2개를 생성합니다. 모두 온디맨드 또는 샤드 1개로 충분합니다.

| 용도 | 스트림 이름 |
|---|---|
| 게임 지표 (Bronze) | `de-ai-05-lol-dev-an2-kds-brz-game` |
| 채팅 (Bronze) | `de-ai-05-lol-dev-an2-kds-brz-chat` |
| 시청자 수 (Bronze) | `de-ai-05-lol-dev-an2-kds-brz-viewer` |
| 하이라이트 점수 (Gold) | `de-ai-05-lol-dev-an2-kds-gld-score` |
| 서빙용 채팅 (Gold) | `de-ai-05-lol-dev-an2-kds-gld-chat` |

## 2. Kinesis Data Firehose

Bronze 스트림을 S3에 적재합니다. 게임/채팅 각각 하나씩 생성합니다.

- **Source**: 해당 Kinesis Data Stream
- **Destination**: S3 (`de-ai-05-lol-an2-s3-datalake`)
- **Dynamic partitioning**: `Enabled`
- **Multi-record deaggregation**: `Disabled` (프로듀서가 레코드를 1건씩 전송)
- **Partition key**
  - Key name: `p_match_id`
  - JQ expression: `.match_id`
- **Buffer hints**: 크기 1MB / 간격 60초

S3 prefix (게임 기준, 채팅은 `game`을 `chat`으로 치환):

```
bronze/game/p_match_id=!{partitionKeyFromQuery:p_match_id}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/
```

Error prefix:

```
bronze/errors/game/!{firehose:error-output-type}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/
```

## 3. Managed Flink (Studio 노트북)

- 애플리케이션: `de-ai-05-lol-dev-an2-stdo-analyzer`
- [`serving/flink/highlight_scoring.py`](../serving/flink/highlight_scoring.py)의 내용을
  Zeppelin 노트북 단락에 붙여 넣고 실행합니다.
- 실행 역할에 대상 Kinesis 스트림 read/write와 S3 write 권한이 필요합니다.

## 4. Glue Job

- Job 이름: `de-ai-05-lol-dev-an2-glue-slv-refiner`
- 스크립트: [`glue_jobs/format_converter.py`](../glue_jobs/format_converter.py)

| 항목 | 값 |
|---|---|
| Type | Spark |
| Language | Python 3 |
| Glue version | 최신 |
| Worker type | G.1X |
| Requested workers | 2 |
| Job timeout | 15분 |
| Retries | 0 |

Job 파라미터로 `--bucket_name`, `--match_id`를 받습니다(Airflow가 전달).

## 5. Athena / Glue Data Catalog

- 데이터베이스: `de_ai_05_lol_dev_an2_glue_db` (하이픈 대신 언더바 사용)
- 워크그룹: `de-ai-05-lol-dev-an2-athena-wg`
- 테이블 DDL은 `1_live_infra_setup_dag`가 자동 생성합니다.
  ([`airflow/dags/sql/ddl_batch_tables/`](../airflow/dags/sql/ddl_batch_tables/))

## 6. OpenSearch

- 도메인: `de-ai-05-lol-dev-an2-os-srsh` (도메인명 28자 제한)
- 인덱스 템플릿을 **적재 시작 전에** 등록합니다.
  → [`serving/opensearch/index_templates.md`](../serving/opensearch/index_templates.md)
- Dashboards의 **Security > Roles**에서 Lambda 실행 역할을 백엔드 역할로 매핑합니다.

접근 정책은 Principal을 Lambda 실행 역할로 한정합니다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<AWS_ACCOUNT_ID>:role/service-role/<os-loader-role>"
      },
      "Action": ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpPut"],
      "Resource": "arn:aws:es:ap-northeast-2:<AWS_ACCOUNT_ID>:domain/de-ai-05-lol-dev-an2-os-srsh/*"
    }
  ]
}
```

> 마스터 자격 증명은 Secrets Manager에 보관하고 Lambda 환경 변수로 주입합니다.
> 소스 코드나 설정 파일에 직접 기록하지 않습니다.

## 7. Lambda

두 함수 모두 `opensearch-py` 레이어가 필요하며, 환경 변수로
`OS_HOST` / `OS_USER` / `OS_PASS`를 주입합니다.

### os_loader

- 이름: `de-ai-05-lol-dev-an2-lmd-gld-os-loader`
- 코드: [`serving/lambda/os_loader.py`](../serving/lambda/os_loader.py)
- 트리거: Gold Kinesis 스트림 3개(score, chat, viewer)
  - 배치 크기: 100
  - 배치 윈도우: 1초

### api_handler

- 이름: `de-ai-05-lol-dev-an2-lmd-gld-api-handler`
- 코드: [`serving/lambda/api_handler.py`](../serving/lambda/api_handler.py)
- 트리거: API Gateway

## 8. API Gateway

- 이름: `de-ai-05-lol-dev-an2-agw-gld-api`
- IP 주소 유형: IPv4
- 리소스 경로: `/lol`
- 메서드: `GET`
- 통합 유형: Lambda (`...-lmd-gld-api-handler`)

배포 후 발급된 호출 URL을 다음 두 곳에 반영합니다.

- [`config/config.yaml`](../config/config.yaml)의 `serving_endpoint`
- 대시보드의 `window.LOL_API_URL`
  (또는 [`frontend/script.js`](../frontend/script.js)의 `CONFIG.API_URL`)
