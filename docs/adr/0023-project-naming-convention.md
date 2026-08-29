# 0023. 프로젝트 명명 규칙을 적용한다 (lol-highlighter)

> 원본: https://app.notion.com/p/34c1198df8d98081bfddd6029ba5ca6e

| | |
|---|---|
| 상태 | **폐기** (→ 0033) |
| 날짜 | 2026-04-24 |
| 결정자 | silver-ring-data |
| 관련 | 0024, 0033 |

## 배경

LoL 하이라이트 탐지 시스템이라는 목적을 명확히 하기 위해 프로젝트 명칭을 `lol-highlighter` 로 변경하고, AWS 리소스 관리 효율화를 위해 통일된 명명 규칙이 필요했다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. `{project}-{env}-{region}-{service}-{data}` 규칙 전면 적용 | 리소스 식별·관리 효율화 | 이름이 길어짐 |
| B. 자유 명명 | 없음 | 리소스 혼재 |

## 결정

**A 를 채택한다.** 프로젝트 공식 명칭을 `lol-highlighter` 로 확정하고, 모든 AWS 리소스에 위 규칙을 적용한다. 운영 리전은 서울(ap-northeast-2)로 고정하며 약어는 `an2` 를 사용한다.

- project: 프로젝트 식별자 / env: `dev`, `test`, `prod` / region: `an2` / service: `kds`, `kdf`, `s3`, `iam` 등 / data: `chat-raw`, `match-cleansed` 등

1차 구현 리소스 명세(v1.1) 예시:

- Kinesis Data Streams: `lol-highlighter-dev-an2-kds-brz-chat`, `...-kds-slv-chat`, `...-kds-brz-game`, `...-kds-slv-game`
- Kinesis Data Firehose: `lol-highlighter-dev-an2-kdf-brz-chat`, `...-kdf-slv-chat`, `...-kdf-brz-game`, `...-kdf-slv-game`
- Studio notebook: `lol-highlighter-dev-an2-stdo-analyzer`
- RDS(PostgreSQL): `lol-highlighter-dev-an2-rds-highlight`
- Managed Flink: `lol-highlighter-dev-an2-flink-analyzer`
- Glue DB: `lol_highlighter_dev_an2_glue_db`
- OpenSearch: `lol-highlighter-dev-an2-os-search`
- EC2 API 서버: `lol-highlighter-dev-an2-ec2-api`

## 결과

- 좋아지는 것: 리소스 소유·용도를 이름만으로 파악한다.
- 감수하는 것: 긴 리소스 이름.
- 다시 볼 조건: 공용 계정에서 사용자 식별자가 필요해질 때. (0033 에서 `{user_id}` 접두사를 포함한 규칙으로 대체되며 폐기되었다.)
