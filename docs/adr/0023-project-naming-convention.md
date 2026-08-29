# 0023. 프로젝트 명명 규칙 적용

> 원본: https://app.notion.com/p/34c1198df8d98081bfddd6029ba5ca6e

| 상태 | 날짜 |
|---|---|
| 폐기 | 2026-04-24 |

## 현상

- LoL 하이라이트 탐지 시스템이라는 목적성을 명확히 하기 위해 프로젝트 명칭을 `lol-highlighter`로 변경함.
- AWS 리소스 관리 효율화를 위해 `{project}-{env}-{region}-{service}-{data}` 규칙을 전면 적용함.

## 결정

- 프로젝트 공식 명칭을 **`lol-highlighter`**로 확정함.
- 모든 AWS 리소스 생성 시 위에서 정의한 명세서의 이름을 사용함.
- 운영 리전은 서울(ap-northeast-2)로 고정하며 약어는 `an2`를 사용함.

## 이유

### 🗺️ lol-highlighter 1차 구현 리소스 명세 (v1.1)

### AWS 인프라 리소스 명명 규칙

AWS 콘솔에서 생성되는 모든 물리적 리소스는 다음 패턴을 따릅니다.

> **`{project}-{env}-{region}-{service}-{data}`**

- **project**: 프로젝트 식별자 (예: `tyrant`)
- **env**: 운영 환경 (예: `dev`, `test`, `prod`)
- **region**: 리전 약어 (예: 서울 리전은 `an2`)
- **service**: 서비스 약어
  - `kds`: Kinesis Data Streams
  - `kdf`: Kinesis Data Firehose
  - `s3`: Simple Storage Service
  - `iam`: IAM Role/Policy
- **data**: 데이터의 성격 및 목적 (예: `chat-raw`, `match-cleansed`)

### 1. Ingestion & Streaming (수집 및 전송)

- **Kinesis Data Streams (KDS)**
  - 채팅 수집:
    - 브론즈 : `lol-highlighter-dev-an2-kds-brz-chat`
    - 실버 : `lol-highlighter-dev-an2-kds-slv-chat`
  - 게임 이벤트 수집:
    - 브론즈 : `lol-highlighter-dev-an2-kds-brz-game`
    - 실버 : `lol-highlighter-dev-an2-kds-slv-game`
- **Kinesis Data Firehose (KDF)**
  - 채팅 S3 전송:
    - 브론즈 : `lol-highlighter-dev-an2-kdf-brz-chat`
    - 실버 : `lol-highlighter-dev-an2-kdf-slv-chat`
  - 게임 S3 전송: lol-highlighter-dev-an2-kdf-brz-chat
    - 브론즈 : `lol-highlighter-dev-an2-kdf-brz-game`
    - 실버 : `lol-highlighter-dev-an2-kdf-slv-game`
- Studio notebook
  - `lol-highlighter-dev-an2-stdo-analyzer`

### 2. Storage & Analysis (저장 및 분석)

- **Amazon S3 (Data Lake)**
  - 원본 데이터 저장소: `{user_id}/lol-highlighter`
- **Amazon RDS (Serving DB)**
  - 결과 데이터 저장용: `lol-highlighter-dev-an2-rds-highlight` (PostgreSQL)
- **Managed Service for Apache Flink**
  - 실시간 분석 앱: `lol-highlighter-dev-an2-flink-analyzer`
- glue
  - : **`lol_highlighter_dev_an2_glue_db`**

### 3. Serving & Search (서빙 및 검색)

- **Amazon OpenSearch**
  - 실시간 검색 클러스터: `lol-highlighter-dev-an2-os-search`
- **EC2 / API Server**
  - 백엔드 서버: `lol-highlighter-dev-an2-ec2-api`

## 트레이드오프

(원문에 없음)
