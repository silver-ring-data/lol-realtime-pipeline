# 자주 사용하는 명령어

개발 중 반복적으로 사용하는 명령을 모아둔 참고 문서입니다.

## Docker Compose

```bash
docker compose up -d --build      # 전체 스택 기동
docker compose logs -f api-server # API 서버 로그 확인
docker compose down               # 전체 중지 및 정리
```

## AWS CLI

자격 증명은 셸 히스토리에 남지 않도록 대화형 `aws configure`를 사용합니다.

```bash
# 설치 (Windows): https://awscli.amazonaws.com/AWSCLIV2.msi
aws configure                     # Access Key / Secret Key / Region 입력
aws configure set region ap-northeast-2
aws sts get-caller-identity       # 자격 증명 확인
```

## AWS CDK

```bash
# 사전 준비: Node.js LTS 설치 후
npm install -g aws-cdk

# Python 가상 환경
python -m venv .venv
.venv/Scripts/activate            # Windows
source .venv/bin/activate         # macOS / Linux
pip install -r requirements.txt

cdk bootstrap                     # 최초 1회
cdk deploy
deactivate
```

`cdk bootstrap` 실행 시 권한 오류가 발생하면 배포 주체에 다음 권한이 필요합니다.

- `cloudformation:*`
- `ssm:*`
- `ecr:*`

> Windows PowerShell에서 가상 환경 활성화가 차단되는 경우:
> `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`
