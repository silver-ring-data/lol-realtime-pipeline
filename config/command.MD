docker compose logs -f api-server
docker compose up -d --build
docker compose down

# CDK
Node.js 설치 (LTS)
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
npm install -g aws-cdk
cdk init app --language python

.\.venv\Scripts\activate
deactivate
pip install -r requirements.txt

https://awscli.amazonaws.com/AWSCLIV2.msi
aws configure set aws_access_key_id [계정입력]
aws configure set aws_secret_access_key [비밀번호입력]
aws configure set region ap-northeast-2

aws sts get-caller-identity

cdk bootstrap
# 권한문제 : 
"cloudformation:*",
"ssm:*",
"ecr:*"