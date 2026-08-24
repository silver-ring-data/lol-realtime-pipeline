FROM python:3.12.12-slim

WORKDIR /app

# 의존성 설치
RUN pip install --no-cache-dir pytchat requests

# 소스 코드 및 데이터 복사
COPY . .

# 실행 명령
CMD ["python", "chat_bot.py"]