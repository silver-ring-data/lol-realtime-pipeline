FROM python:3.12.12-slim

WORKDIR /app

# 필요한 패키지 설치 (pytchat, requests 등)
RUN pip install --no-cache-dir pytchat requests

# 소스 코드 및 데이터 복사
COPY . .

# 실행 명령어 (은비의 파일명에 맞춰 수정해줘!)
CMD ["python", "game_bot.py"]