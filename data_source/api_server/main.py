"""데이터 소스 수집 API 서버.

프로듀서가 발생시킨 경기/채팅 데이터를 수신하는 HTTP 엔드포인트를 제공한다.
수신한 데이터는 후속 단계에서 Kinesis Data Streams로 전달된다.
"""

import logging
import time

from fastapi import FastAPI, Request, HTTPException

GAME_DATA_PATH="/source/game"
CHAT_DATA_PATH="/source/chat"

# 애플리케이션 생성
app = FastAPI(
    title="LoL Data Source Ingestion API",
    description="경기 데이터와 채팅 데이터를 수집하여 파이프라인으로 넘겨주는 창구입니다.",
    version="1.0.0"
)

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("DataSourceAPI")

# 1. 헬스체크: 프로듀서가 송출 전에 서버 가용 여부를 확인한다.
@app.get("/")
async def health_check():
    return {
        "status": "online",
        "message": "데이터 수집 API가 정상 동작 중입니다.",
        "server_time": time.strftime('%Y-%m-%d %H:%M:%S')
    }

# 2. 인게임 데이터 수신 엔드포인트
@app.post(GAME_DATA_PATH)
async def receive_game_data(request: Request):
    try:
        data = await request.json()
        
        # 필수 값 확인 (match_id, timestamp)
        match_id = data.get('match_id', 'Unknown')
        timestamp = data.get('timestamp', 0)
        
        logger.info(f"[GAME] match_id={match_id} timestamp={timestamp} 수신 완료")
        
        # TODO(phase-2): boto3로 Kinesis Data Streams에 전송
        # kinesis_client.put_record(StreamName='lol-game-stream', Data=json.dumps(data), ...)
        
        return {"status": "success", "received": "game_data"}
    
    except Exception as e:
        logger.error(f"[GAME] 데이터 처리 중 오류가 발생했습니다: {str(e)}")
        raise HTTPException(status_code=400, detail="게임 데이터 형식이 올바르지 않습니다.")

# 3. 채팅 데이터 수신 엔드포인트
@app.post(CHAT_DATA_PATH)
async def receive_chat_data(request: Request):
    try:
        data = await request.json()
        
        author = data.get('author_name', 'anonymous')
        message = data.get('message', '')
        
        # 개인정보 노출을 줄이기 위해 메시지 앞부분만 기록한다.
        logger.info(f"[CHAT] {author}: {message[:15]}...")
        
        # TODO(phase-2): boto3로 Kinesis Data Streams에 전송
        # kinesis_client.put_record(StreamName='lol-chat-stream', Data=json.dumps(data), ...)
        
        return {"status": "success", "received": "chat_data"}

    except Exception as e:
        logger.error(f"[CHAT] 데이터 처리 중 오류가 발생했습니다: {str(e)}")
        raise HTTPException(status_code=400, detail="채팅 데이터 처리에 실패했습니다.")

# ---------------------------------------------------------------------------
# 실행 방법: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# ---------------------------------------------------------------------------