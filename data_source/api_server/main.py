from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import logging
import time

GAME_DATA_PATH="/source/game"
CHAT_DATA_PATH="/source/chat"

# 🚀 서버 앱 생성
app = FastAPI(
    title="LoL Data Source Ingestion API",
    description="경기 데이터와 채팅 데이터를 수집하여 파이프라인으로 넘겨주는 창구입니다.",
    version="1.0.0"
)

# 📋 로그 설정 (실시간 모니터링을 위해 상세하게!)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("DataSourceAPI")

# 🏥 1. 헬스체크 (봇들이 서버 상태를 확인하고 데이터를 쏘기 시작하는 기준점)
@app.get("/")
async def health_check():
    return {
        "status": "online",
        "message": "데이터 수집 창구가 활짝 열려 있습니다! 🚀",
        "server_time": time.strftime('%Y-%m-%d %H:%M:%S')
    }

# 🎮 2. 인게임 데이터 수신 창구
@app.post(GAME_DATA_PATH)
async def receive_game_data(request: Request):
    try:
        data = await request.json()
        
        # 필수 값 확인 (match_id, timestamp)
        match_id = data.get('match_id', 'Unknown')
        timestamp = data.get('timestamp', 0)
        
        logger.info(f"🎮 [GAME DATA] Match: {match_id} | Time: {timestamp}s 수신 성공")
        
        # 💡 [TODO: Phase 2] boto3를 사용하여 AWS Kinesis Data Streams로 데이터 전송
        # kinesis_client.put_record(StreamName='lol-game-stream', Data=json.dumps(data), ...)
        
        return {"status": "success", "received": "game_data"}
    
    except Exception as e:
        logger.error(f"❌ [GAME ERROR] 데이터 처리 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=400, detail="게임 데이터 형식이 올바르지 않아!")

# 💬 3. 채팅 데이터 수신 창구
@app.post(CHAT_DATA_PATH)
async def receive_chat_data(request: Request):
    try:
        data = await request.json()
        
        author = data.get('author_name', '익명')
        message = data.get('message', '')
        
        # 보안 및 가독성을 위해 메시지 앞부분만 로그에 남기기
        logger.info(f"💬 [CHAT DATA] {author}: {message[:15]}...")
        
        # 💡 [TODO: Phase 2] boto3를 사용하여 AWS Kinesis Data Streams로 데이터 전송
        # kinesis_client.put_record(StreamName='lol-chat-stream', Data=json.dumps(data), ...)
        
        return {"status": "success", "received": "chat_data"}

    except Exception as e:
        logger.error(f"❌ [CHAT ERROR] 채팅 데이터 처리 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=400, detail="채팅 데이터 처리에 실패했어!")

# ---------------------------------------------------------------------------
# 실행 방법: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# ---------------------------------------------------------------------------