from fastapi import FastAPI, Request
import logging

# 서버 앱(식당) 생성!
app = FastAPI(title="LoL Data Pipeline API")

# 로그 예쁘게 찍기 위한 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------------------------------
# 🥩 창구 1: 인게임 데이터 전용 창구
# ----------------------------------------------------
@app.post("/api/game")
async def receive_game_data(request: Request):
    data = await request.json()
    
    # 지금은 일단 콘솔에 잘 들어오는지 출력만 해보자!
    logger.info(f"🎮 [게임 데이터 도착] 시간: {data.get('timestamp')}초")
    
    # 나중에는 여기서 데이터를 Kafka나 Flink로 넘겨줄 거야! (Dumb Pipe 역할)
    
    return {"status": "success", "message": "게임 데이터 잘 받았어!"}

# ----------------------------------------------------
# 🍺 창구 2: 채팅 데이터 전용 창구
# ----------------------------------------------------
@app.post("/api/chat")
async def receive_chat_data(request: Request):
    data = await request.json()
    
    # 채팅 내용 살짝 출력해보기
    author = data.get('author_name', '익명')
    msg = data.get('message', '')
    logger.info(f"💬 [채팅 도착] {author}: {msg}")
    
    # 나중에는 여기서 채팅 데이터를 Kafka로 쏴줄 거야!
    
    return {"status": "success", "message": "채팅 데이터 잘 받았어!"}

# ----------------------------------------------------
# 🏥 서버가 살아있는지 확인하는 헬스체크 창구
# ----------------------------------------------------
@app.get("/")
def health_check():
    return {"message": "서버가 아주 건강하게 돌아가고 있습니다! 🚀"}