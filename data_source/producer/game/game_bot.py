import requests
import json
import time
import os
import logging

# 📋 로그 설정 (게임 캐스터 느낌으로!)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 게임봇: %(message)s')
logger = logging.getLogger("GameProducer")

# 🌍 설정 정보 (환경변수에서 읽어오기)
API_URL = os.getenv("API_URL", "http://api-server:8000/source/game")
# 은비가 만든 소중한 4경기 또는 5경기 데이터를 여기에 매핑!
DATA_FILE = "./data/t1_blg_g4_exact.json" 

def wait_for_server():
    """API 서버가 켜질 때까지 문 두드리기"""
    health_url = API_URL.replace("/source/game", "/")
    logger.info(f"⏳ 서버 접속 시도 중... ({health_url})")
    while True:
        try:
            response = requests.get(health_url, timeout=2)
            if response.status_code == 200:
                logger.info("✅ 서버 연결 성공! 인게임 데이터 중계를 시작합니다. 🎙️")
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)

def send_game_data():
    # 1. JSON 파일 읽어오기
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            game_data_list = json.load(f)
            
            # 만약 데이터가 리스트가 아니라 단일 딕셔너리면 리스트로 감싸주기
            if isinstance(game_data_list, dict):
                game_data_list = [game_data_list]
                
    except FileNotFoundError:
        logger.error(f"❌ 데이터 파일을 찾을 수 없어! 경로를 확인해줘: {DATA_FILE}")
        return

    logger.info(f"🚀 총 {len(game_data_list)}개의 인게임 프레임(시간대) 데이터를 로드했어.")

    # 2. 시간에 맞춰 데이터 쏘기
    for frame in game_data_list:
        try:
            response = requests.post(API_URL, json=frame, timeout=5)
            
            match_id = frame.get('match_id', 'Unknown')
            timestamp = frame.get('timestamp', 0)
            
            if response.status_code == 200:
                logger.info(f"🎮 전송 성공: [매치: {match_id} | 게임시간: {timestamp}초]")
            else:
                logger.warning(f"⚠️ 전송 실패! 상태 코드: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ 에러 발생: {e}")
        
        # 💡 라이엇 API 느낌을 살리기 위해 1초 대기 (1초마다 데이터 갱신)
        time.sleep(1.0)

if __name__ == "__main__":
    wait_for_server()
    send_game_data()