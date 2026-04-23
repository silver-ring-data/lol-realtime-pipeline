import json
import time
import requests
import logging

# 1. 설정: 127.0.0.1로 명확히 지정해서 404 에러 원천 차단!
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 채팅봇: %(message)s')
logger = logging.getLogger("ChatProducer")
API_URL = "http://api-server:8000/source/chat"
DATA_FILE = "data/t1_blg_chat.jsonl"  # 은비 환경에 맞게 경로 맞춰줘

def wait_for_server():
    """서버가 완전히 켜질 때까지 헬스체크를 보내며 대기해!"""
    health_url = API_URL.replace("/source/chat", "/")
    logger.info(f"⏳ 서버 접속 시도 중... ({health_url})")
    while True:
        try:
            response = requests.get(health_url, timeout=2)
            if response.status_code == 200:
                logger.info("✅ 서버 연결 성공! 데이터를 쏘기 시작합니다.")
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)

def run_chat_producer():
    # 2. 데이터 불러오기 (JSON Lines 형식 완벽 지원)
    print("📂 채팅 데이터를 메모리에 장전하는 중...")
    chat_data_list = []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chat_data_list.append(json.loads(line))
    except FileNotFoundError:
        print(f"🚨 앗! {DATA_FILE} 파일을 찾을 수 없어.")
        return

    print(f"✅ 총 {len(chat_data_list)}개의 채팅 발사 준비 완료! 🔫")
    print("-" * 50)

    if not chat_data_list:
        return
    
    # 3. 기준 시간 잡기
    prev_time = chat_data_list[0]['timestamp']

    # 4. 실시간 폭주 시뮬레이션
    for chat in chat_data_list:
        current_time = chat['timestamp']
        
        # 💡 핵심 수정: 밀리초(ms) 단위 차이를 구한 뒤 1000으로 나눠서 '초' 단위로 변환!
        sleep_time = (current_time - prev_time) / 1000.0
        
        # 소수점 시간만큼 정확하게 대기 (예: 0.1초, 1초 등)
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        try:
            # 서버로 전송
            response = requests.post(API_URL, json=chat)
            
            # 💡 핵심 수정: 은비 데이터에 맞는 Key 이름(nickname, content)으로 변경!
            author = chat.get('nickname', '익명')
            message = chat.get('content', '내용없음')
            
            # 로그 출력 (응답 코드가 200이면 성공!)
            print(f"[{current_time}] 💬 {author}: {message[:20]}... -> 🎯 전송 ({response.status_code})")
            
        except Exception as e:
            print(f"🚨 전송 실패: {e}")
            time.sleep(2)
            
        # 다음 루프를 위해 시간 업데이트
        prev_time = current_time

if __name__ == "__main__":
    wait_for_server()
    run_chat_producer()