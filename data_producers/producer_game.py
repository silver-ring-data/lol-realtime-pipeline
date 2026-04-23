import json
import time
import requests

# 1. 설정 (main.py의 주소와 완벽하게 일치시킴!)
API_URL = "http://127.0.0.1:8000/api/game"  
DATA_FILE = "sample_data/t1_blg_g4_final_stream.json"  # 경로 통일

def run_game_producer():
    print("📂 인게임 데이터를 메모리에 장전하는 중...")
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            saved_data_list = json.load(f)
    except FileNotFoundError:
        print(f"🚨 앗! {DATA_FILE} 파일을 찾을 수 없어. 경로를 다시 확인해줘!")
        return

    print(f"✅ 총 {len(saved_data_list)}개의 게임 데이터 발사 준비 완료! 🎮")
    print("-" * 50)

 # 2. 실시간처럼 1초에 한 번씩 쏘기!
    for frame_data in saved_data_list:
        try:
            # API 서버에 데이터 쏘기
            response = requests.post(API_URL, json=frame_data)
            
            # 💡 [시간 처리] 40000, 41000 처럼 1000단위로 늘어나는 걸 보니 밀리초(ms) 같아! 
            # 1000으로 나눠서 깔끔하게 초(s) 단위로 보여주자.
            raw_time = frame_data.get('timestamp', 0)
            sec_time = int(raw_time / 1000) if isinstance(raw_time, (int, float)) else raw_time
            
            # 💡 [데이터 요약] JSON 데이터에서 앞부분 40글자만 싹둑 잘라서 보여주기!
            # (만약 T1 골드, KDA 등 특정 지표만 보고 싶다면 frame_data.get('t1_gold') 처럼 뽑아내도 돼)
            data_snippet = str(frame_data)[:50] 
            
            print(f"[{sec_time}초] 🎮 {data_snippet}... -> 🎯 응답: {response.status_code}")
            
        except Exception as e:
            print(f"🚨 전송 실패: {e}")
        
        # 3. 다음 데이터를 보내기 전 1초 동안 숨참기
        time.sleep(1)

if __name__ == "__main__":
    run_game_producer()