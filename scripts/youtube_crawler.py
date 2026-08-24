"""YouTube 라이브 채팅 수집 스크립트.

pytchat으로 지정한 영상의 라이브 채팅을 JSONL로 저장한다.
파이프라인 시뮬레이션에 사용할 채팅 원본을 확보하기 위한 도구다.

의존성: pip install pytchat

주의: 수집한 채팅에는 시청자 닉네임이 포함되므로, 저장소에 커밋하기 전에
      반드시 익명화(scripts/anonymize_chat.py)를 거친다.
"""

import os
import pytchat
import json

VIDEO_ID = "NsWPXB5Wqzs" 
OUTPUT_FILE = "../mock_data/t1_vs_blg_chat.jsonl"

def download_youtube_chat_pytchat(video_id, output_filename):
    print(f"[{video_id}] 라이브 채팅 수집을 시작합니다.")

    # 출력 디렉터리가 없으면 생성한다.
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    
    try:
        chat = pytchat.create(video_id=video_id)
        count = 0
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            while chat.is_alive():
                for c in chat.get().sync_items():
                    chat_data = {
                        "timestamp": c.timestamp,       
                        "time_text": c.elapsedTime,     
                        "nickname": c.author.name,
                        "content": c.message,
                        "platform": "youtube"
                    }
                    f.write(json.dumps(chat_data, ensure_ascii=False) + '\n')
                    count += 1
                    
                    if count % 100 == 0:
                        print(f"수집 중... {count}건 저장")
                        
        print(f"수집 완료: 총 {count}건을 '{output_filename}'에 저장했습니다.")
        
    except Exception as e:
        print(f"채팅 수집 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    download_youtube_chat_pytchat(VIDEO_ID, OUTPUT_FILE)