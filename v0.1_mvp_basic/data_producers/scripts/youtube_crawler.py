'''
-pip install pytchat
'''

import os
import pytchat
import json

VIDEO_ID = "NsWPXB5Wqzs" 
OUTPUT_FILE = "../mock_data/t1_vs_blg_chat.jsonl"

def download_youtube_chat_pytchat(video_id, output_filename):
    print(f"🎬 [Video ID: {video_id}] pytchat으로 채팅 추출을 시작합니다...")
    
    # 💡 폴더 없으면 알아서 만들기!
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
                        print(f"📥 폭풍 수집 중... 현재 {count}개 저장 완료!")
                        
        print(f"\n✅ 수집 끝! 총 {count}개의 데이터가 '{output_filename}'에 장전됐어. 🚀")
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    download_youtube_chat_pytchat(VIDEO_ID, OUTPUT_FILE)