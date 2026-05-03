import json
import os

def update_match_id(file_path, new_id):
    try:
        # 1. 파일 읽기
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 2. match_id 값 수정
        for entry in data:
            if 'match_id' in entry:
                entry['match_id'] = new_id
        
        # 3. 변경된 내용 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        print(f"✅ 수정 완료: {os.path.basename(file_path)}")
        
    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없어. 경로를 다시 확인해줘: {file_path}")
    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")

# --- 실행 부분 ---

# 은링이 알려준 절대 경로 사용 (문자열 앞에 r을 붙이는 게 핵심이야!)
g4_path = r"C:\Users\cindy\OneDrive\문서\project\Bootcamp\lol-realtime-pipeline\data_source\producer\game\data\t1_blg_g4_exact.json"
update_match_id(g4_path, "worlds_2024_20241102_t1_blg_g4")

# g5 파일도 같은 폴더에 있다면 아래처럼 경로만 바꿔서 실행하면 돼
g5_path = r"C:\Users\cindy\OneDrive\문서\project\Bootcamp\lol-realtime-pipeline\data_source\producer\game\data\t1_blg_g5_exact.json"
update_match_id(g5_path, "worlds_2024_20241102_t1_blg_g5")