import pandas as pd
import json
import os
import datetime

# 구글 스프레드시트 주소
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-pUsxNEso5MsmsQs7EAB10on-9u8ObCKxDwPgWgjrDw/export?format=xlsx"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, '..')       # v0.1_mvp_basic 폴더
DATA_DIR = os.path.join(PROJECT_ROOT, 'mock_data')     # 결과물을 저장할 data 폴더

def time_to_ms(time_val):
    """엑셀의 타임스탬프를 유연하게 파싱 (이미 밀리초 숫자로 들어온 경우 완벽 방어!)"""
    if pd.isna(time_val):
        return 0
    
    time_str = str(time_val).strip()
    try:
        # 1. 엑셀에 '01:24' 같은 시간 형식이 남아있는 경우
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 3:
                h, m, s = parts
                return (int(h) * 60 + int(m)) * 1000
            elif len(parts) == 2:
                m, s = parts
                return (int(m) * 60 + int(s)) * 1000
        # 2. 엑셀에 이미 '182000' 처럼 숫자로 예쁘게 들어온 경우!
        else:
            return int(float(time_str)) # 1000 곱하던 걸 과감히 뺐어!
            
    except Exception as e:
        return 0

def safe_id(val, default=None):
    """숫자면 정수로, 글자면 문자열 그대로 반환하는 만능 함수"""
    if pd.isna(val) or str(val).strip() in ('', '-'):
        return default
    
    val_str = str(val).strip()
    try:
        # 1. 먼저 숫자로 변환 시도 (예: 5.0 -> 5, "3" -> 3)
        return int(float(val_str))
    except ValueError:
        # 2. 숫자로 바꿀 수 없는 글자(예: "DRAGON", "T1 타워")면 글자 그대로 반환!
        return val_str

def parse_events(event_df):
    """이벤트 DataFrame을 밀리초(ms) 딕셔너리로 변환"""
    time_col = None
    for col in event_df.columns:
        if 'time' in str(col).lower() or 'stamp' in str(col).lower():
            time_col = col
            break
            
    if not time_col:
        print("❌ 에러: 엑셀에서 시간(Timestamp) 컬럼을 찾을 수 없어!")
        return {}

    events_dict = {}
    for _, row in event_df.iterrows():
        t_ms = time_to_ms(row[time_col])
        if t_ms == 0: continue
        
        # 어시스트 전처리 (문자열/숫자 섞여 있어도 완벽 방어)
        assists_raw = row.get('assisting_participant_ids', '')
        assists = []
        if pd.notna(assists_raw):
            raw_str = str(assists_raw).strip()
            if raw_str not in ('', '-'):
                for x in raw_str.split(','):
                    clean_x = x.strip()
                    if clean_x and clean_x != '-':
                        try:
                            # 숫자면 숫자로
                            assists.append(int(float(clean_x)))
                        except ValueError:
                            # 글자면 글자로 리스트에 추가
                            assists.append(clean_x)
            
        event_obj = {
            "timestamp": t_ms,
            "event_type": str(row.get('event_type', '')),
            "killer_id": safe_id(row.get('killer_id')), # 👈 업그레이드된 함수 적용!
            "assisting_participant_ids": assists,
            "victim_id": safe_id(row.get('victim_id')), # 👈 업그레이드된 함수 적용!
            "team_id": safe_id(row.get('team_id', 100), 100)
        }
        
        if t_ms not in events_dict:
            events_dict[t_ms] = []
        events_dict[t_ms].append(event_obj)
        
    return events_dict

def inject_events_to_mock(mock_relative_path, event_df, output_filename):
    events_dict = parse_events(event_df)
    
    # 디버깅: 엑셀에서 변환된 시간(ms) 몇 개만 살짝 보여주기
    sample_times = sorted(list(events_dict.keys()))[:5]
    print(f"  👉 [디버그] 엑셀에서 읽어온 타임스탬프 예시: {sample_times} ... (총 {len(events_dict)}개 순간)")
    
    mock_file_path = os.path.join(PROJECT_ROOT, mock_relative_path)
    output_file_path = os.path.join(DATA_DIR, output_filename)
    
    if not os.path.exists(mock_file_path):
        print(f"❌ 파일을 찾을 수 없어: {os.path.abspath(mock_file_path)}")
        return
        
    with open(mock_file_path, 'r', encoding='utf-8') as f:
        timeline = json.load(f)
        
    match_count = 0
    # 타임스탬프 매칭해서 이벤트 끼워넣기
    for frame in timeline:
        t_ms = frame.get("timestamp", 0)
        frame_events = events_dict.get(t_ms, [])
        frame["events"] = frame_events
        if frame_events:
            match_count += len(frame_events) # 꽂아넣은 이벤트 개수 세기
            
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(timeline, f, indent=2, ensure_ascii=False)
        
    print(f"✅ 주입 완료! -> {os.path.abspath(output_file_path)} (🎉 총 {match_count}개의 이벤트가 정확히 꽂힘!)\n")

# --- 실행부 ---
print("스프레드시트에서 이벤트 데이터 다운로드 중... ⏳")
sheets = pd.read_excel(SHEET_URL, sheet_name=None)

print("4경기 데이터 합치는 중... 🚀")
inject_events_to_mock('mock_data/t1_blg_g4_exact.json', sheets['4_event'], 't1_blg_g4_final_stream.json')

print("5경기 데이터 합치는 중... 🚀")
inject_events_to_mock('mock_data/t1_blg_g5_exact.json', sheets['5_event'], 't1_blg_g5_final_stream.json')

print("🎉 모든 작업이 끝났어! 'data' 폴더를 확인해 봐!")