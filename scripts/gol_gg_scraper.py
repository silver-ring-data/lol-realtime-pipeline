"""경기 이벤트를 타임라인 원본에 주입하는 전처리 스크립트.

외부 스프레드시트로 정리한 경기 이벤트(킬, 오브젝트 등)를 읽어 밀리초 단위로
정규화한 뒤, 타임스탬프가 일치하는 타임라인 프레임의 events 필드에 병합한다.

프로듀서가 송출할 최종 경기 데이터를 만들기 위한 1회성 전처리 도구다.
"""

import pandas as pd
import json
import os
import datetime

# 이벤트 데이터가 정리된 스프레드시트 (xlsx로 내보내기)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-pUsxNEso5MsmsQs7EAB10on-9u8ObCKxDwPgWgjrDw/export?format=xlsx"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, '..')
DATA_DIR = os.path.join(PROJECT_ROOT, 'mock_data')  # 결과물 저장 위치

def time_to_ms(time_val):
    """스프레드시트의 시간 값을 밀리초로 변환한다.

    시트에 따라 '01:24' 형식과 밀리초 숫자가 혼재하므로 두 경우를 모두 처리한다.
    """
    if pd.isna(time_val):
        return 0
    
    time_str = str(time_val).strip()
    try:
        # 'HH:MM:SS' 또는 'MM:SS' 형식
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) == 3:
                h, m, s = parts
                return (int(h) * 60 + int(m)) * 1000
            elif len(parts) == 2:
                m, s = parts
                return (int(m) * 60 + int(s)) * 1000
        # 이미 밀리초 숫자로 저장된 경우
        else:
            return int(float(time_str))
            
    except Exception as e:
        return 0

def safe_id(val, default=None):
    """숫자로 변환 가능하면 int로, 아니면 원본 문자열을 그대로 반환한다."""
    if pd.isna(val) or str(val).strip() in ('', '-'):
        return default
    
    val_str = str(val).strip()
    try:
        # 숫자 변환 시도 (예: 5.0 -> 5)
        return int(float(val_str))
    except ValueError:
        # 'DRAGON', 'NEXUS DESTROYED' 등 문자열 식별자는 그대로 유지
        return val_str

def parse_events(event_df):
    """이벤트 DataFrame을 밀리초(ms) 딕셔너리로 변환"""
    time_col = None
    for col in event_df.columns:
        if 'time' in str(col).lower() or 'stamp' in str(col).lower():
            time_col = col
            break
            
    if not time_col:
        print("시트에서 타임스탬프 컬럼을 찾을 수 없습니다.")
        return {}

    events_dict = {}
    for _, row in event_df.iterrows():
        t_ms = time_to_ms(row[time_col])
        if t_ms == 0: continue
        
        # 어시스트 목록은 숫자 ID와 문자열이 섞여 들어오므로 각각 변환한다.
        assists_raw = row.get('assisting_participant_ids', '')
        assists = []
        if pd.notna(assists_raw):
            raw_str = str(assists_raw).strip()
            if raw_str not in ('', '-'):
                for x in raw_str.split(','):
                    clean_x = x.strip()
                    if clean_x and clean_x != '-':
                        try:
                            assists.append(int(float(clean_x)))
                        except ValueError:
                            assists.append(clean_x)
            
        event_obj = {
            "timestamp": t_ms,
            "event_type": str(row.get('event_type', '')),
            "killer_id": safe_id(row.get('killer_id')),
            "assisting_participant_ids": assists,
            "victim_id": safe_id(row.get('victim_id')),
            "team_id": safe_id(row.get('team_id', 100), 100)
        }
        
        if t_ms not in events_dict:
            events_dict[t_ms] = []
        events_dict[t_ms].append(event_obj)
        
    return events_dict

def inject_events_to_mock(mock_relative_path, event_df, output_filename):
    events_dict = parse_events(event_df)
    
    # 타임스탬프 매칭이 어긋나는 경우를 조기에 확인하기 위한 샘플 출력
    sample_times = sorted(list(events_dict.keys()))[:5]
    print(f"  타임스탬프 샘플: {sample_times} ... (총 {len(events_dict)}개 시점)")
    
    mock_file_path = os.path.join(PROJECT_ROOT, mock_relative_path)
    output_file_path = os.path.join(DATA_DIR, output_filename)
    
    if not os.path.exists(mock_file_path):
        print(f"파일을 찾을 수 없습니다: {os.path.abspath(mock_file_path)}")
        return
        
    with open(mock_file_path, 'r', encoding='utf-8') as f:
        timeline = json.load(f)
        
    match_count = 0
    # 타임스탬프가 일치하는 프레임에 이벤트를 병합한다.
    for frame in timeline:
        t_ms = frame.get("timestamp", 0)
        frame_events = events_dict.get(t_ms, [])
        frame["events"] = frame_events
        if frame_events:
            match_count += len(frame_events)
            
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(timeline, f, indent=2, ensure_ascii=False)
        
    print(f"주입 완료: {os.path.abspath(output_file_path)} (이벤트 {match_count}건)\n")

# --- 실행부 ---
print("스프레드시트에서 이벤트 데이터를 내려받는 중입니다...")
sheets = pd.read_excel(SHEET_URL, sheet_name=None)

print("4세트 데이터를 병합하는 중입니다...")
inject_events_to_mock('mock_data/t1_blg_g4_exact.json', sheets['4_event'], 't1_blg_g4_final_stream.json')

print("5세트 데이터를 병합하는 중입니다...")
inject_events_to_mock('mock_data/t1_blg_g5_exact.json', sheets['5_event'], 't1_blg_g5_final_stream.json')

print("모든 작업이 완료되었습니다. 결과물은 mock_data 폴더에 있습니다.")