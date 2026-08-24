"""시청자 수 스트림 프로듀서.

방송 경과 시간에 따라 시청자 수를 시뮬레이션하여 Kinesis Data Streams로
전송한다. 경기 진행/휴식/세리머니 구간별로 서로 다른 증감 곡선을 적용하고,
세트 종료 직전에는 스파이크를 반영한다.

구간 경계값은 config.yaml의 세트별 대기·진행 시간에서 계산하므로 대상 경기가
바뀌어도 코드 수정이 필요 없다.
"""

import time
import random
import json
import boto3
import logging
import yaml
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("ViewerBot")

# ---------------------------------------------------------
# 1. Config 로딩
# ---------------------------------------------------------
CONFIG_PATH = os.environ.get('CONFIG_PATH', '/opt/airflow/config/config.yaml')
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']

metadata = config['matches'][match_key]['metadata']
game_sets = config['matches'][match_key]['data']['game_sets']
timeline = config['matches'][match_key]['timeline']

REGION = config['global']['region']
kinesis_client = boto3.client('kinesis', region_name=REGION)
STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['viewer_brz']

# 세트별 대기 시간과 진행 시간
g4_wait = game_sets['g4']['wait_seconds']
g4_duration = game_sets['g4']['duration_seconds']
g5_wait = game_sets['g5']['wait_seconds']
g5_duration = game_sets['g5']['duration_seconds']

def get_current_match_id(elapsed_seconds, timeline_config, metadata):
    """경과 시간이 속한 구간에 해당하는 match_id를 반환한다."""
    current_stage = "post_game"  # 타임라인에 속하지 않으면 경기 종료 이후로 간주
    
    for stage_key, info in timeline_config.items():
        start = info['start_seconds']
        end = start + info['duration']
        if start <= elapsed_seconds < end:
            current_stage = stage_key
            break

    # 경기 외 구간은 구간명만 반환한다.
    if current_stage in ['pre_game', 'intermission', 'post_game']:
        return current_stage
    
    # 경기 중에는 전체 포맷으로 조립한다. 예: worlds_2024_20241102_t1_blg_g4
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{current_stage}"

# ---------------------------------------------------------
# 2. 구간 경계 자동 계산
# ---------------------------------------------------------
G4_END_TIME = g4_wait + g4_duration
G5_START_TIME = g5_wait
G5_END_TIME = g5_wait + g5_duration

logger.info(f"[타임라인] 4세트 종료 {G4_END_TIME}초 | 5세트 시작 {G5_START_TIME}초 | 5세트 종료 {G5_END_TIME}초")

def get_real_viewer_count_dynamic(elapsed_seconds):
    """경과 시간에 따른 시청자 수를 구간별 곡선으로 산출한다."""
    base_viewers = 35000 
    
    # [1구간] 4경기 진행 중
    if elapsed_seconds <= G4_END_TIME:
        progress = elapsed_seconds / G4_END_TIME
        viewers = base_viewers + (progress * 35000)
        
        # 세트 종료 200초 전부터 시청자 수 스파이크 적용
        if elapsed_seconds > (G4_END_TIME - 200):
            spike = ((elapsed_seconds - (G4_END_TIME - 200)) / 200) * 8000
            viewers += spike
            
    # [2구간] 쉬는 시간 (4경기 종료 ~ 5경기 시작 전)
    elif G4_END_TIME < elapsed_seconds <= G5_START_TIME:
        peak_g4_viewers = 78000
        break_duration = G5_START_TIME - G4_END_TIME
        current_break_time = elapsed_seconds - G4_END_TIME
        
        # 휴식 구간 동안 점진적으로 감소
        viewers = peak_g4_viewers - ((current_break_time / break_duration) * 28000)
        
    # [3구간] 5경기 진행 중
    elif G5_START_TIME < elapsed_seconds <= G5_END_TIME:
        g5_elapsed = elapsed_seconds - G5_START_TIME
        
        base_g5 = 50000 
        progress = g5_elapsed / g5_duration
        viewers = base_g5 + (progress * 40000) 
        
        # 최종 세트 종료 300초 전부터 스파이크 폭을 크게 적용
        if g5_elapsed > (g5_duration - 300):
            spike = ((g5_elapsed - (g5_duration - 300)) / 300) * 15000 
            viewers += spike
            
    # [4구간] 경기 종료 후 세리머니
    else:
        ceremony_time = elapsed_seconds - G5_END_TIME
        viewers = 105000 - (ceremony_time * 10) 
        
    return int(viewers + random.randint(-300, 300))

# ---------------------------------------------------------
# 3. 실행 로직
# ---------------------------------------------------------
def run_viewer_producer():
    logger.info("시청자 수 시뮬레이션을 시작합니다.")
    start_time = time.time()
    
    # 전체 방송 종료 시각 = 최종 세트 종료 + 세리머니 10분
    total_broadcast_time = G5_END_TIME + 600 

    try:
        while True:
            # 1. 경과 시간 계산
            elapsed_seconds = int(time.time() - start_time)
            
            # 2. 현재 구간에 해당하는 match_id 결정
            current_match_id = get_current_match_id(elapsed_seconds, timeline, metadata)
            
            # 3. 시청자 수 계산
            current_viewers = get_real_viewer_count_dynamic(elapsed_seconds)
            
            # 4. 전송할 페이로드 구성
            viewer_data = {
                "match_id": current_match_id,  # pre_game / intermission / worlds_2024_..._g4 등
                "timestamp": int(time.time() * 1000), 
                "viewer_count": current_viewers,
                "elapsed_seconds": elapsed_seconds
            }
            
            logger.info(f"[{current_match_id}] {elapsed_seconds}초 경과 | 시청자 수 {current_viewers:,}명")
            
            # 5. Kinesis로 데이터 전송
            kinesis_client.put_record(
                StreamName=STREAM_NAME, 
                Data=json.dumps(viewer_data).encode('utf-8'),
                PartitionKey="viewer"
            )
            
            # 6. 세리머니까지 종료되면 프로듀서를 종료한다.
            if elapsed_seconds >= total_broadcast_time:
                logger.info("방송이 종료되어 시청자 프로듀서를 종료합니다.")
                break
                
            # 7. 시청자 수는 변화가 완만하므로 10초 주기로 전송한다.
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("사용자 요청으로 시청자 프로듀서를 종료합니다.")

if __name__ == "__main__":
    run_viewer_producer()