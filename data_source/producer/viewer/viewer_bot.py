import time
import random
import json
import boto3
import logging
import yaml
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 시청자봇: %(message)s')
logger = logging.getLogger("ViewerBot")

# ---------------------------------------------------------
# 1. Config 파일에서 변수 가져오기! (하드코딩 제로✨)
# ---------------------------------------------------------
#CONFIG_PATH = r'C:\Users\Dell3571\Documents\lol-realtime-pipeline\config\config.yaml'
CONFIG_PATH = "/opt/airflow/config/config.yaml"  
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']

# ✨ 수정포인트: metadata 뽑아오기 추가!
metadata = config['matches'][match_key]['metadata']
game_sets = config['matches'][match_key]['data']['game_sets']
timeline = config['matches'][match_key]['timeline']

REGION = config['global']['region']
kinesis_client = boto3.client('kinesis', region_name=REGION)
STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['viewer_brz']

# 4경기, 5경기 시간 변수로 쏙쏙 빼오기
g4_wait = game_sets['g4']['wait_seconds']          # 55초
g4_duration = game_sets['g4']['duration_seconds']  # 1902초
g5_wait = game_sets['g5']['wait_seconds']          # 3182초
g5_duration = game_sets['g5']['duration_seconds']  # 1933초

def get_current_match_id(elapsed_seconds, timeline_config, metadata):
    # 1. 지금이 타임라인 상 어디인지 확인
    current_stage = "post_game" # 기본값
    
    for stage_key, info in timeline_config.items():
        start = info['start_seconds']
        end = start + info['duration']
        if start <= elapsed_seconds < end:
            current_stage = stage_key # 'pre_game', 'g4', 'intermission', 'g5' 중 하나
            break

    # 2. 대기 시간이나 쉬는 시간이면 그냥 깔끔하게 단어만 반환! (요청사항 100% 반영)
    if current_stage in ['pre_game', 'intermission', 'post_game']:
        return current_stage
    
    # 3. 경기 중(g4, g5)일 때만 원래 포맷으로 조립!
    # 예: worlds_2024_20241102_t1_blg_g4
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{current_stage}"

# ---------------------------------------------------------
# 2. 구간(Boundary) 자동 계산!
# ---------------------------------------------------------
G4_END_TIME = g4_wait + g4_duration       # 1957초
G5_START_TIME = g5_wait                   # 3182초
G5_END_TIME = g5_wait + g5_duration       # 5115초

logger.info(f"📊 [타임라인 세팅] 4경기 종료: {G4_END_TIME}초 | 5경기 시작: {G5_START_TIME}초 | 5경기 종료: {G5_END_TIME}초")

def get_real_viewer_count_dynamic(elapsed_seconds):
    """config 변수 기반으로 작동하는 시청자 수 생성기"""
    base_viewers = 35000 
    
    # [1구간] 4경기 진행 중
    if elapsed_seconds <= G4_END_TIME:
        progress = elapsed_seconds / G4_END_TIME
        viewers = base_viewers + (progress * 35000)
        
        # 4경기 끝날 때쯤 (종료 200초 전부터 스파이크)
        if elapsed_seconds > (G4_END_TIME - 200):
            spike = ((elapsed_seconds - (G4_END_TIME - 200)) / 200) * 8000
            viewers += spike
            
    # [2구간] 쉬는 시간 (4경기 종료 ~ 5경기 시작 전)
    elif G4_END_TIME < elapsed_seconds <= G5_START_TIME:
        peak_g4_viewers = 78000
        break_duration = G5_START_TIME - G4_END_TIME
        current_break_time = elapsed_seconds - G4_END_TIME
        
        # 쉬는 동안 2.8만명 하락
        viewers = peak_g4_viewers - ((current_break_time / break_duration) * 28000)
        
    # [3구간] 5경기 진행 중
    elif G5_START_TIME < elapsed_seconds <= G5_END_TIME:
        g5_elapsed = elapsed_seconds - G5_START_TIME
        
        base_g5 = 50000 
        progress = g5_elapsed / g5_duration
        viewers = base_g5 + (progress * 40000) 
        
        # 5경기 끝날 때쯤 (종료 300초 전부터 역대급 스파이크)
        if g5_elapsed > (g5_duration - 300):
            spike = ((g5_elapsed - (g5_duration - 300)) / 300) * 15000 
            viewers += spike
            
    # [4구간] 5경기 종료 후 세리머니
    else:
        ceremony_time = elapsed_seconds - G5_END_TIME
        viewers = 105000 - (ceremony_time * 10) 
        
    return int(viewers + random.randint(-300, 300))

# ---------------------------------------------------------
# 3. 봇 실행 로직
# ---------------------------------------------------------
def run_viewer_producer():
    logger.info("👀 [시청자 봇] 다이내믹 시청자 수 시뮬레이션 시작!")
    start_time = time.time()
    
    # 전체 방송 종료 시간: 5세트 종료 시간 + 우승 세리머니 10분(600초)
    total_broadcast_time = G5_END_TIME + 600 

    try:
        while True:
            # 1. 경과 시간 계산
            elapsed_seconds = int(time.time() - start_time)
            
            # 2. ✨ 현재 상태 알아내기! (이게 진짜 동적인 아키텍처지 😎)
            # ✨ 수정포인트: 함수 이름 맞추고, metadata 인자 추가!
            current_match_id = get_current_match_id(elapsed_seconds, timeline, metadata)
            
            # 3. 시청자 수 계산
            current_viewers = get_real_viewer_count_dynamic(elapsed_seconds)
            
            # 4. ✨ OpenSearch로 보낼 JSON 데이터 포맷 업데이트
            viewer_data = {
                "match_id": current_match_id,          # 이제 pre_game, worlds_2024_..._g4, intermission 이 예쁘게 들어감!
                "timestamp": int(time.time() * 1000), 
                "viewer_count": current_viewers,
                "elapsed_seconds": elapsed_seconds
            }
            
            logger.info(f"📊 [{current_match_id} | {elapsed_seconds}초 경과] 현재 시청자 수: {current_viewers:,}명")
            
            # 5. Kinesis로 데이터 전송
            kinesis_client.put_record(
                StreamName=STREAM_NAME, 
                Data=json.dumps(viewer_data).encode('utf-8'),
                PartitionKey="viewer"
            )
            
            # 6. 방송 종료 조건: 세리머니 시간까지 다 지나면 봇 종료!
            if elapsed_seconds >= total_broadcast_time:
                logger.info("🎉 [시청자 봇] 모든 방송(세리머니 포함)이 종료되었습니다. 수고하셨습니다!")
                break
                
            # 7. 10초 대기 (시청자 수는 1초마다 보낼 필요 없이 폴링 방식으로 10초에 한 번씩!)
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("🛑 [시청자 봇] 관리자에 의해 강제 종료되었습니다!")

if __name__ == "__main__":
    run_viewer_producer()