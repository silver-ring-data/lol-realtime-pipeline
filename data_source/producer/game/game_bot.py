import sys
import json
import time
import boto3
import logging
import yaml
import os
from datetime import datetime

# 📋 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 게임봇: %(message)s')
logger = logging.getLogger("GameProducer")

CONFIG_PATH = "/opt/airflow/config/config.yaml"

def run_game_producer():
    logger.info("🎬 실시간 LoL 방송 중계를 시작합니다! (Airflow 연동 버전)")
    
    # 1. YAML 설정 파일 읽어오기
    # (절대경로나 Airflow 환경에 맞게 경로 확인 필요, 현재 경로 유지)
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 2. 🌟 Airflow가 넘겨주는 인자(sys.argv) 받기
    # 실행 예시: python game_bot.py "2026-05-04T09:00:00+00:00" "g5"
    if len(sys.argv) > 1:
        airflow_ts = sys.argv[1]
        base_time = datetime.fromisoformat(airflow_ts).timestamp()
        logger.info(f"⏰ [동기화] Airflow 기준 시각(Base Time): {airflow_ts}")
    else:
        base_time = time.time()
        logger.warning("⚠️ Airflow 신호가 없어 현재 시간을 기준 시각으로 사용합니다.")

    # 3. 세트(g4/g5) 결정 로직 (Airflow 인자가 config를 이기도록 설정!)
    ctx = config['app_context']
    match_key = ctx['active_match_key']
    
    if len(sys.argv) > 2:
        current_set = sys.argv[2].lower() # Airflow에서 넘겨준 'g4' 또는 'g5'
    else:
        current_set = ctx['active_set']   # 인자가 없으면 config.yaml 값 사용

    # 4. 경기 메타데이터 및 경로 추출
    match_info = config['matches'][match_key]
    meta = match_info['metadata']
    data_paths = match_info['data']
    
    current_set_info = data_paths['game_sets'][current_set]
    game_file_name = current_set_info['file_name']
    wait_seconds = current_set_info['wait_seconds'] # g4: 55, g5: 3182

    # S3 폴더명 및 파티션 키로 쓸 절대 안 겹치는 MATCH_ID 생성
    match_id = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"
    data_file = os.path.join(data_paths['local_path'], game_file_name)

    # AWS 클라이언트 세팅
    REGION = config['global']['region']
    STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['game_brz']
    BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']
    
    kinesis_client = boto3.client('kinesis', region_name=REGION)
    s3_client = boto3.client('s3', region_name=REGION)

    logger.info("="*50)
    logger.info(f"🔥 타겟 MATCH_ID: {match_id}")
    logger.info(f"📂 읽어올 데이터: {data_file}")
    logger.info(f"⏳ 설정된 전체 대기 시간: {wait_seconds}초")
    logger.info("="*50)

    # 🌟 5. 스마트 대기 로직 (핵심!)
    target_start_time = base_time + wait_seconds
    
    while True:
        current_time = time.time()
        remaining_wait = target_start_time - current_time
        
        if remaining_wait <= 0:
            logger.info(f"🚀 [{match_id}] 설정된 시작 시각에 도달했습니다! 즉시 시작합니다.")
            break
        
        logger.info(f"⏳ [{match_id}] 완벽한 싱크를 위해 {int(remaining_wait)}초 더 대기합니다...")
        time.sleep(min(5, remaining_wait)) # 5초마다 로그 출력

    # 6. 인게임 데이터 전송
    logger.info(f"🎮 {match_id} 데이터 송출 시작!!!")
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            game_data = json.load(f)
        
        total_frames = len(game_data)
        
        # [TODO] : 마지막 100개만
        for idx, frame in enumerate(game_data[-100:]):
            frame['match_id'] = match_id
            
            # 🌟 1. 껍데기 시간(프레임 시간) 현재로 덮어쓰기!
            current_now = int(time.time() * 1000)
            frame['timestamp'] = current_now
            frame['ingame_timestamp_ms'] = current_now
            
            # 🌟 2. 알맹이 시간 덮어쓰기 + victim_id 문자열로 강제 변환!
            if 'events' in frame:
                for event in frame['events']:
                    event['timestamp'] = current_now 
                    
                    if 'victim_id' in event and event['victim_id'] is not None:
                        event['victim_id'] = str(event['victim_id'])
            
            frame_str = json.dumps(frame, ensure_ascii=False) + "\n"
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=frame_str.encode('utf-8'),
                PartitionKey=match_id
            )
            
            timestamp = frame.get('timestamp', 0)
            participants = frame.get('participant_frames', {})
            blue_gold = sum(p.get('total_gold', 0) for i, p in participants.items() if int(i) <= 5)
            red_gold = sum(p.get('total_gold', 0) for i, p in participants.items() if int(i) > 5)
            
            event_count = len(frame.get('events', []))
            event_msg = f" | 🔴 이벤트 발생! ({event_count}건)" if event_count > 0 else ""
            
            logger.info(f"📊 [{match_id}] {idx+1}/{total_frames} | 시간: {timestamp//1000}초 | 🔵 {blue_gold} vs 🔴 {red_gold}{event_msg}")

            is_game_over = False
            # 1️⃣ 넥서스 파괴 여부 먼저 확인만 하기 (종료는 아직 안 함)
            for event in frame.get('events', []):
                if event.get('victim_id') == "NEXUS DESTROYED":
                    is_game_over = True
                    logger.info(f"\n🔥 [넥서스 파괴 감지!] 현재 프레임 전송 후 봇을 종료합니다...")

            # 2️⃣ 무조건 Kinesis로 현재 프레임 전송 (마지막 장면 유실 방지!)
            frame['match_id'] = match_id
            frame['timestamp'] = int(current_time * 1000)
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(frame, ensure_ascii=False).encode('utf-8'),
                PartitionKey=match_id
            )
            
            # 3️⃣ 전송 완료 후, 아까 넥서스가 터졌다면 S3에 깃발 꽂고 종료!
            if is_game_over:
                file_key = f"status/{match_id}_finished.txt"
                try:
                    s3_client.put_object(
                        Bucket=BUCKET_NAME,
                        Key=file_key,
                        Body=f"Game Over! Match ID: {match_id}".encode('utf-8')
                    )
                    time.sleep(1) # S3 반영 대기
                    logger.info(f"🎉 [검증 성공] S3 버킷에 {file_key} 깃발 꽂기 성공! 람다야 일어나라 🚀")
                    return # 여기서 안전하게 종료!
                except Exception as e:
                    logger.error(f"🚨 S3 업로드 실패: {e}")
                    return

            time.sleep(1) # 1초 간격 전송
            
    except Exception as e:
        logger.error(f"🚨 {match_id} 데이터 읽기/전송 실패: {e}")

if __name__ == "__main__":
    run_game_producer()