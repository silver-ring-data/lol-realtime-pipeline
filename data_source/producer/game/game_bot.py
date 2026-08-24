"""인게임 지표 스트림 프로듀서.

사전에 수집한 경기 타임라인을 세트 단위로 재생하여 Kinesis Data Streams로
전송한다. 실행할 세트는 Airflow가 인자로 전달하며, 인자가 없으면
config.yaml의 active_set을 사용한다.

넥서스 파괴 이벤트를 감지하면 해당 프레임까지 전송한 뒤 S3에 완료 시그널
파일을 남기고 종료한다. 이 시그널을 수집 릴레이 DAG의 S3KeySensor가 감지해
분석 DAG를 트리거한다.
"""

import sys
import json
import time
import boto3
import logging
import yaml
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GameProducer")

CONFIG_PATH = os.environ.get('CONFIG_PATH', '/opt/airflow/config/config.yaml')

def run_game_producer():
    logger.info("게임 프로듀서를 시작합니다.")
    
    # 1. 설정 파일 읽기
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 2. Airflow가 전달한 기준 시각(ts)과 세트 키를 인자로 받는다.
    #    실행 예시: python game_bot.py "2026-05-04T09:00:00+00:00" "g5"
    if len(sys.argv) > 1:
        airflow_ts = sys.argv[1]
        base_time = datetime.fromisoformat(airflow_ts).timestamp()
        logger.info(f"[동기화] Airflow 기준 시각: {airflow_ts}")
    else:
        base_time = time.time()
        logger.warning("Airflow 인자가 없어 현재 시각을 기준 시각으로 사용합니다.")

    # 3. 실행할 세트 결정 (Airflow 인자가 config 값보다 우선한다)
    ctx = config['app_context']
    match_key = ctx['active_match_key']
    
    if len(sys.argv) > 2:
        current_set = sys.argv[2].lower()
    else:
        current_set = ctx['active_set']  # 인자가 없으면 config.yaml 값을 사용

    # 4. 경기 메타데이터 및 경로 추출
    match_info = config['matches'][match_key]
    meta = match_info['metadata']
    data_paths = match_info['data']
    
    current_set_info = data_paths['game_sets'][current_set]
    game_file_name = current_set_info['file_name']
    wait_seconds = current_set_info['wait_seconds']

    # S3 경로 및 파티션 키로 사용할 고유 match_id 생성
    match_id = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"
    data_file = os.path.join(data_paths['local_path'], game_file_name)

    # AWS 클라이언트 세팅
    REGION = config['global']['region']
    STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['game_brz']
    BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']
    
    kinesis_client = boto3.client('kinesis', region_name=REGION)
    s3_client = boto3.client('s3', region_name=REGION)

    logger.info("="*50)
    logger.info(f"대상 match_id: {match_id}")
    logger.info(f"데이터 파일: {data_file}")
    logger.info(f"시작까지 대기 시간: {wait_seconds}초")
    logger.info("="*50)

    # 5. 시작 시각까지 대기
    #    기준 시각에 세트별 오프셋을 더해 다른 프로듀서와 타임라인을 맞춘다.
    target_start_time = base_time + wait_seconds
    
    while True:
        current_time = time.time()
        remaining_wait = target_start_time - current_time
        
        if remaining_wait <= 0:
            logger.info(f"[{match_id}] 시작 시각에 도달하여 송출을 시작합니다.")
            break
        
        logger.info(f"[{match_id}] 시작까지 {int(remaining_wait)}초 남았습니다.")
        time.sleep(min(5, remaining_wait))  # 5초 간격으로 진행 상황 출력

    # 6. 인게임 데이터 전송
    logger.info(f"[{match_id}] 데이터 송출을 시작합니다.")
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            game_data = json.load(f)
        
        total_frames = len(game_data)
        
        for idx, frame in enumerate(game_data):
            frame['match_id'] = match_id
            
            # 실시간 스트림처럼 보이도록 프레임 시각을 현재 시각으로 덮어쓴다.
            current_now = int(time.time() * 1000)
            frame['timestamp'] = current_now
            frame['ingame_timestamp_ms'] = current_now
            
            # 하위 이벤트 시각도 맞추고 victim_id 타입을 문자열로 통일한다.
            # (int와 str이 섞이면 Glue 스키마 추론 시 파티션별 타입 충돌이 발생한다)
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
            event_msg = f" | 이벤트 {event_count}건" if event_count > 0 else ""
            
            logger.info(f"[{match_id}] {idx+1}/{total_frames} | {timestamp//1000}초 | blue={blue_gold} red={red_gold}{event_msg}")

            # 넥서스 파괴 여부 판정. 마지막 프레임까지 전송한 뒤에 종료해야 하므로
            # 위의 put_record가 끝난 다음에 확인한다.
            is_game_over = any(
                event.get('victim_id') == "NEXUS DESTROYED"
                for event in frame.get('events', [])
            )

            if is_game_over:
                logger.info(f"[{match_id}] 넥서스 파괴를 감지했습니다. 완료 시그널을 기록합니다.")
                file_key = f"status/{match_id}_finished.txt"
                try:
                    s3_client.put_object(
                        Bucket=BUCKET_NAME,
                        Key=file_key,
                        Body=f"Game Over! Match ID: {match_id}".encode('utf-8')
                    )
                    time.sleep(1)  # S3 결과 정합성 반영 대기
                    logger.info(f"[{match_id}] 완료 시그널 업로드 성공: {file_key}")
                    return
                except Exception as e:
                    logger.error(f"완료 시그널 업로드 실패: {e}")
                    return

            time.sleep(1)  # 프레임 간 1초 간격 유지
            
    except Exception as e:
        logger.error(f"[{match_id}] 데이터 읽기 또는 전송에 실패했습니다: {e}")

if __name__ == "__main__":
    run_game_producer()