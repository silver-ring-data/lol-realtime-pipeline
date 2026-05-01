import json
import time
import boto3
import logging
import yaml
import os

# 📋 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 게임봇: %(message)s')
logger = logging.getLogger("GameProducer")

# 1. 설정 파일 읽기
with open('../../../config/config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 2. 현재 실행 컨텍스트 추출
ctx = config['app_context']
match_key = ctx['active_match_key']  # "worlds_2024_final"
current_set = ctx['active_set']      # "g4" 또는 "g5"

# 3. 경기 메타데이터 가져오기
match_info = config['matches'][match_key]
meta = match_info['metadata']
data_paths = match_info['data']

# 🔥 4. 핵심! 현재 선택된 세트(g4/g5)의 파일명과 대기시간을 쏙 뽑아오기
current_set_info = data_paths['game_sets'][current_set]
game_file_name = current_set_info['file_name']
wait_seconds = current_set_info['wait_seconds'] # 55 또는 3182가 자동으로 들어감!

# 5. S3 폴더명 및 파티션 키로 쓸 MATCH_ID 생성
MATCH_ID = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"

# 📂 게임 데이터 파일 절대/상대 경로 조합
DATA_FILE = os.path.join(data_paths['local_path'], game_file_name)

# 🚀 세팅 완료 로그 출력 (실행 시 터미널에서 꼭 확인해!)
REGION = config['global']['region']
STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['game_brz']
BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']

# AWS 클라이언트 세팅
kinesis_client = boto3.client('kinesis', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION) # 🛠️ S3 클라이언트 추가!


def run_game_producer():
    logger.info("🎬 실시간 LoL 방송 중계를 시작합니다! (테스트 모드: 마지막 3개 프레임만 전송)")
    
    # 1. YAML 설정 파일 읽어오기
    with open('../../../config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 2. 현재 실행할 세트 및 메타데이터 추출
    ctx = config['app_context']
    match_key = ctx['active_match_key']
    current_set = ctx['active_set']

    match_info = config['matches'][match_key]
    meta = match_info['metadata']
    data_paths = match_info['data']
    
    current_set_info = data_paths['game_sets'][current_set]
    game_file_name = current_set_info['file_name']
    
    wait_seconds = current_set_info['wait_seconds'] 

    # S3 폴더명 및 파티션 키로 쓸 절대 안 겹치는 MATCH_ID 생성
    match_id = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"
    data_file = os.path.join(data_paths['local_path'], game_file_name)

    logger.info("="*50)
    logger.info(f"🔥 타겟 MATCH_ID: {match_id}")
    logger.info(f"📂 읽어올 데이터: {data_file}")
    logger.info(f"⏳ 시작 전 대기 시간: {wait_seconds}초")
    logger.info("="*50)

    program_start_time = time.time()
    target_start_time = program_start_time + wait_seconds
    
    while True:
        remaining_wait = target_start_time - time.time()
        if remaining_wait <= 0:
            break
        
        logger.info(f"⏳ [{match_id}] 시작까지 {int(remaining_wait)}초 남았습니다...")
        time.sleep(min(5, remaining_wait))

    # 4. 인게임 데이터 전송
    logger.info(f"🎮 {match_id} 데이터 송출 시작!!!")
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            game_data = json.load(f)
        
        # 🔥 여기서 마지막 3개의 데이터만 딱 자르기!
        #game_data = game_data[-3:]
        total_frames = len(game_data)
        
        for idx, frame in enumerate(game_data):
            frame['match_id'] = match_id
            frame['ingame_timestamp_ms'] = int(time.time() * 1000) 
            
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
            
            # 테스트 모드니까 매 프레임마다 출력하게 고침
            logger.info(f"📊 [{match_id}] {idx+1}/{total_frames} | 시간: {timestamp//1000}초 | 🔵 {blue_gold} vs 🔴 {red_gold}{event_msg}")


            # 2️⃣ 방금 쏜 프레임 안에 '넥서스 파괴'가 있었는지 검사!
            for event in frame.get('events', []):
                if event.get('victim_id') == "NEXUS DESTROYED":
                    logger.info(f"\n🔥 [넥서스 파괴 감지!] S3에 종료 깃발을 꽂습니다...")
                    
                    file_key = f"status/{match_id}_finished.txt"
                    try:
                        # 3️⃣ S3에 상태 파일(finished.txt) 업로드
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=file_key,
                            Body=f"Game Over! Match ID: {match_id}".encode('utf-8')
                        )
                        logger.info(f"🎉 [검증 완료] S3 버킷에 {file_key} 파일 생성 성공! 람다야 일어나라 🚀")
                        
                        # 넥서스 터졌으니 더 쏠 데이터도 없다! 봇 종료!
                        return 
                        
                    except Exception as e:
                        logger.error(f"❌ S3 깃발 꽂기 실패: {e}")
                    
                    try:
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=file_key,
                            Body=f"Game Over! Match ID: {match_id}".encode('utf-8')
                        )
                        
                        # 1초 대기 후 S3에 진짜 파일이 있는지 확인!
                        time.sleep(1)
                        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=file_key)
                        
                        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                            logger.info(f"🎉 [검증 성공] S3 버킷({BUCKET_NAME})에서 {file_key} 파일을 완벽하게 확인했습니다! (람다 기상 준비 끝 🚀)")
                            
                    except Exception as e:
                        logger.error(f"❌ S3 깃발 꽂기 또는 검증 실패: {e}")

                        
            time.sleep(1) # 1초 간격 전송
            
    except Exception as e:
        logger.error(f"🚨 {match_id} 데이터 읽기/전송 실패: {e}")


    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=f"Game Over! Match ID: {match_id}".encode('utf-8')
        )
        
        # 1초 대기 후 S3에 진짜 파일이 있는지 확인!
        time.sleep(1)
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=file_key)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"🎉 [검증 성공] S3 버킷({BUCKET_NAME})에서 {file_key} 파일을 완벽하게 확인했습니다! (람다 기상 준비 끝 🚀)")
            
    except Exception as e:
        logger.error(f"❌ S3 깃발 꽂기 또는 검증 실패: {e}")


if __name__ == "__main__":
    run_game_producer()