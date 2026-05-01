import json
import time
import boto3
import logging
import yaml
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 채팅봇: %(message)s')
logger = logging.getLogger("ChatProducer")

# 1. 설정 파일 읽기
with open('../../../config/config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 2. 현재 실행 컨텍스트 및 메타데이터 추출 (v1.5 구조 반영!)
REGION = config['global']['region']
STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['chat_brz']

ctx = config['app_context']
match_key = ctx['active_match_key']
current_set = ctx['active_set']

match_info = config['matches'][match_key]
meta = match_info['metadata']
data_paths = match_info['data']

# 🔥 동기화를 위한 대기 시간과 매치 ID 생성
wait_seconds = data_paths['game_sets'][current_set]['wait_seconds']
MATCH_ID = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"

# 📂 채팅 데이터 파일 경로 조합
DATA_FILE = os.path.join(data_paths['local_path'], data_paths['chat_source'])

# AWS Kinesis 클라이언트 생성
kinesis_client = boto3.client('kinesis', region_name=REGION)

def run_chat_producer():
    logger.info("="*50)
    logger.info(f"💬 채팅봇 세팅 완료!")
    logger.info(f"🔥 타겟 MATCH_ID: {MATCH_ID}")
    logger.info(f"📂 읽어올 데이터: {DATA_FILE}")
    logger.info(f"⏳ 시작 전 대기 시간: {wait_seconds}초")
    logger.info("="*50)

    # 1. 채팅 데이터 메모리에 로드
    chat_data_list = []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chat_data_list.append(json.loads(line))
    except FileNotFoundError:
        logger.error(f"🚨 앗! {DATA_FILE} 파일을 찾을 수 없어.")
        return

    if not chat_data_list:
        logger.warning("채팅 데이터가 없습니다.")
        return

    logger.info(f"✅ 총 {len(chat_data_list)}개의 채팅 발사 준비 완료! 🚀")
    

    logger.info(f"💬 {MATCH_ID} 채팅 데이터 송출 시작!!!")

    # 3. 데이터 전송 (은비의 천재적인 시간차 로직 유지!)
    prev_original_ts = chat_data_list[0]['timestamp']

    for idx, chat in enumerate(chat_data_list):
        curr_original_ts = chat['timestamp']
        
        # 이전 채팅과의 시간 차이만큼 대기
        wait_time = (curr_original_ts - prev_original_ts) / 1000.0
        if wait_time > 0:
            time.sleep(wait_time)
        
        try:
            # 💡 Flink 연동을 위한 필수 데이터 추가 (match_id, 실시간 타임스탬프)
            chat['match_id'] = MATCH_ID
            chat['timestamp_ms'] = int(time.time() * 1000)
            
            chat_str = json.dumps(chat, ensure_ascii=False) + "\n"  
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=chat_str.encode('utf-8'),
                PartitionKey=MATCH_ID # 파티션 키를 match_id로 통일해서 Kinesis 분산 최적화!
            )
            
            # 모니터링 로그 (너무 많으면 느려지니 50개마다 한 번씩만 출력)
            if (idx + 1) % 50 == 0:
                nickname = chat.get('nickname', '익명')
                content = chat.get('content', '')
                logger.info(f"📊 [{MATCH_ID}] {idx+1}/{len(chat_data_list)} 전송 중 | 💬 {nickname}: {content[:15]}...")
            
        except Exception as e:
            logger.error(f"🚨 전송 실패: {e}")
            
        prev_original_ts = curr_original_ts

    logger.info(f"✅ {MATCH_ID} 채팅 송출이 모두 끝났습니다!")

if __name__ == "__main__":
    run_chat_producer()