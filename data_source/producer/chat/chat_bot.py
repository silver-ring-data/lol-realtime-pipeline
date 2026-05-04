import sys
import json
import time
import boto3
import logging
import yaml
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] 채팅봇: %(message)s')
logger = logging.getLogger("ChatProducer")

CONFIG_PATH = "/opt/airflow/config/config.yaml"

# 1. 설정 파일 읽기
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 2. Airflow가 넘겨주는 인자(sys.argv) 확인 (로그용)
if len(sys.argv) > 1:
    airflow_ts = sys.argv[1]
    base_time = datetime.fromisoformat(airflow_ts).timestamp()
    logger.info(f"⏰ [동기화] Airflow 기준 시각(Base Time): {airflow_ts}")
else:
    base_time = time.time()
    logger.warning("⚠️ Airflow 신호가 없어 현재 시간을 기준 시각으로 사용합니다.")

# 3. 환경 변수 세팅
REGION = config['global']['region']
STREAM_NAME = config['resources']['ingestion_streaming']['kinesis_data_streams']['chat_brz']

ctx = config['app_context']
match_key = ctx['active_match_key']
current_set = ctx['active_set']

match_info = config['matches'][match_key]
meta = match_info['metadata']
data_paths = match_info['data']

# 🔥 매치 ID 생성
#MATCH_ID = f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{current_set}"
DATA_FILE = os.path.join(data_paths['local_path'], data_paths['chat_source'])

kinesis_client = boto3.client('kinesis', region_name=REGION)

def run_chat_producer():
    logger.info("="*50)
    logger.info(f"💬 채팅봇 세팅 완료! 대기 없이 즉시 송출을 시작합니다.")
    logger.info("="*50)

    # 데이터 로드
    chat_data_list = []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chat_data_list.append(json.loads(line))
    except FileNotFoundError:
        logger.error(f"🚨 {DATA_FILE} 파일을 찾을 수 없습니다.")
        return

    if not chat_data_list:
        return

    logger.info(f"✅ 총 {len(chat_data_list)}개의 채팅 발사! 🚀")

    # 🌟 은비의 천재적인 간격 유지 로직 (수정 없이 그대로!)
    first_original_ts = chat_data_list[0]['timestamp']
    prev_original_ts = first_original_ts

    for idx, chat in enumerate(chat_data_list):
        curr_original_ts = chat['timestamp']
        
        wait_time = (curr_original_ts - prev_original_ts) / 1000.0
        if wait_time > 0:
            time.sleep(wait_time)
        
        try:
            #chat['match_id'] = MATCH_ID
            chat['timestamp'] = int(time.time() * 1000)  # 현재 시간으로 덮어쓰기!
            
            chat_str = json.dumps(chat, ensure_ascii=False) + "\n"  
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=chat_str.encode('utf-8'),
                # 💡 파티션 키 변경: 기존 MATCH_ID 대신 '닉네임'이나 '타임스탬프'를 쓰면 
                # Kinesis 샤드(Shard)들에 데이터가 골고루 예쁘게 분산돼!
                PartitionKey=chat.get('nickname', str(chat['timestamp'])) 
            )
            prev_original_ts = curr_original_ts
            
            if (idx + 1) % 50 == 0:
                nickname = chat.get('nickname', '익명')
                content = chat.get('content', '')
                logger.info(f"📊 {idx+1}/{len(chat_data_list)} 전송 중 | 💬 {nickname}: {content[:15]}...")
            
        except Exception as e:
            logger.error(f"🚨 전송 실패: {e}")
            
        prev_original_ts = curr_original_ts

    logger.info(f"✅  채팅 송출 완료!")

if __name__ == "__main__":
    run_chat_producer()