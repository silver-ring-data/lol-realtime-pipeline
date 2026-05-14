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
# CONFIG_PATH = r'C:\Users\Dell3571\Documents\lol-realtime-pipeline\config\config.yaml'

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
timeline = match_info['timeline']

# 🔥 매치 ID 생성

DATA_FILE = os.path.join(data_paths['local_path'], data_paths['chat_source'])

kinesis_client = boto3.client('kinesis', region_name=REGION)
# ✨ 추가: viewer_bot에서 쓰던 다이내믹 match_id 생성 함수 도입!
def get_current_match_id(elapsed_seconds, timeline_config, metadata):
    # 1. 지금이 타임라인 상 어디인지 확인
    current_stage = "post_game" # 기본값
    
    for stage_key, info in timeline_config.items():
        start = info['start_seconds']
        end = start + info['duration']
        if start <= elapsed_seconds < end:
            current_stage = stage_key # 'pre_game', 'g4', 'intermission', 'g5' 중 하나
            break

    # 2. 대기 시간이나 쉬는 시간이면 단어만 반환
    if current_stage in ['pre_game', 'intermission', 'post_game']:
        return current_stage
    
    # 3. 경기 중일 때만 원래 포맷으로 조립 (예: worlds_2024_20241102_t1_blg_g4)
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{current_stage}"
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

    for idx, chat in enumerate(chat_data_list):
        curr_original_ts = chat['timestamp']

        # 1. 첫 채팅으로부터 이 채팅이 몇 초 뒤에 나와야 하는지 계산 (오프셋)
        offset_seconds = (curr_original_ts - first_original_ts) / 1000.0

        # 2. 🌟 핵심: Airflow 기준 시간 + 오프셋 = 이 채팅이 발사되어야 할 "완벽한 절대 시각"
        target_fire_time = base_time + offset_seconds

        # 3. 아직 그 시간이 안 됐다면, 남은 시간만큼만 정확히 대기!
        now = time.time()
        if target_fire_time > now:
            time.sleep(target_fire_time - now)

        try:
            # ✨ 수정포인트: 현재 경과 시간 계산 후 다이내믹하게 match_id 받아오기
            elapsed_seconds = int(time.time() - base_time)
            current_match_id = get_current_match_id(elapsed_seconds, timeline, meta)

            chat['match_id'] = current_match_id

            # (이하 은비가 짠 Kinesis 전송 로직 동일하게 유지!)
            chat['timestamp'] = int(time.time() * 1000)  # 현재 시간으로 덮어쓰기!

            chat_str = json.dumps(chat, ensure_ascii=False) + "\n"

            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=chat_str.encode('utf-8'),
                PartitionKey=chat.get('nickname', str(chat['timestamp']))
            )

            if (idx + 1) % 50 == 0:
                nickname = chat.get('nickname', '익명')
                content = chat.get('content', '')
                logger.info(f"📊 {idx+1}/{len(chat_data_list)} 전송 중 | [{current_match_id}] 💬 {nickname}: {content[:15]}...")

        except Exception as e:
            logger.error(f"🚨 전송 실패: {e}")

    logger.info(f"✅  채팅 송출 완료!")

if __name__ == "__main__":
    run_chat_producer()