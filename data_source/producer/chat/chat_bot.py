"""채팅 스트림 프로듀서.

수집된 채팅 로그를 원본의 발생 간격 그대로 Kinesis Data Streams에 재생한다.

Airflow가 전달한 논리적 실행 시각(ts)을 기준 시각으로 삼고, 각 채팅의 원본
타임스탬프 오프셋을 더해 절대 발사 시각을 계산한다. 매 건마다 절대 시각을
다시 계산하므로 지연이 누적되지 않으며, 게임 봇/시청자 봇과 동일한 타임라인
위에서 동작한다.
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
logger = logging.getLogger("ChatProducer")

CONFIG_PATH = os.environ.get('CONFIG_PATH', '/opt/airflow/config/config.yaml')

# 1. 설정 파일 읽기
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# 2. Airflow가 전달한 기준 시각(ts) 확인
if len(sys.argv) > 1:
    airflow_ts = sys.argv[1]
    base_time = datetime.fromisoformat(airflow_ts).timestamp()
    logger.info(f"[동기화] Airflow 기준 시각: {airflow_ts}")
else:
    base_time = time.time()
    logger.warning("Airflow 인자가 없어 현재 시각을 기준 시각으로 사용합니다.")

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

DATA_FILE = os.path.join(data_paths['local_path'], data_paths['chat_source'])

kinesis_client = boto3.client('kinesis', region_name=REGION)


def get_current_match_id(elapsed_seconds, timeline_config, metadata):
    """경과 시간이 속한 구간에 해당하는 match_id를 반환한다.

    경기 외 구간(pre_game, intermission, post_game)은 구간명을 그대로 반환하고,
    경기 진행 중에는 파티션 키로 사용할 전체 match_id를 조립한다.
    """
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


def run_chat_producer():
    logger.info("="*50)
    logger.info("채팅 프로듀서 초기화 완료. 송출을 시작합니다.")
    logger.info("="*50)

    # 데이터 로드
    chat_data_list = []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chat_data_list.append(json.loads(line))
    except FileNotFoundError:
        logger.error(f"데이터 파일을 찾을 수 없습니다: {DATA_FILE}")
        return

    if not chat_data_list:
        return

    logger.info(f"총 {len(chat_data_list)}건의 채팅을 송출합니다.")

    # 원본 채팅 간격을 유지하기 위한 기준점
    first_original_ts = chat_data_list[0]['timestamp']

    for idx, chat in enumerate(chat_data_list):
        curr_original_ts = chat['timestamp']

        # 1. 첫 채팅으로부터 이 채팅이 몇 초 뒤에 나와야 하는지 계산 (오프셋)
        offset_seconds = (curr_original_ts - first_original_ts) / 1000.0

        # 2. 기준 시각 + 오프셋으로 절대 발사 시각을 구한다.
        target_fire_time = base_time + offset_seconds

        # 3. 발사 시각까지 남은 시간만큼만 대기한다.
        now = time.time()
        if target_fire_time > now:
            time.sleep(target_fire_time - now)

        try:
            # 경과 시간에 따라 현재 구간의 match_id를 결정한다.
            elapsed_seconds = int(time.time() - base_time)
            current_match_id = get_current_match_id(elapsed_seconds, timeline, meta)

            chat['match_id'] = current_match_id

            # 다운스트림이 실시간 스트림으로 인식하도록 발생 시각을 현재 시각으로 덮어쓴다.
            chat['timestamp'] = int(time.time() * 1000)

            chat_str = json.dumps(chat, ensure_ascii=False) + "\n"

            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=chat_str.encode('utf-8'),
                PartitionKey=chat.get('nickname', str(chat['timestamp']))
            )

            if (idx + 1) % 50 == 0:
                nickname = chat.get('nickname', 'anonymous')
                content = chat.get('content', '')
                logger.info(f"{idx+1}/{len(chat_data_list)} 전송 | [{current_match_id}] {nickname}: {content[:15]}...")

        except Exception as e:
            logger.error(f"채팅 전송 실패: {e}")

    logger.info("채팅 송출을 완료했습니다.")

if __name__ == "__main__":
    run_chat_producer()