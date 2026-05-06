import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 🌟 핵심: 다른 DAG 호출!

# --- (config.yaml 불러오는 부분 생략) ---
CONFIG_PATH = "/opt/airflow/config/config.yaml"
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']
game_data = config['matches'][match_key]['data']
metadata = config['matches'][match_key]['metadata']

BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']

GAME_BOT_DIR = "/opt/airflow/data_source/producer/game"
CHAT_BOT_DIR = "/opt/airflow/data_source/producer/chat"

default_args = {'owner': 'eunbee', 'start_date': datetime(2026, 5, 4), 'retries': 1}
def generate_match_id(set_key):
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{set_key}"

with DAG(
    dag_id='2_live_ingestion_relay_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ingestion', 'relay']
) as dag:

    start_relay = EmptyOperator(task_id='start_relay')

    # 채팅 봇은 백그라운드에서 계속 실행
    run_series_chat_bot = BashOperator(
        task_id='run_all_series_chat_bot',
        bash_command=f"nohup python3 {CHAT_BOT_DIR}/chat_bot.py '{{{{ ts }}}}' > /dev/null 2>&1 &"
    )
    start_relay >> run_series_chat_bot

    previous_game_end_task = start_relay
    set_keys = list(game_data['game_sets'].keys())

    for idx, (set_key, set_info) in enumerate(game_data['game_sets'].items()):
        match_id = generate_match_id(set_key)
        wait_seconds = set_info.get('wait_seconds_before_start', set_info.get('wait_seconds', 0))
        is_last_set = (idx == len(set_keys) - 1) # 🌟 마지막 세트인지 판별

        wait_before_set = BashOperator(task_id=f'wait_before_{set_key}', bash_command=f"sleep {wait_seconds}")
        
        run_game_bot = BashOperator(
            task_id=f'run_game_bot_{set_key}',
            bash_command=f"nohup python3 {GAME_BOT_DIR}/game_bot.py '{{{{ ts }}}}' '{set_key}' > /dev/null 2>&1 &"
        )

        wait_for_nexus = S3KeySensor(
            task_id=f'wait_for_nexus_{set_key}', bucket_name=BUCKET_NAME,
            bucket_key=f'status/{match_id}_finished.txt', poke_interval=30, timeout=3600
        )

        # 🌟 넥서스가 터지면? 분석용 DAG(3번)한테 바통을 넘기고 자기는 다음 루프로!
        # 🌟 분석 DAG(3_batch_analysis_dag) 호출
        trigger_analysis = TriggerDagRunOperator(
            task_id=f'trigger_analysis_{set_key}',
            trigger_dag_id='3_batch_analysis_dag',  # 호출할 DAG ID
            conf={
                'match_id': match_id,
                'set_key': set_key,
                'is_last_set': is_last_set
            },
            # 💡 중요: 실행 ID가 겹치지 않게 설정
            trigger_run_id=f"analysis_run_{match_id}_{{{{ ts_nodash }}}}",
            wait_for_completion=False # 분석 끝날 때까지 안 기다리고 다음 세트 준비!
        )

        # 의존성 연결
        previous_game_end_task >> wait_before_set >> run_game_bot >> wait_for_nexus >> trigger_analysis
        previous_game_end_task = wait_for_nexus