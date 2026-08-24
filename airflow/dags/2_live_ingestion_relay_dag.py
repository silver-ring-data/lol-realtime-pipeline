"""실시간 수집 릴레이 DAG.

경기 세트를 순차적으로 재생하면서 스트리밍 데이터를 발생시킨다.

- 채팅 봇과 시청자 봇은 시리즈 전체 구간에서 백그라운드로 상주한다.
- 게임 봇은 세트별로 기동하며, 넥서스 파괴 시 S3에 완료 시그널을 남긴다.
- S3KeySensor가 시그널을 감지하면 해당 세트의 분석 DAG를 트리거하고
  다음 세트로 넘어간다. 분석 완료를 기다리지 않으므로 수집과 분석이
  중첩되어 진행된다.
"""

import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ---------------------------------------------------------
# Config 로딩 및 환경 변수 설정
# ---------------------------------------------------------
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
VIEWER_BOT_DIR = "/opt/airflow/data_source/producer/viewer"

default_args = {'owner': 'data-engineering', 'start_date': datetime(2026, 5, 4), 'retries': 1}
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

    # 채팅 봇: 시리즈 전 구간 동안 백그라운드 상주
    run_series_chat_bot = BashOperator(
        task_id='run_all_series_chat_bot',
        bash_command=f"cd {CHAT_BOT_DIR} && nohup python3 chat_bot.py '{{{{ ts }}}}' > /dev/null 2>&1 &"
    )

    # 시청자 봇: 채팅 봇과 동시에 기동
    run_series_viewer_bot = BashOperator(
        task_id='run_all_series_viewer_bot',
        bash_command=(
            f"cd {VIEWER_BOT_DIR} && "
            f"setsid python3 viewer_bot.py '{{{{ ts }}}}' > viewer_bot.log 2>&1 < /dev/null &"
        )
    )
    start_relay >> [run_series_chat_bot, run_series_viewer_bot]

    previous_game_end_task = start_relay
    set_keys = list(game_data['game_sets'].keys())

    for idx, (set_key, set_info) in enumerate(game_data['game_sets'].items()):
        match_id = generate_match_id(set_key)
        wait_seconds = set_info.get('wait_seconds_before_start', set_info.get('wait_seconds', 0))
        is_last_set = (idx == len(set_keys) - 1)  # 시리즈 종합 리포트 분기 조건

        wait_before_set = BashOperator(task_id=f'wait_before_{set_key}', bash_command=f"sleep {wait_seconds}")
        
        run_game_bot = BashOperator(
            task_id=f'run_game_bot_{set_key}',
            bash_command=f"cd {GAME_BOT_DIR} && nohup python3 game_bot.py '{{{{ ts }}}}' '{set_key}' > /dev/null 2>&1 &"
        )

        wait_for_nexus = S3KeySensor(
            task_id=f'wait_for_nexus_{set_key}', bucket_name=BUCKET_NAME,
            bucket_key=f'status/{match_id}_finished.txt', poke_interval=30, timeout=3600
        )

        # 세트 종료 시그널을 감지하면 분석 DAG로 작업을 넘기고
        # 본 DAG는 다음 세트 수집을 계속 진행한다.
        trigger_analysis = TriggerDagRunOperator(
            task_id=f'trigger_analysis_{set_key}',
            trigger_dag_id='3_batch_analysis_dag',
            conf={
                'match_id': match_id,
                'set_key': set_key,
                'is_last_set': is_last_set
            },
            # 세트별 DAG Run ID가 충돌하지 않도록 match_id와 타임스탬프를 조합한다.
            trigger_run_id=f"analysis_run_{match_id}_{{{{ ts_nodash }}}}",
            wait_for_completion=False,  # 분석 완료를 대기하지 않고 다음 세트로 진행
        )

        # 세트 단위 태스크 의존성 정의
        previous_game_end_task >> wait_before_set >> run_game_bot >> wait_for_nexus >> trigger_analysis
        previous_game_end_task = wait_for_nexus