import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor

from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# ---------------------------------------------------------
# 🛠️ 1. Config 및 환경 변수 설정
# ---------------------------------------------------------
CONFIG_PATH = "/opt/airflow/config/config.yaml"
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']
game_data = config['matches'][match_key]['data']
metadata = config['matches'][match_key]['metadata']

BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']
GLUE_ETL_JOB = config['resources']['processing']['glue_job_refiner']
ATHENA_DB = config['resources']['processing']['athena_db']
TABLES = config['resources']['processing']['tables']

GAME_BOT_DIR = "/opt/airflow/data_source/producer/game"
CHAT_BOT_DIR = "/opt/airflow/data_source/producer/chat"

game_set_keys = list(game_data['game_sets'].keys())

# Match ID 생성 함수 (중복 방지용)
def generate_match_id(set_key):
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{set_key}"

default_args = {
    'owner': 'eunbee',
    'start_date': datetime(2026, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lol_series_analysis_master_v4',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['lol', 'highlight', 'final']
) as dag:

    start_task = EmptyOperator(task_id='start_series_pipeline')

    # ---------------------------------------------------------
    # 🧹 2. Clean Task: S3의 모든 종료 시그널 파일을 미리 삭제
    # ---------------------------------------------------------
    # 루프 돌면서 하나씩 지우는 대신, 시리즈 시작 전에 해당 매치들의 시그널을 미리 정리해!
    target_match_ids = [generate_match_id(set_key) for set_key in game_data['game_sets'].keys()]
    clean_all_signals = BashOperator(
        task_id='clean_all_signals',
        bash_command=f"aws s3 rm s3://{BUCKET_NAME}/signals/ --recursive --exclude '*' " + 
                     " ".join([f"--include '{m_id}_finished.txt'" for m_id in target_match_ids])
    )

    # ---------------------------------------------------------
    # 🏗️ 3. Create Tasks: Athena 테이블들이 있는지 먼저 확인 (DDL)
    # ---------------------------------------------------------
    create_slv_game = AthenaOperator(
        task_id='create_slv_game_table',
        query='sql/ddl_batch_tables/create_slv_game.sql',
        database=ATHENA_DB,
        params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_slv_game'], 'BUCKET': BUCKET_NAME}
    )

    create_slv_chat = AthenaOperator(
        task_id='create_slv_chat_table',
        query='sql/ddl_batch_tables/create_slv_chat.sql',
        database=ATHENA_DB,
        params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_slv_chat'], 'BUCKET': BUCKET_NAME}
    )

    create_gld_report = AthenaOperator(
        task_id='create_gld_report_table',
        query='sql/ddl_batch_tables/create_gld_report.sql',
        database=ATHENA_DB,
        params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_gld_report'], 'BUCKET': BUCKET_NAME}
    )

    ddl_done = EmptyOperator(task_id='all_ddl_and_clean_done')
    start_task >> clean_all_signals >> [create_slv_game, create_slv_chat, create_gld_report] >> ddl_done
    # ---------------------------------------------------------
    # 🧨 3.5 Series Chat Bot: 시리즈 내내 상주하며 채팅 수집 (루프 밖!)
    # ---------------------------------------------------------
    run_series_chat_bot = BashOperator(
        task_id='run_all_series_chat_bot',
        # Airflow 기준 시각인 ts를 넘겨서 동기화하고, nohup으로 백그라운드 실행!
        bash_command=f"nohup python3 {CHAT_BOT_DIR}/chat_bot.py '{{{{ ts }}}}' > /dev/null 2>&1 &"
    )

    # 🔗 DDL이랑 청소가 다 끝나면 채팅 봇 출발!
    ddl_done >> run_series_chat_bot
    # ---------------------------------------------------------
    # 🤖 4. Game Sets Loop: 여기서부터 개별 세트 진행
    # ---------------------------------------------------------
    previous_set_task = ddl_done
    all_set_analysis_tasks = []


    for set_key, set_info in game_data['game_sets'].items():
        match_id = generate_match_id(set_key)
        wait_seconds = set_info['wait_seconds']

        wait_before_set = BashOperator(
            task_id=f'wait_before_{set_key}',
            bash_command=f"sleep {wait_seconds}"
        )

        run_game_bot = BashOperator(
            task_id=f'run_game_bot_{set_key}',
            bash_command=f"nohup python3 {GAME_BOT_DIR}/game_bot.py '{{{{ ts }}}}' '{set_key}' > /dev/null 2>&1 &"
        )

        # D. 넥서스 파괴 신호 감시 (S3 센서)
        wait_for_nexus = S3KeySensor(
            task_id=f'wait_for_nexus_{set_key}',
            bucket_name=BUCKET_NAME,
            bucket_key=f'status/{match_id}_finished.txt',
            poke_interval=30,
            timeout=3600
        )

        # E. 분석 프로세스 (Glue -> Partition Repair -> Athena Analysis)
        glue_transform = GlueJobOperator(
            task_id=f'glue_transform_{set_key}',
            job_name=GLUE_ETL_JOB,
            script_args={'--match_id': match_id}
        )

        repair_game = AthenaOperator(
            task_id=f'repair_game_table_{set_key}',
            query=f"MSCK REPAIR TABLE {ATHENA_DB}.{TABLES['btch_slv_game']};",
            database=ATHENA_DB
        )

        repair_chat = AthenaOperator(
            task_id=f'repair_chat_table_{set_key}',
            query=f"MSCK REPAIR TABLE {ATHENA_DB}.{TABLES['btch_slv_chat']};",
            database=ATHENA_DB
        )

        athena_set_analysis = AthenaOperator(
            task_id=f'analyze_set_{set_key}',
            query='sql/analyze_set_reports.sql',
            database=ATHENA_DB,
            params={
                'DB': ATHENA_DB,
                'GAME_TABLE_NAME': TABLES['btch_slv_game'],
                'CHAT_TABLE_NAME': TABLES['btch_slv_chat'],
                'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
                'match_id': match_id
            }
        )

        # 🔗 의존성 연결 로직 (for문 안쪽)
        
        # 👉 [수정 3] 봇 실행 후 넥서스 센서가 돌도록 >> 연결 추가
        previous_set_task >> wait_before_set >> run_game_bot >> wait_for_nexus
        
        # 개별 분석 트랙: 종료 감지 시 즉시 분석 시작
        wait_for_nexus >> glue_transform >> [repair_game, repair_chat] >> athena_set_analysis
        
        # 👉 [수정 4] 다음 세트(루프)가 '이번 세트의 분석(또는 넥서스 대기)'이 끝나면 돌도록 바통 터치!
        previous_set_task = wait_for_nexus

    # 🥇 4. 시리즈 종합 분석 (전체 경기가 끝나면 즉시 실행)
    # 🌟 포인트: all_set_analysis_tasks를 기다리지 않고, 마지막 wait_for_nexus가 끝나면 바로 실행됨
    target_match_ids_str = ", ".join([f"'{m_id}'" for m_id in target_match_ids])

    series_final_report = AthenaOperator(
        task_id='generate_final_series_report',
        query='sql/generate_final_report.sql',
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/gold/final_reports/{match_key}/",
        params={
            'ATHENA_DB': ATHENA_DB,
            'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
            'target_match_ids_str': target_match_ids_str,
            'match_key': match_key
        }
    )

    send_report_email = EmailOperator(
        task_id='send_final_report_email',
        to='cindypink17@gmail.com', # 은비 이메일
        subject=f'🔥 LoL Series Report: {match_key} 🔥',
        html_content=f"""
        <h3>LoL {match_key} 시리즈 데이터 수집 및 분석이 완료되었습니다.</h3>
        <p>전체 세트의 넥서스 파괴 시그널이 감지되었으며, 종합 리포트 생성이 완료되었습니다.</p>
        <p>S3 경로: s3://{BUCKET_NAME}/gold/final_reports/{match_key}/</p>
        """
    )

    # 🌟 최종 분석은 마지막 세트의 wait_for_nexus가 성공하면 바로 실행되도록 연결!
    all_set_analysis_tasks >> series_final_report >> send_report_email