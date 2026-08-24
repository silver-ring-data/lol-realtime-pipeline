"""배치 파이프라인 초기화 DAG.

경기 시리즈 수집을 시작하기 전에 실행 환경을 준비한다.

1. 이전 실행에서 남은 S3 완료 시그널 파일을 삭제한다.
2. Athena 외부 테이블(Silver/Gold)을 IF NOT EXISTS로 생성한다.
3. 수집 릴레이 DAG(2_live_ingestion_relay_dag)를 트리거한다.

대상 경기와 리소스 이름은 config/config.yaml에서 주입받는다.
"""

import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
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
GLUE_ETL_JOB = config['resources']['processing']['glue_job_refiner']
ATHENA_DB = config['resources']['processing']['athena_db']
TABLES = config['resources']['processing']['tables']

GAME_BOT_DIR = "/opt/airflow/data_source/producer/game"
CHAT_BOT_DIR = "/opt/airflow/data_source/producer/chat"

game_set_keys = list(game_data['game_sets'].keys())
# 세트별로 고유한 Match ID를 생성한다 (파티션 키 및 시그널 파일명으로 사용).
def generate_match_id(set_key):
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{set_key}"
target_match_ids = [generate_match_id(set_key) for set_key in game_data['game_sets'].keys()]

default_args = {'owner': 'data-engineering', 'start_date': datetime(2026, 5, 4), 'retries': 1}

with DAG(
    dag_id='1_live_infra_setup_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'infra']
) as dag:

    start_task = EmptyOperator(task_id='start_setup')

    # 1. 이전 실행에서 남은 완료 시그널 제거
    clean_all_signals = S3DeleteObjectsOperator(
        task_id='clean_all_signals',
        bucket=BUCKET_NAME,
        # status/ 경로의 시그널 파일만 정확히 지정하여 삭제한다.
        keys=[f"status/{m_id}_finished.txt" for m_id in target_match_ids],
        aws_conn_id='aws_default'
    )

    # 2. Athena 테이블 생성 (DDL)
    create_slv_game = AthenaOperator(
        task_id='create_slv_game_table', 
        query='sql/ddl_batch_tables/create_slv_game.sql',
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/ddl/",
        aws_conn_id='aws_default',
        params={'DB': ATHENA_DB, 
                'TABLE_NAME': TABLES['btch_slv_game'], 
                'BUCKET': BUCKET_NAME
                }
    )
    create_slv_chat = AthenaOperator(
        task_id='create_slv_chat_table', 
        query='sql/ddl_batch_tables/create_slv_chat.sql',
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/ddl/",
        aws_conn_id='aws_default',
        params={'DB': ATHENA_DB, 
                'TABLE_NAME': TABLES['btch_slv_chat'], 
                'BUCKET': BUCKET_NAME}
    )
    create_gld_report = AthenaOperator(
        task_id='create_gld_report_table', 
        query='sql/ddl_batch_tables/create_gld_report.sql',
        output_location=f"s3://{BUCKET_NAME}/athena-results/ddl/",
        aws_conn_id='aws_default',
        database=ATHENA_DB, 
        params={'DB': ATHENA_DB, 
                'TABLE_NAME': TABLES['btch_gld_report'], 
                'BUCKET': BUCKET_NAME}
    )

    setup_done = EmptyOperator(task_id='setup_done')

    # 3. 수집 릴레이 DAG 트리거
    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_2_live_ingestion',
        trigger_dag_id='2_live_ingestion_relay_dag',
        wait_for_completion=False
    )

    # 태스크 의존성 정의
    start_task >> clean_all_signals >> [create_slv_game, create_slv_chat, create_gld_report] >> setup_done >> trigger_dag_2