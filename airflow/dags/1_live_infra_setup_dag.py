import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

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
target_match_ids = [generate_match_id(set_key) for set_key in game_data['game_sets'].keys()]

default_args = {'owner': 'eunbee', 'start_date': datetime(2026, 5, 4), 'retries': 1}

with DAG(
    dag_id='1_live_infra_setup_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'infra']
) as dag:

    start_task = EmptyOperator(task_id='start_setup')

    # 1. 찌꺼기 청소
    clean_all_signals = S3DeleteObjectsOperator(
        task_id='clean_all_signals',
        bucket=BUCKET_NAME,
        # 네가 정한 status/ 경로로 정확하게 타겟팅!
        keys=[f"status/{m_id}_finished.txt" for m_id in target_match_ids],
        aws_conn_id='aws_default'
    )

    # 2. Athena 테이블 생성 (DDL)
    create_slv_game = AthenaOperator(
        task_id='create_slv_game_table', query='sql/ddl_batch_tables/create_slv_game.sql',
        database=ATHENA_DB, params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_slv_game'], 'BUCKET': BUCKET_NAME}
    )
    create_slv_chat = AthenaOperator(
        task_id='create_slv_chat_table', query='sql/ddl_batch_tables/create_slv_chat.sql',
        database=ATHENA_DB, params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_slv_chat'], 'BUCKET': BUCKET_NAME}
    )
    create_gld_report = AthenaOperator(
        task_id='create_gld_report_table', query='sql/ddl_batch_tables/create_gld_report.sql',
        database=ATHENA_DB, params={'DB': ATHENA_DB, 'TABLE_NAME': TABLES['btch_gld_report'], 'BUCKET': BUCKET_NAME}
    )

    setup_done = EmptyOperator(task_id='setup_done')

    # 의존성 연결
    start_task >> clean_all_signals >> [create_slv_game, create_slv_chat, create_gld_report] >> setup_done