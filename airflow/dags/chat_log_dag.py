from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import Config
import random

def fetch_and_load_chat_data(**context):
    # XCom으로 가져오되, 실패 시 Config의 기본 MATCH_ID 사용
    match_id = context['ti'].xcom_pull(
        dag_id=f'lol_match_data_{Config.VERSION}', 
        task_ids='fetch_and_load_match_data', 
        key='current_match_id'
    ) or Config.MATCH_ID

    pg_hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)
    # ... (기존 랜덤 채팅 생성 및 INSERT 로직)
    print(f"[{match_id}] 채팅 적재 중...")

with DAG(
    dag_id=f'lol_chat_log_{Config.VERSION}',
    start_date=Config.START_DATE,
    schedule_interval='* * * * *',
    catchup=False,
    tags=['chat'] + Config.PROJECT_TAGS
) as dag:

    load_chat = PythonOperator(
        task_id='fetch_and_load_chat_data',
        python_callable=fetch_and_load_chat_data
    )