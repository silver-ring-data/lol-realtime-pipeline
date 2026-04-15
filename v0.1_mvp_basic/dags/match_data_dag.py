from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import Config
import random

def fetch_and_load_match_data(**context):
    match_id = Config.MATCH_ID # 변수 사용
    
    # [Step 1] 오늘 배운 S3 백업 (진짜 연동의 핵심!)
    Config.upload_to_s3(raw_data, "raw_match", f"{Config.MATCH_ID}_log")
    # 시뮬레이션 데이터 생성 로직 생략 (규격은 유지)
    pg_hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)
    # ... (기존 INSERT 로직 동일하게 Config.DB_CONN_ID 사용)
    
    context['ti'].xcom_push(key='current_match_id', value=match_id)

with DAG(
    dag_id=f'lol_match_data_{Config.VERSION}',
    start_date=Config.START_DATE,
    schedule_interval='* * * * *',
    catchup=False,
    tags=['data'] + Config.PROJECT_TAGS
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_and_load_match_data',
        python_callable=fetch_and_load_match_data
    )