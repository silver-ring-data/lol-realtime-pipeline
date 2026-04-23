from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import Config
import requests  # 👈 [핵심 추가 1] 외부 API를 찌르기 위해 꼭 필요해!
import random

def fetch_and_load_match_data(**context):
    match_id = Config.MATCH_ID # 변수 사용
    
    # 👈 [핵심 추가 2] 가짜 라이엇 서버(Mock API)에 데이터 요청하기
    # 도커 내부망이니까 'http://mock-api:8000' 이라는 주소로 찌를 거야.
    api_url = f"http://riot-api-server:8000/api/live-match?match_id={match_id}"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status() # 에러(404, 500 등)가 나면 즉시 중단!
        raw_data = response.json()  # API가 준 JSON 데이터를 파이썬 딕셔너리로 변환
        print(f"✅ Mock API 데이터 수집 성공: {raw_data}")
        
    except Exception as e:
        print(f"❌ API 호출 실패 (서버가 죽었거나 주소가 틀림): {e}")
        raise # Airflow 태스크를 실패(Failed) 상태로 만듦

    # [Step 1] 오늘 배운 S3 백업 (진짜 연동의 핵심!)
    # 이제 raw_data에 진짜 데이터가 들어있으니 당당하게 업로드!
    Config.upload_to_s3(raw_data, "raw_match", f"{match_id}_log")
    
    # [Step 2] DB 적재
    pg_hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)
    
    # ... (기존 INSERT 로직: raw_data['gameTime'], raw_data['activeEvents'] 등을 뽑아서 쿼리 작성) ...
    
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