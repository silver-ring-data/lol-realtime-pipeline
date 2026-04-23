from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from config import Config
import requests

def check_game_start():
    response = requests.get(Config.TEST_API_URL)
    if response.status_code == 200:
        print(f"[{Config.MATCH_ID}] 경기 시작 확인!")
        return True
    raise Exception("경기 시작 전입니다.")

with DAG(
    dag_id=f'master_commander_{Config.VERSION}',
    start_date=Config.START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=['master'] + Config.PROJECT_TAGS
) as dag:

    detect_start = PythonOperator(
        task_id='detect_game_start',
        python_callable=check_game_start
    )

    # 하위 DAG 트리거 시에도 Config 버전 적용
    trigger_match = TriggerDagRunOperator(
        task_id='trigger_match_data',
        trigger_dag_id=f'lol_match_data_{Config.VERSION}'
    )

    trigger_chat = TriggerDagRunOperator(
        task_id='trigger_chat_log',
        trigger_dag_id=f'lol_chat_log_{Config.VERSION}'
    )

    detect_start >> [trigger_match, trigger_chat]