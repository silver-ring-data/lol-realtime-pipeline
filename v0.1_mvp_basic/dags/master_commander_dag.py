from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests

# 경기 시작 여부를 확인하는 함수 (Riot API의 'Active Games'라고 가정)
def check_game_start():
    # 실제로는 Riot API를 호출해서 "In Progress" 상태인지 확인해
    # 지금은 연습이니까 무조건 시작한 걸로(True) 리턴하자!
    url = "https://jsonplaceholder.typicode.com/posts/1" 
    response = requests.get(url)
    if response.status_code == 200:
        print("경기가 시작된 것을 확인했습니다. 파이프라인 가동!")
        return True
    else:
        raise Exception("아직 경기가 시작되지 않았습니다.")

with DAG(
    dag_id='master_commander_v0.1',
    start_date=datetime(2026, 4, 14),
    schedule_interval=None, # 일단 수동으로 "경기 시작했다 치고" 실행!
    catchup=False,
    tags=['master', 'eunbee']
) as dag:

    # 1. 경기 시작 감지
    detect_start = PythonOperator(
        task_id='detect_game_start',
        python_callable=check_game_start
    )

    # 2. 경기 지표 수집 트리거 (병렬)
    trigger_match = TriggerDagRunOperator(
        task_id='trigger_match_data',
        trigger_dag_id='lol_match_data_v0.2',
    )

    # 3. 채팅 데이터 수집 트리거 (병렬)
    trigger_chat = TriggerDagRunOperator(
        task_id='trigger_chat_log',
        trigger_dag_id='lol_chat_log_v0.2',
    )

    # 지휘관의 명령: 시작 확인되면 -> 수집A, 수집B 동시에 실행!
    detect_start >> [trigger_match, trigger_chat]