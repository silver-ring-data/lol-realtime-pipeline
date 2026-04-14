'''
PostgresHook 사용: 저번에는 PostgresOperator를 썼지? 이번엔 PythonOperator 안에서 PostgresHook을 썼어. 왜냐하면 한 번의 작업으로 두 개의 다른 테이블에 데이터를 나눠 담아야 하기 때문이야. 이게 훨씬 유연해!

데이터의 세분화: 이제 단순히 gold_diff만 넣지 않고 blue_total_gold 같은 세부 지표와 event_detail(스틸 여부 등)을 다 넣어. 그래야 나중에 "역전의 바론 스틸" 같은 하이라이트 제목을 뽑을 수 있어.

XCom 활용: current_match_id를 넘겨줘서 나중에 분석 DAG가 "아, 지금 LCK_2026_T1_GEN 경기를 분석하면 되는구나"라고 알게 할 거야.
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import json

# 1. 경기 수치(Metrics)와 이벤트(Events)를 동시에 시뮬레이션해서 가져오는 함수
def fetch_and_load_match_data(**context):
    # 실제로는 Riot API나 GOL.GG를 크롤링한 데이터를 가져오겠지만
    # 지금은 테스트를 위해 바뀐 스키마 규격에 맞는 데이터를 생성할게.
    
    match_id = "LCK_2026_T1_GEN"
    
    # [데이터 A] 경기 수치 (match_metrics 테이블용)
    metrics_data = {
        "match_id": match_id,
        "game_time": 1200,      # 20분
        "gold_diff": -2500,     # 블루팀이 2500골드 뒤지는 중
        "blue_total_gold": 35000,
        "red_total_gold": 37500
    }
    
    # [데이터 B] 경기 이벤트 (match_events 테이블용)
    # 은비가 말한 '바론/용' 이벤트를 발생시켜보자!
    event_data = {
        "match_id": match_id,
        "event_type": "BARON",
        "event_detail": "STEAL", # 바론 스틸 상황! (하이라이트 점수 폭발 예정)
        "team_color": "BLUE",
        "game_time": 1205
    }

    pg_hook = PostgresHook(postgres_conn_id='postgres_db_eunbee')

    # 1. Metrics 저장
    metrics_sql = """
        INSERT INTO match_metrics (match_id, game_time, gold_diff, blue_total_gold, red_total_gold)
        VALUES (%s, %s, %s, %s, %s);
    """
    pg_hook.run(metrics_sql, parameters=(
        metrics_data['match_id'], metrics_data['game_time'], 
        metrics_data['gold_diff'], metrics_data['blue_total_gold'], metrics_data['red_total_gold']
    ))

    # 2. Events 저장
    events_sql = """
        INSERT INTO match_events (match_id, event_type, event_detail, team_color, game_time)
        VALUES (%s, %s, %s, %s, %s);
    """
    pg_hook.run(events_sql, parameters=(
        event_data['match_id'], event_data['event_type'], 
        event_data['event_detail'], event_data['team_color'], event_data['game_time']
    ))
    
    # 나중에 분석 Task에서 쓸 수 있게 match_id를 넘겨줌
    context['ti'].xcom_push(key='current_match_id', value=match_id)
    print(f"[{match_id}] 수치 및 이벤트 데이터 적재 완료!")

with DAG(
    dag_id='lol_match_data_v0.2',
    start_date=datetime(2026, 4, 14),
    schedule_interval=None,
    catchup=False,
    tags=['v0.2', 'hybrid_detection']
) as dag:

    # [TASK] 데이터 수집 및 적재 (PythonOperator로 통합 처리)
    # 쿼리가 복잡해지면 PostgresOperator보다 Python에서 Hook을 쓰는게 관리가 편해!
    extract_and_load_task = PythonOperator(
        task_id='fetch_and_load_match_data',
        python_callable=fetch_and_load_match_data,
        provide_context=True
    )

    extract_and_load_task