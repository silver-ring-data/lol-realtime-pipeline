'''
타 DAG XCom 참조: xcom_pull 할 때 dag_id를 명시했지? 이렇게 하면 match_data DAG가 만든 경기 ID를 chat_log DAG가 그대로 이어받아서 **"같은 경기의 데이터"**임을 보장할 수 있어. (병렬 실행 시 데이터 동기화의 핵심이야!)

벌크(Bulk) 스타일: 한 번 실행될 때 여러 개의 채팅 행을 넣도록 구성했어. 나중에 우리가 테스트할 '채팅 폭증(Spike)' 상황을 시뮬레이션하기 위해서야.

스키마 매칭: match_id, user_id, content, sentiment_score 4개 컬럼을 정확히 채워 넣었어.
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

# 1. 채팅 데이터를 생성하고 DB에 적재하는 함수
def fetch_and_load_chat_data(**context):
    # 1번 DAG(match_data)가 push한 match_id를 가져옴 (동기화)
    # 만약 직접 실행할 경우를 대비해 기본값 설정
    match_id = context['ti'].xcom_pull(dag_id='lol_match_data_v0.2', task_ids='fetch_and_load_match_data', key='current_match_id')
    if not match_id:
        match_id = "LCK_2026_T1_GEN"

    # 테스트를 위한 랜덤 채팅 데이터 생성
    chat_samples = [
        "와 무빙 미쳤다 ㄷㄷ", "이걸 사네?", "진짜 역대급 경기", 
        "바론 스틸?? 실화냐", "ㅋㅋㅋㅋㅋㅋ", "???", "나이스!!!"
    ]
    
    # 5개의 채팅이 동시에 들어왔다고 가정 (빈도 테스트용)
    rows = []
    for _ in range(5):
        rows.append((
            match_id,
            f"user_{random.randint(1, 100)}",
            random.choice(chat_samples),
            random.uniform(0, 1) # 0~1 사이의 가짜 감성 점수
        ))

    pg_hook = PostgresHook(postgres_conn_id='postgres_db_eunbee')
    
    # 새 스키마 규격에 맞춰 INSERT (created_at은 DB에서 자동생성)
    sql = """
        INSERT INTO chat_logs (match_id, user_id, content, sentiment_score)
        VALUES (%s, %s, %s, %s);
    """
    
    # 여러 행을 한꺼번에 삽입
    for row in rows:
        pg_hook.run(sql, parameters=row)
        
    print(f"[{match_id}] 채팅 로그 5건 적재 완료!")

with DAG(
    dag_id='lol_chat_log_v0.2',
    start_date=datetime(2026, 4, 14),
    schedule_interval=None, # 지휘관 DAG에 의해 실행됨
    catchup=False,
    tags=['v0.2', 'sentiment']
) as dag:

    # [TASK] 채팅 데이터 수집 및 적재
    load_chat_task = PythonOperator(
        task_id='fetch_and_load_chat',
        python_callable=fetch_and_load_chat_data,
        provide_context=True
    )

    load_chat_task