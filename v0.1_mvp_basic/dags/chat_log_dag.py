from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='lol_chat_log_v0.1',
    start_date=datetime(2026, 4, 13),
    schedule_interval=None,
    catchup=False,
    tags=['mvp', 'eunbee']
) as dag:

    # chat_logs 테이블에 가짜 채팅 데이터를 넣는 작업
    insert_chat_task = PostgresOperator(
        task_id='insert_mock_chat',
        postgres_conn_id='postgres_db_eunbee',
        sql="""
            INSERT INTO chat_logs (match_id, content)
            VALUES ('MATCH_V01_TEST', '대박! 역전 가즈아!!');
        """
    )