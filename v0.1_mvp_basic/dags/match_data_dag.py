from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='lol_match_data_v0.1',
    start_date=datetime(2026, 4, 13),
    schedule_interval=None,  # 일단은 수동 실행!
    catchup=False,
    tags=['mvp', 'eunbee']
) as dag:

    # 오늘은 실습이니까 match_metrics 테이블에 가짜 경기 데이터를 넣을 거야
    insert_match_task = PostgresOperator(
        task_id='insert_mock_match',
        postgres_conn_id='postgres_db_eunbee',  # 아까 Airflow UI에서 만든 ID!
        sql="""
            INSERT INTO match_metrics (match_id, game_time, gold_diff)
            VALUES ('MATCH_V01_TEST', 1200, 2500);
        """
    )
