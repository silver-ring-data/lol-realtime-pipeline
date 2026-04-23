from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from config import Config

def analyze_hybrid_highlights(**context):
    match_id = context['ti'].xcom_pull(
        dag_id=f'lol_match_data_{Config.VERSION}', 
        task_ids='fetch_and_load_match_data', 
        key='current_match_id'
    ) or Config.MATCH_ID

    pg_hook = PostgresHook(postgres_conn_id=Config.DB_CONN_ID)

    # Config 변수를 SQL 쿼리에 삽입 (유지보수 끝판왕)
    analysis_sql = f"""
        INSERT INTO highlight_summary (match_id, start_time, end_time, highlight_type, impact_score, is_major)
        SELECT 
            m.match_id,
            m.game_time - {Config.TIME_WINDOW_BEFORE} as start_time,
            m.game_time + {Config.TIME_WINDOW_AFTER} as end_time,
            CASE 
                WHEN e.event_type IS NOT NULL THEN e.event_type
                WHEN COUNT(c.id) >= {Config.CHAT_THRESHOLD} THEN 'SUPER_PLAY_DOUBT'
                ELSE 'GENERAL_MOMENT'
            END,
            ...
    """
    pg_hook.run(analysis_sql)

with DAG(
    dag_id=f'lol_analysis_{Config.VERSION}',
    start_date=Config.START_DATE,
    schedule_interval='*/2 * * * *',
    catchup=False,
    tags=['analysis'] + Config.PROJECT_TAGS
) as dag:

    analyze = PythonOperator(
        task_id='analyze_hybrid_highlights',
        python_callable=analyze_hybrid_highlights
    )