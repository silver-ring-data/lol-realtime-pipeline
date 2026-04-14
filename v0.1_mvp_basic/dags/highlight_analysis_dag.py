from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def analyze_hybrid_highlights(**context):
    # 1. 대상 Match ID 가져오기 (XCom 동기화)
    match_id = context['ti'].xcom_pull(dag_id='lol_match_data_v0.2', task_ids='fetch_and_load_match_data', key='current_match_id')
    if not match_id:
        match_id = "LCK_2026_T1_GEN"

    pg_hook = PostgresHook(postgres_conn_id='postgres_db_eunbee')

    # 2. 하이브리드 분석 쿼리 (핵심 로직)
    # 로직: 
    #   1) 이벤트(바론/용)가 있으면 우선적으로 하이라이트 타입 지정
    #   2) 이벤트는 없는데 특정 시간대 채팅 수(count)가 평소보다 높으면 'SUPER_PLAY'로 마킹
    #   3) 채팅 수와 골드 격차를 합산하여 Impact Score 계산
    analysis_sql = f"""
        INSERT INTO highlight_summary (match_id, start_time, end_time, highlight_type, impact_score, is_major)
        SELECT 
            m.match_id,
            m.game_time - 10 as start_time, -- 사건 전 10초
            m.game_time + 20 as end_time,   -- 사건 후 20초 (총 30초 구간)
            CASE 
                WHEN e.event_type IS NOT NULL THEN e.event_type || '_' || COALESCE(e.event_detail, 'OCCURRED')
                WHEN COUNT(c.id) >= 5 THEN 'SUPER_PLAY_DOUBT' -- 지표는 없는데 채팅이 터진 경우 (테스트용으로 5개 설정)
                ELSE 'GENERAL_MOMENT'
            END as highlight_type,
            (COUNT(c.id) * 10 + COALESCE(ABS(m.gold_diff)/100, 0)) as impact_score,
            CASE WHEN e.event_type = 'BARON' OR COUNT(c.id) >= 10 THEN TRUE ELSE FALSE END as is_major
        FROM match_metrics m
        LEFT JOIN match_events e ON m.match_id = e.match_id AND ABS(m.game_time - e.game_time) <= 5
        LEFT JOIN chat_logs c ON m.match_id = c.match_id 
            AND c.created_at BETWEEN m.created_at - interval '10 seconds' AND m.created_at + interval '20 seconds'
        WHERE m.match_id = '{match_id}'
        GROUP BY m.match_id, m.game_time, m.gold_diff, e.event_type, e.event_detail
        HAVING COUNT(c.id) > 0 OR e.event_type IS NOT NULL; -- 무의미한 구간 제외
    """
    
    pg_hook.run(analysis_sql)
    print(f"[{match_id}] 하이브리드 하이라이트 분석 및 적재 완료!")

with DAG(
    dag_id='lol_analysis_v0.2',
    start_date=datetime(2026, 4, 14),
    schedule_interval=None,
    catchup=False,
    tags=['v0.2', 'analysis', 'hybrid']
) as dag:

    task_analyze = PythonOperator(
        task_id='calculate_hybrid_highlight',
        python_callable=analyze_hybrid_highlights,
        provide_context=True
    )

    task_analyze