"""세트 단위 배치 분석 DAG.

수집 릴레이 DAG가 세트 종료를 감지할 때마다 트리거되며,
해당 세트의 원본 데이터를 분석 가능한 형태로 가공한다.

1. Glue ETL로 Bronze(JSON)를 Silver(Parquet)로 변환한다.
2. 변환된 경로를 Athena 파티션으로 등록한다.
3. 세트별 하이라이트 리포트를 Gold 테이블에 적재한다.
4. 마지막 세트인 경우에만 시리즈 종합 리포트를 생성한다.

대상 세트는 트리거 시 전달되는 dag_run.conf(match_id, set_key,
is_last_set)로 결정된다.
"""

import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# ---------------------------------------------------------
# Config 로딩 및 환경 변수 설정
# ---------------------------------------------------------
CONFIG_PATH = "/opt/airflow/config/config.yaml"
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']
game_data = config['matches'][match_key]['data']
metadata = config['matches'][match_key]['metadata']

BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']
GLUE_ETL_JOB = config['resources']['processing']['glue_job_refiner']
ATHENA_DB = config['resources']['processing']['athena_db']
TABLES = config['resources']['processing']['tables']

GAME_BOT_DIR = "/opt/airflow/data_source/producer/game"
CHAT_BOT_DIR = "/opt/airflow/data_source/producer/chat"

game_set_keys = list(game_data['game_sets'].keys())

# 세트별로 고유한 Match ID를 생성한다 (파티션 키로 사용).
def generate_match_id(set_key):
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{set_key}"


target_match_ids = [generate_match_id(set_key) for set_key in game_data['game_sets'].keys()]
target_match_ids_str = ", ".join([f"'{m_id}'" for m_id in target_match_ids])

default_args = {'owner': 'data-engineering', 'start_date': datetime(2026, 5, 4), 'retries': 1}

with DAG(
    dag_id='3_batch_analysis_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['analysis', 'glue', 'athena']
) as dag:

    # 트리거 시 전달된 conf 값을 Jinja 템플릿으로 참조한다.
    match_id = "{{ dag_run.conf.get('match_id') }}"
    is_last_set = "{{ dag_run.conf.get('is_last_set') }}"

    start_analysis = EmptyOperator(task_id='start_analysis')

    # A. Glue로 JSON -> Parquet 변환
    glue_transform = GlueJobOperator(
        task_id='glue_transform_step',
        job_name=GLUE_ETL_JOB,
        script_args={
            '--bucket_name': BUCKET_NAME,
            '--match_id': match_id
            },
        aws_conn_id='aws_default'
    )

    # B. 파티션 등록
    # 전체 경로를 스캔하는 MSCK REPAIR 대신 ALTER TABLE ADD PARTITION을 사용해
    # 해당 세트의 파티션만 등록한다. 세트 수가 늘어도 비용이 일정하다.
    repair_game = AthenaOperator(
        task_id='repair_game_table',
        query=f"""
            ALTER TABLE {ATHENA_DB}.{TABLES['btch_slv_game']} 
            ADD IF NOT EXISTS PARTITION (p_match_id='{{{{ dag_run.conf.get('match_id') }}}}') 
            LOCATION 's3://{BUCKET_NAME}/silver/game/p_match_id={{{{ dag_run.conf.get('match_id') }}}}/';
        """,
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    repair_chat = AthenaOperator(
        task_id='repair_chat_table',
        query=f"""
            ALTER TABLE {ATHENA_DB}.{TABLES['btch_slv_chat']} 
            ADD IF NOT EXISTS PARTITION (p_match_id='{{{{ dag_run.conf.get('match_id') }}}}') 
            LOCATION 's3://{BUCKET_NAME}/silver/chat/p_match_id={{{{ dag_run.conf.get('match_id') }}}}/';
        """,
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )
    # C. 개별 세트 분석
    athena_set_analysis = AthenaOperator(
        task_id='analyze_set_to_gold',
        query='sql/analyze_set_reports.sql',
        database=ATHENA_DB,
        params={
            'DB': ATHENA_DB,
            'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
            'GAME_TABLE_NAME': TABLES['btch_slv_game'],
            'CHAT_TABLE_NAME': TABLES['btch_slv_chat'],
            'match_id': match_id
        },
        output_location=f"s3://{BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    def check_if_last_set(**context):
        """마지막 세트일 때만 시리즈 종합 리포트 태스크로 분기한다."""
        # conf 값은 Jinja 렌더링을 거치며 문자열로 전달되므로 str로 비교한다.
        if str(context['dag_run'].conf.get('is_last_set')) == 'True':
            return 'generate_final_series_report'
        return 'end_analysis'

    branch_task = BranchPythonOperator(
        task_id='check_last_set_branch',
        python_callable=check_if_last_set
    )

    # D. 시리즈 종합 분석 (세트별 Gold 리포트를 합산)
    series_final_report = AthenaOperator(
        task_id='generate_final_series_report',
        query='sql/generate_final_report.sql',
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/gold/final_reports/{match_key}/",
        params={
            'ATHENA_DB': ATHENA_DB,
            'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
            'target_match_ids_str': target_match_ids_str,
            'match_key': match_key
        },
        aws_conn_id='aws_default'
    )
    end_analysis = EmptyOperator(task_id='end_analysis')

    # 태스크 의존성 정의
    start_analysis >> glue_transform >> [repair_game, repair_chat] >> athena_set_analysis >> branch_task
    branch_task >> series_final_report
    branch_task >> end_analysis