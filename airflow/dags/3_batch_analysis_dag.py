import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator # 🌟 갈림길을 만드는 오퍼레이터
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# --- (config.yaml 불러오는 부분 생략) ---
# ---------------------------------------------------------
# 🛠️ 1. Config 및 환경 변수 설정
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

# Match ID 생성 함수 (중복 방지용)
def generate_match_id(set_key):
    return f"{metadata['tournament']}_{metadata['match_date']}_{metadata['teams']}_{set_key}"


target_match_ids = [generate_match_id(set_key) for set_key in game_data['game_sets'].keys()]
target_match_ids_str = ", ".join([f"'{m_id}'" for m_id in target_match_ids])

default_args = {'owner': 'eunbee', 'start_date': datetime(2026, 5, 4), 'retries': 1}

with DAG(
    dag_id='3_batch_analysis_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['analysis', 'glue', 'athena']
) as dag:

    # 🌟 2번 DAG에서 넘겨준 파라미터(conf)를 꺼내 쓰는 방법!
    match_id = "{{ dag_run.conf.get('match_id') }}"
    is_last_set = "{{ dag_run.conf.get('is_last_set') }}"

    start_analysis = EmptyOperator(task_id='start_analysis')

# A. Glue로 JSON -> Parquet 변환
    glue_transform = GlueJobOperator(
        task_id='glue_transform_step',
        job_name=GLUE_ETL_JOB,
        script_args={'--match_id': match_id}
    )

    # B. 파티션 갱신
    repair_game = AthenaOperator(
        task_id='repair_game_table',
        query=f"MSCK REPAIR TABLE {ATHENA_DB}.{TABLES['btch_slv_game']};",
        database=ATHENA_DB
    )
    repair_chat = AthenaOperator(
        task_id='repair_chat_table',
        query=f"MSCK REPAIR TABLE {ATHENA_DB}.{TABLES['btch_slv_chat']};",
        database=ATHENA_DB
    )

    # C. 개별 세트 분석 (결과를 btch_gld_report 테이블에 저장)
    athena_set_analysis = AthenaOperator(
        task_id='analyze_set_to_gold',
        query='sql/analyze_set_reports.sql',
        database=ATHENA_DB,
        params={
            'DB': ATHENA_DB,
            'GAME_TABLE_NAME': TABLES['btch_slv_game'],
            'CHAT_TABLE_NAME': TABLES['btch_slv_chat'],
            'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
            'match_id': match_id
        }
    )

    # 🌟 갈림길: 마지막 세트인가?
    def check_if_last_set(**context):
        # conf에서 꺼낼 때 문자열 비교 주의!
        if str(context['dag_run'].conf.get('is_last_set')) == 'True':
            return 'generate_final_series_report'
        return 'end_analysis'

    branch_task = BranchPythonOperator(
        task_id='check_last_set_branch',
        python_callable=check_if_last_set
    )

    # D. 시리즈 종합 분석 (Gold Report 테이블을 읽어서 합산!)
    series_final_report = AthenaOperator(
        task_id='generate_final_series_report',
        query='sql/generate_final_report.sql',
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/gold/final_reports/{match_key}/",
        params={
            'ATHENA_DB': ATHENA_DB,
            'REPORT_TABLE_NAME': TABLES['btch_gld_report'],
            'target_match_ids_str': target_match_ids_str, # 상단에서 정의한 G1~G5 ID 리스트
            'match_key': match_key
        }
    )

    send_report_email = EmailOperator(
        task_id='send_final_report_email',
        to='cindypink17@gmail.com',
        subject=f'🔥 LoL Series Report: {match_key} 🔥',
        html_content=f"시리즈 분석이 완료되었습니다. S3에서 결과를 확인하세요!"
    )

    end_analysis = EmptyOperator(task_id='end_analysis')

    # 🔗 의존성 연결 (폭포수처럼!)
    start_analysis >> glue_transform >> [repair_game, repair_chat] >> athena_set_analysis >> branch_task
    branch_task >> series_final_report >> send_report_email
    branch_task >> end_analysis