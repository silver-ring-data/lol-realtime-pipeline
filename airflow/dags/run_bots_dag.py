import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator  # 🏁 시작점용
from airflow.sensors.time_delta import TimeDeltaSensor

from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# ---------------------------------------------------------
# 🛠️ 1. Config 및 환경 변수 설정
# ---------------------------------------------------------
CONFIG_PATH = "/opt/airflow/config/config.yaml"
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

ctx = config['app_context']
match_key = ctx['active_match_key']
meta = config['matches'][match_key]['metadata']
game_data = config['matches'][match_key]['data']

BUCKET_NAME = config['resources']['storage_serving']['s3_bucket']
GLUE_ETL_JOB = config['resources']['processing']['glue_job_refiner']
ATHENA_DB = config['resources']['processing']['athena_db']

GAME_BOT_DIR = "/opt/airflow/data_source/producer/game"
CHAT_BOT_DIR = "/opt/airflow/data_source/producer/chat"

game_set_keys = list(game_data['game_sets'].keys())

default_args = {
    'owner': 'eunbee',
    'start_date': datetime(2026, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_match_id(set_key):
    return f"{meta['tournament']}_{meta['match_date']}_{meta['teams']}_{set_key}"

# ---------------------------------------------------------
# 🚀 2. DAG 정의
# ---------------------------------------------------------
with DAG(
    dag_id='lol_series_analysis_master_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['lol', 'analysis', 'athena', 'batch'],
) as dag:

 # 🏁 1. 모든 프로세스의 통합 시작점
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    tables_created = EmptyOperator(task_id='tables_created') # 👈 테이블 생성 완료 체크용

    # 🛠️ 2. 배치용 테이블 자동 생성 (각각 개별 태스크로 분리!)
    ddl_queries = {
        'game': f"CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.btch_slv_game_table ( match_id STRING, ts BIGINT, events ARRAY<STRUCT<timestamp: BIGINT, event_type: STRING, killer_id: INT, victim_id: STRING, team_id: INT>> ) PARTITIONED BY (p_match_id STRING) STORED AS PARQUET LOCATION 's3://{BUCKET_NAME}/silver/game/'",
        'chat': f"CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.btch_slv_chat_table ( nickname STRING, content STRING, platform STRING, ts BIGINT ) PARTITIONED BY (p_match_id STRING) STORED AS PARQUET LOCATION 's3://{BUCKET_NAME}/silver/chat/'",
        'report': f"CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.btch_gld_match_reports ( match_id STRING, team STRING, total_kills INT, total_gold INT, chat_count INT ) STORED AS PARQUET LOCATION 's3://{BUCKET_NAME}/gold/match_reports/'"
    }

    for name, query in ddl_queries.items():
        create_tbl_task = AthenaOperator(
            task_id=f'create_batch_{name}_table',
            query=query,
            database=ATHENA_DB,
            output_location=f"s3://{BUCKET_NAME}/athena-results/ddl/",
        )
        start_pipeline >> create_tbl_task >> tables_created

    # 🛠️ 2. 배치용 테이블 자동 생성 (DDL)
    # 분석 전, 실버/골드 테이블이 Glue Catalog에 등록되어 있는지 확인하고 없으면 생성해!
    create_batch_tables = AthenaOperator(
        task_id='create_batch_medallion_tables',
        query='sql/create_batch_tables.sql',  # 👈 SQL 파일 경로 지정
        params={                             # 👈 SQL 안에서 쓸 변수들 전달
            'ATHENA_DB': ATHENA_DB,
            'BUCKET_NAME': BUCKET_NAME
        },
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/ddl/",
        aws_conn_id='aws_default',
    )

    # [Step 1] 공통 채팅 봇 실행 (독립 트랙)
    run_chat_bot = BashOperator(
        task_id='run_all_series_chat_bot',
        bash_command=(
            f"cd {CHAT_BOT_DIR} && "
            f"setsid python chat_bot.py '{{{{ ts }}}}' > chat_bot.log 2>&1 < /dev/null & "
            "sleep 2;"
        ),
    )

    # 🔗 시작하면 테이블 생성과 채팅 봇이 동시에 진행됨
    start_pipeline >> run_chat_bot

    all_set_analysis_tasks = []
    # 🔥 게임 세트 흐름은 '테이블 생성이 완료된 후' 시작되도록 설정!
    previous_game_start_task = tables_created

    # 🔄 3. 루프를 통한 동적 태스크 생성 (세트별 로직)
    for idx, set_key in enumerate(game_set_keys):
        set_info = game_data['game_sets'][set_key]
        match_id = set_info.get('match_id', generate_match_id(set_key))
        wait_seconds = set_info['wait_seconds']
        
        # A. 세트 시작 대기
        wait_before_set = TimeDeltaSensor(
            task_id=f'wait_before_{set_key}',
            delta=timedelta(seconds=wait_seconds),
        )

        # B-0. 기존 종료 시그널 제거 🧹 (Bash로 더 확실하게 처리)
        clean_finished_signal = BashOperator(
            task_id=f'clean_{set_key}_finished_signal',
            bash_command=f"aws s3 rm s3://{BUCKET_NAME}/status/{match_id}_finished.txt || true",
        )

        # B. 게임 봇 실행
        run_game_bot = BashOperator(
            task_id=f'run_game_bot_{set_key}',
            bash_command=(
                f"cd {GAME_BOT_DIR} && "
                f"python game_bot.py '{{{{ ts }}}}' {set_key}"
            ),
        )

        # C. 종료 시그널 감시 (S3 finished.txt)
        wait_for_nexus = S3KeySensor(
            task_id=f'wait_{set_key}_finished_signal',
            bucket_name=BUCKET_NAME,
            bucket_key=f"status/{match_id}_finished.txt",
            poke_interval=30,
            timeout=7200,
        )

        # D. Glue Job: JSON -> Parquet (Silver Layer)
        glue_transform = GlueJobOperator(
            task_id=f'glue_transform_{set_key}',
            job_name=GLUE_ETL_JOB,
            script_args={
                '--match_id': match_id,
                '--bucket_name': BUCKET_NAME
            },
            aws_conn_id='aws_default',
        )

       
        # E. Athena: 세트별 분석 (Gold Layer)
        athena_set_analysis = AthenaOperator(
            task_id=f'athena_analyze_{set_key}',
            query='sql/analyze_set_reports.sql',  # 👈 SQL 파일 경로만 딱!
            params={                              # 👈 SQL 파일 안에서 쓸 변수들 전달!
                'ATHENA_DB': ATHENA_DB,
                'match_id': match_id
            },
            database=ATHENA_DB,
            output_location=f"s3://{BUCKET_NAME}/athena-results/sets/{set_key}/",
            aws_conn_id='aws_default',
        )
        # 🛠️ F. 새 파티션 인식 (MSCK REPAIR) - 두 개로 쪼개기!
        repair_game_pt = AthenaOperator(
            task_id=f'repair_game_pt_{set_key}',
            query=f"MSCK REPAIR TABLE {ATHENA_DB}.btch_slv_game_table",
            database=ATHENA_DB,
            output_location=f"s3://{BUCKET_NAME}/athena-results/repair/"
        )
        
        repair_chat_pt = AthenaOperator(
            task_id=f'repair_chat_pt_{set_key}',
            query=f"MSCK REPAIR TABLE {ATHENA_DB}.btch_slv_chat_table",
            database=ATHENA_DB,
            output_location=f"s3://{BUCKET_NAME}/athena-results/repair/"
        )

        previous_game_start_task >> wait_before_set >> clean_finished_signal >> run_game_bot
        
        # 쪼개진 repair 태스크들을 병렬로 실행 후 분석으로 넘김
        run_game_bot >> wait_for_nexus >> glue_transform >> [repair_game_pt, repair_chat_pt] >> athena_set_analysis
        
        previous_game_start_task = run_game_bot
        all_set_analysis_tasks.append(athena_set_analysis)


    # 🥇 4. 전체 경기 종합 분석
    
    # 💡 SQL에 넘겨주기 위해 리스트를 미리 콤마로 연결해둔다!
    target_match_ids_str = ", ".join(target_match_ids)

    series_final_report = AthenaOperator(
        task_id='generate_final_series_comprehensive_report',
        query='sql/generate_final_series_report.sql',  # 👈 SQL 파일 경로
        params={                                       # 👈 SQL 파일로 넘겨줄 변수들
            'match_key': match_key,
            'ATHENA_DB': ATHENA_DB,
            'target_match_ids_str': target_match_ids_str
        },
        database=ATHENA_DB,
        output_location=f"s3://{BUCKET_NAME}/athena-results/final_series_report/",
        aws_conn_id='aws_default',
        trigger_rule='all_success' 
    )

    all_set_analysis_tasks >> series_final_report