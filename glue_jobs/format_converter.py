"""Bronze -> Silver 포맷 변환 Glue Job.

S3 Bronze 레이어의 원본 JSON을 읽어 중복을 제거하고, match_id로 파티셔닝된
Parquet으로 Silver 레이어에 적재한다. Athena 조회 성능과 스캔 비용을 개선하는
것이 목적이다.

Airflow가 대상 match_id와 버킷명을 Job 인자로 전달한다.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# 1. Job 인자 파싱 (JOB_NAME은 Glue 기본 인자, 나머지는 Airflow가 전달)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'match_id', 'bucket_name'])

# 2. Glue / Spark 컨텍스트 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

MATCH_ID = args['match_id']
BUCKET = args['bucket_name']

# S3 경로 설정
GAME_BRONZE = f"s3://{BUCKET}/bronze/game/"
CHAT_BRONZE = f"s3://{BUCKET}/bronze/chat/"
GAME_SILVER = f"s3://{BUCKET}/silver/game/"
CHAT_SILVER = f"s3://{BUCKET}/silver/chat/"

# ---------------------------------------------------------
# 3. 게임 데이터 정제 (원본에 match_id가 포함되어 있음)
# ---------------------------------------------------------
# 세트에 따라 Bronze에 파일이 없을 수 있으므로 실패해도 Job 전체를 중단하지 않는다.
try:
    game_df = spark.read.json(GAME_BRONZE)
    refined_game = game_df.filter(F.col("match_id") == MATCH_ID) \
                          .dropDuplicates()

    # Silver 레이어로 저장 (match_id 파티셔닝)
    refined_game.write.mode("append").partitionBy("match_id").parquet(GAME_SILVER)
    print(f"[{MATCH_ID}] 게임 데이터 Silver 적재 완료")
except Exception as e:
    print(f"[{MATCH_ID}] 게임 데이터 처리를 건너뜁니다: {str(e)}")

# ---------------------------------------------------------
# 4. 채팅 데이터 정제 (원본에 match_id가 없어 리터럴로 주입)
# ---------------------------------------------------------
try:
    chat_df = spark.read.json(CHAT_BRONZE)

    # 채팅 원본에는 match_id가 없으므로 Job 인자 값을 리터럴 컬럼으로 추가해
    # 게임 데이터와 동일한 파티션 구조를 맞춘다. 봇 계정은 분석에서 제외한다.
    refined_chat = chat_df.withColumn("match_id", F.lit(MATCH_ID)) \
                          .filter(F.col("nickname") != "@nightbot") \
                          .dropDuplicates() \
                          .withColumn("timestamp", F.col("timestamp").cast("long"))

    # Silver 레이어로 저장 (match_id 파티셔닝)
    refined_chat.write.mode("append").partitionBy("match_id").parquet(CHAT_SILVER)
    print(f"[{MATCH_ID}] 채팅 데이터 Silver 적재 완료")
except Exception as e:
    print(f"[{MATCH_ID}] 채팅 데이터 처리를 건너뜁니다: {str(e)}")

# Job 커밋 (북마크 상태를 확정하고 Glue에 성공을 보고한다)
job.commit()