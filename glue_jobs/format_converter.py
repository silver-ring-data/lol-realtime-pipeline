import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# 1. 인자값 가져오기 (Airflow가 넘겨주는 값)
# JOB_NAME은 Glue 내부 기본 인자고, 나머지는 우리가 추가한 거야!
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'match_id', 'bucket_name'])

# 🌟 Glue/Spark 컨텍스트 초기화 (이게 빠지면 안 돼!)
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
# 🎮 2. 게임 데이터 정제 (이미 match_id가 있음)
# ---------------------------------------------------------
# [꿀팁] S3 브론즈에 파일이 없을 경우를 대비해 에러 핸들링을 해주면 더 좋아!
try:
    game_df = spark.read.json(GAME_BRONZE)
    refined_game = game_df.filter(F.col("match_id") == MATCH_ID) \
                          .dropDuplicates()

    # 실버 레이어로 저장 (match_id로 파티셔닝)
    refined_game.write.mode("append").partitionBy("match_id").parquet(GAME_SILVER)
    print(f"✅ {MATCH_ID} 게임 데이터 실버 이관 완료!")
except Exception as e:
    print(f"⚠️ 게임 데이터 처리 중 건너뜀 (혹은 에러): {str(e)}")

# ---------------------------------------------------------
# 💬 3. 채팅 데이터 정제 (🔥 match_id 강제 주입!)
# ---------------------------------------------------------
try:
    chat_df = spark.read.json(CHAT_BRONZE)

    # 은비가 짠 핵심 로직! match_id 컬럼을 리터럴로 주입하는 부분이야.
    refined_chat = chat_df.withColumn("match_id", F.lit(MATCH_ID)) \
                          .filter(F.col("nickname") != "@nightbot") \
                          .dropDuplicates() \
                          .withColumn("timestamp", F.col("timestamp").cast("long"))

    # 실버 레이어로 저장 (이제 채팅도 match_id 파티션이 생겨!)
    refined_chat.write.mode("append").partitionBy("match_id").parquet(CHAT_SILVER)
    print(f"✅ {MATCH_ID} 채팅 데이터 실버 이관 완료!")
except Exception as e:
    print(f"⚠️ 채팅 데이터 처리 중 건너뜀 (혹은 에러): {str(e)}")

# 🌟 Glue Job 종료 선언 (이걸 해야 Glue 콘솔에서 'Success'가 떠!)
job.commit()