%flink.pyflink

######################################
# 채팅 데이터
######################################

# 1. 체크포인트 설정 (장애 복구용)
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")


# 2. 브론즈 채팅 스트림 (Source - 날것의 데이터 입구)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS brz_chat_strm (
        `timestamp` BIGINT,
        `time_text` STRING,
        `nickname` STRING,
        `content` STRING,
        `platform` STRING
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'lol-highlighter-dev-an2-kds-brz-chat',  
        'aws.region' = 'ap-northeast-2',
        'scan.stream.initpos' = 'LATEST',
        'format' = 'json'
    )
""")

# 3. 실버 채팅 스트림 (to Kinesis)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS slv_chat_strm (
        nickname STRING,
        clean_msg STRING,
        platform STRING,
        `timestamp` BIGINT,
        row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
        WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'lol-highlighter-dev-an2-kds-slv-chat',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json'
    )
""")

# 4. 데이터 정제 및 적재 실행 (Nightbot 필터링)
st_env.execute_sql("""
    INSERT INTO slv_chat_strm
    SELECT 
        `nickname`,
        TRIM(`content`) as clean_msg,
        `platform`,
        `timestamp`
    FROM brz_chat_strm
    WHERE `nickname` <> '@nightbot'          -- 봇 메시지 1차 컷!
      AND `content` NOT LIKE '%[warning]%'   -- 혹시 모를 시스템 경고 메시지 2차 컷!
""")
