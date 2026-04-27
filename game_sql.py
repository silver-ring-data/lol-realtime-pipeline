%flink.pyflink
######################################
# 게임 데이터
######################################

# 1. 체크포인트 설정
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")


# 3. 브론즈 게임 스트림 생성 (Source)
# 파이썬 봇이 쏘는 원본 데이터 규격과 100% 일치시킴
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS brz_game_strm (
        `match_id` STRING,
        `timestamp` BIGINT,
        `participant_frames` MAP<STRING, ROW<`total_gold` INT, `minions_killed` INT>>,
        `events` ARRAY<ROW<
            `timestamp` BIGINT,
            `event_type` STRING,
            `killer_id` INT,
            `assisting_participant_ids` ARRAY<INT>,
            `victim_id` INT,
            `team_id` INT
        >>
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'lol-highlighter-dev-an2-kds-brz-game',
        'aws.region' = 'ap-northeast-2',
        'scan.stream.initpos' = 'LATEST',
        'format' = 'json'
    )
""")

# 4. 실버 게임 스트림 생성 (Sink)
# 브론즈와 컬럼 타입/순서를 완벽하게 일치시킴 (minions_killed 포함)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS slv_game_strm (
        `match_id` STRING,
        `timestamp` BIGINT,
        `participant_frames` MAP<STRING, ROW<`total_gold` INT, `minions_killed` INT>>, 
        `events` ARRAY<ROW<
            `timestamp` BIGINT,
            `event_type` STRING,
            `killer_id` INT,
            `assisting_participant_ids` ARRAY<INT>,
            `victim_id` INT,
            `team_id` INT
        >>
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'lol-highlighter-dev-an2-kds-slv-game',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json',
        'sink.partitioner-field-delimiter' = ''
    )
""")

# 5. 데이터 적재 실행 (Bronze -> Silver)
st_env.execute_sql("""
    INSERT INTO slv_game_strm
    SELECT 
        match_id,
        `timestamp`,
        participant_frames,
        events
    FROM brz_game_strm
""")
