%flink.pyflink

# ==========================================
# ⚙️ 1. 환경 설정 및 점수 기준
# ==========================================
st_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "5000 ms")
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")

# 점수 기준
EVENT_WEIGHTS = {'CHAMPION_KILL': 20, 'TOWER_BUILDING_KILL': 15, 'DRAGON_KILL': 25, 'BARON_KILL': 40}
CHAT_LEVELS = [(100, 100), (51, 50), (21, 20)]
MATCH_OFFSETS = {"worlds_2024_20241102_t1_blg_g4": 55000, "worlds_2024_20241102_t1_blg_g5": 3182000}

# SQL 로직 변수 생성
event_case = "\n".join([f"WHEN event_type = '{k}' THEN {v}" for k, v in EVENT_WEIGHTS.items()])
chat_case = "\n".join([f"WHEN SUM(c_count) >= {count} THEN {score}" for count, score in CHAT_LEVELS])
offset_logic = "\n".join([f"WHEN match_id = '{k}' THEN {v}" for k, v in MATCH_OFFSETS.items()])

# ==========================================
# 🥉 2. 테이블 초기화 및 재정의 (S3 롤링 정책 수정 완료)
# ==========================================
# (1) 게임 데이터 소스
st_env.execute_sql("DROP TABLE IF EXISTS brz_game_tbl")
st_env.execute_sql(f"""
    CREATE TABLE brz_game_tbl (
        `match_id` STRING,
        `timestamp` BIGINT,
        `events` ARRAY<ROW<`timestamp` BIGINT, `event_type` STRING, `killer_id` INT, 
                           `assisting_participant_ids` ARRAY<INT>, `victim_id` INT, `team_id` INT>>,
        adjusted_time AS TO_TIMESTAMP(FROM_UNIXTIME((`timestamp` + CASE {offset_logic} ELSE 0 END) / 1000)),
        WATERMARK FOR adjusted_time AS adjusted_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-game',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")

# (2) 채팅 데이터 소스
st_env.execute_sql("DROP TABLE IF EXISTS brz_chat_tbl")
st_env.execute_sql("""
    CREATE TABLE brz_chat_tbl (
        `timestamp` BIGINT, `nickname` STRING, `content` STRING,
        row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
        WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-chat',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")

# (3) S3 골드 싱크 정의 (Rolling Policy 추가)
st_env.execute_sql("DROP TABLE IF EXISTS gld_s3_score_tbl")
st_env.execute_sql("""
    CREATE TABLE gld_s3_score_tbl (
        match_id STRING, window_start TIMESTAMP(3),
        event_score DOUBLE, chat_score INT, final_score DOUBLE
    ) PARTITIONED BY (match_id)
    WITH (
        'connector' = 'filesystem',
        'path' = 's3://de-ai-05-lol-an2-s3-datalake/gold/highlight_scores/',
        'format' = 'json',
        -- 🌟 롤링 정책 설정: 파일 관리 최적화
        'sink.rolling-policy.file-size' = '128MB',         -- 파일 크기가 128MB에 도달하면 새 파일로 교체
        'sink.rolling-policy.rollover-interval' = '1 min', -- 1분이 지나면 새 파일 생성 (체크포인트 주기와 연동됨)
        'sink.rolling-policy.check-interval' = '10 sec'    -- 10초마다 롤링 조건(크기/시간)을 체크
    )
""")

# ==========================================
# 🚀 3. 통합 연산 및 S3 적재
# ==========================================
unified_stream = f"""
    SELECT match_id, adjusted_time as ts, (CASE {event_case} ELSE 0 END) as e_score, 0 as c_count
    FROM brz_game_tbl
    CROSS JOIN UNNEST(events) AS E (`timestamp`, event_type, killer_id, assisting_participant_ids, victim_id, team_id)
    UNION ALL
    SELECT 'worlds_2024_20241102_t1_blg_g4' as match_id, row_time as ts, 0 as e_score, 1 as c_count
    FROM brz_chat_tbl
    WHERE nickname <> '@nightbot' AND content NOT LIKE '%[warning]%'
"""

st_env.execute_sql(f"""
    INSERT INTO gld_s3_score_tbl
    SELECT match_id,
           HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND) as window_start,
           CAST(SUM(e_score) AS DOUBLE),
           CASE {chat_case} ELSE 0 END,
           (SUM(e_score) * 0.6 + (CASE {chat_case} ELSE 0 END) * 0.4)
    FROM ({unified_stream})
    GROUP BY match_id, HOP(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND)
""")
