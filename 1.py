%flink.pyflink

# ==========================================
# ⚙️ 1. 환경 설정 및 점수 기준 (최상단 배치 완벽!)
# ==========================================
st_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "5000 ms")
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")

# 점수 기준
EVENT_WEIGHTS = {
    'CHAMPION_KILL': 20,
    'TOWER_PLATE': 10,       # BOT PLATE, TOP PLATE 등
    'TURRET': 15,            # BOT TOWER, MID TOWER, NEXUS TURRET 등
    'INHIBITOR': 35,         # MID INHIBITOR 등 (전략적 가치 높음)
    'NEXUS_DESTROYED': 100,  # 게임 끝!
    'DRAGON': 25,            # 모든 원소 용
    'BARON_NASHOR': 45,      # 바론
    'RIFT_HERALD': 20,       # 전령
    'VOID_GRUB': 7           # 공허 유충 (한 마리당 점수)
}
CHAT_LEVELS = [
    (100, 100), (90, 90), (80, 80), (70, 70), (60, 60), 
    (50, 50), (40, 40), (30, 30), (20, 20), (10, 10)
]
MATCH_OFFSETS = {"worlds_2024_20241102_t1_blg_g4": 55000, "worlds_2024_20241102_t1_blg_g5": 3182000}

# SQL 로직 변수 생성
event_case = "\n".join([f"WHEN event_type = '{k}' THEN {v}" for k, v in EVENT_WEIGHTS.items()])
chat_case = "\n".join([f"WHEN SUM(c_count) >= {count} THEN {score}" for count, score in CHAT_LEVELS])
offset_logic = "\n".join([f"WHEN match_id = '{k}' THEN {v}" for k, v in MATCH_OFFSETS.items()])

# ==========================================
# 🥉 2. 테이블 초기화 및 재정의
# ==========================================
# (1) 게임 데이터 소스 (proc_time 추가)
st_env.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS brz_game_tbl (
        `match_id` STRING,
        `timestamp` BIGINT,
        `events` ARRAY<ROW<`timestamp` BIGINT, `event_type` STRING, `killer_id` INT, 
                           `assisting_participant_ids` ARRAY<INT>, `victim_id` STRING, `team_id` INT>>,
        join_key AS CAST(1 AS INT),
        adjusted_time AS TO_TIMESTAMP(FROM_UNIXTIME((`timestamp` + CAST((CASE {offset_logic} ELSE 0 END) AS BIGINT)) / 1000)),
        WATERMARK FOR adjusted_time AS adjusted_time - INTERVAL '5' SECOND,
        proc_time AS PROCTIME()  -- 🌟 시스템 현재 시간 컬럼 추가!
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-game',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")

# (2) 채팅 데이터 소스 (proc_time 추가)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS brz_chat_tbl (
        `timestamp` BIGINT, `nickname` STRING, `content` STRING,
        join_key AS CAST(1 AS INT),
        row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
        WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND,
        proc_time AS PROCTIME()  -- 🌟 시스템 현재 시간 컬럼 추가!
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-chat',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")



# (3) S3 골드 싱크 (롤링 정책 유지)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS gld_s3_score_tbl (
        match_id STRING, window_start TIMESTAMP(3),
        event_score DOUBLE, chat_score INT, final_score DOUBLE
    ) PARTITIONED BY (match_id)
    WITH (
        'connector' = 'filesystem',
        'path' = 's3://de-ai-05-lol-an2-s3-datalake/gold/highlight_scores/',
        'format' = 'json',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '1 min',
        'sink.rolling-policy.check-interval' = '10 sec'
    )
""")


# ==========================================
# 🌟 3. 통합 연산 로직 (시간 핀포인트 타겟팅!)
# ==========================================
unified_stream = f"""
    -- 1. 채팅 데이터 (match_id는 조인 대신 여기서 NULL 처리)
    SELECT 
        CAST(NULL AS STRING) as match_id, 
        C.row_time as ts, 
        CAST(0 AS DOUBLE) as e_score,   
        1 as c_count
    FROM brz_chat_tbl C
    WHERE C.nickname <> '@nightbot' AND C.content NOT LIKE '%[warning]%'
    
    UNION ALL
    
    -- 2. 🌟 게임 하트비트 (핵심!)
    -- 이벤트가 없어도 1초마다 들어오는 이 데이터가 match_id를 멱살 잡고 끌고 옵니다.
    SELECT 
        match_id, 
        adjusted_time as ts, 
        CAST(0 AS DOUBLE) as e_score, 
        0 as c_count
    FROM brz_game_tbl
    
    UNION ALL
    
    -- 3. 게임 이벤트 데이터
    SELECT 
        match_id, 
        TO_TIMESTAMP(FROM_UNIXTIME((E.e_ts + CAST((CASE {offset_logic} ELSE 0 END) AS BIGINT)) / 1000)) as ts, 
        CAST(CASE 
            WHEN event_type = 'CHAMPION_KILL' THEN {EVENT_WEIGHTS['CHAMPION_KILL']}
            WHEN event_type = 'ELITE_MONSTER' AND victim_id LIKE '%BARON%' THEN {EVENT_WEIGHTS['BARON_NASHOR']}
            WHEN event_type = 'ELITE_MONSTER' AND victim_id LIKE '%DRAGON%' THEN {EVENT_WEIGHTS['DRAGON']}
            WHEN event_type = 'ELITE_MONSTER' AND victim_id LIKE '%VOID GRUB%' THEN {EVENT_WEIGHTS['VOID_GRUB']}
            WHEN event_type = 'ELITE_MONSTER' AND victim_id LIKE '%HERALD%' THEN {EVENT_WEIGHTS['RIFT_HERALD']}
            WHEN event_type = 'BUILDING_KILL' AND victim_id LIKE '%PLATE%' THEN {EVENT_WEIGHTS['TOWER_PLATE']}
            WHEN event_type = 'BUILDING_KILL' AND victim_id LIKE '%INHIBITOR%' THEN {EVENT_WEIGHTS['INHIBITOR']}
            WHEN event_type = 'BUILDING_KILL' AND victim_id LIKE '%DESTROYED%' THEN {EVENT_WEIGHTS['NEXUS_DESTROYED']}
            WHEN event_type = 'BUILDING_KILL' AND (victim_id LIKE '%TOWER%' OR victim_id LIKE '%TURRET%') THEN {EVENT_WEIGHTS['TURRET']}
            ELSE 0 
        END AS DOUBLE) as e_score, 
        0 as c_count
    FROM brz_game_tbl
    CROSS JOIN UNNEST(events) AS E (e_ts, event_type, killer_id, assisting_participant_ids, victim_id, team_id)
"""

# (최종 Insert 로직 실행)
st_env.execute_sql(f"""
    INSERT INTO gld_s3_score_tbl
    SELECT 
        -- 🌟 포인트 2: 10초 윈도우 안에 섞여 들어온 게임 데이터에서 match_id를 뽑아냄! 
        -- 만약 게임 이벤트가 하나도 없는 조용한 10초라면 'live_match'라는 폴더에 저장하게 예외 처리 (S3 에러 방지)
        COALESCE(MAX(match_id), 'live_match') as match_id,
        HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND) as window_start,
        SUM(e_score) as event_score,
        CASE {chat_case} ELSE 0 END as chat_score,
        (SUM(e_score) * 0.6 + (CASE {chat_case} ELSE 0 END) * 0.4) as final_score
    FROM ({unified_stream})
    -- 🌟 포인트 3: match_id를 빼고 오직 '시간(HOP)'으로만 평등하게 그룹핑!
    GROUP BY HOP(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND)
""")
