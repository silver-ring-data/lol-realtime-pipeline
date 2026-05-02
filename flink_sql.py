%flink.pyflink

# ==========================================
# ⚙️ 1. 환경 설정 및 점수 기준 
# ==========================================
st_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "5000 ms")
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")

# 🌟 OpenSearch 직접 연결 정보 삭제 (Lambda가 대신 하니까!)

# 점수 기준
EVENT_WEIGHTS = {
    'CHAMPION_KILL': 20,
    'TOWER_PLATE': 10,       
    'TURRET': 15,            
    'INHIBITOR': 35,         
    'NEXUS_DESTROYED': 100,  
    'DRAGON': 25,            
    'BARON_NASHOR': 45,      
    'RIFT_HERALD': 20,       
    'VOID_GRUB': 7           
}
CHAT_LEVELS = [
    (100, 100), (90, 90), (80, 80), (70, 70), (60, 60), 
    (50, 50), (40, 40), (30, 30), (20, 20), (10, 10)
]

chat_case = "\n".join([f"WHEN SUM(c_count) >= {count} THEN {score}" for count, score in CHAT_LEVELS])

# ==========================================
# 🥉 2. 테이블 초기화 및 재정의
# ==========================================

# (1) 🎮 게임 데이터 소스 (기존 동일)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS brz_game_tbl (
        `match_id` STRING,
        `timestamp` BIGINT,
        `events` ARRAY<ROW<`timestamp` BIGINT, `event_type` STRING, `killer_id` INT, 
                           `assisting_participant_ids` ARRAY<INT>, `victim_id` STRING, `team_id` INT>>,
        join_key AS CAST(1 AS INT),
        row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)), 
        WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND,
        proc_time AS PROCTIME() 
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-game',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")

# (2) 💬 채팅 데이터 소스 (기존 동일)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS brz_chat_tbl (
        `timestamp` BIGINT, `nickname` STRING, `content` STRING, `platform` STRING,
        join_key AS CAST(1 AS INT),
        row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
        WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND,
        proc_time AS PROCTIME() 
    ) WITH (
        'connector' = 'kinesis', 'stream' = 'de-ai-05-lol-dev-an2-kds-brz-chat',
        'aws.region' = 'ap-northeast-2', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
    )
""")

# (3) 🥇 S3 골드 싱크 (기존 동일 - 아테나용 백업)
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

# (4) 🥇 Kinesis 골드 하이라이트 점수 싱크 (OpenSearch 대신 Kinesis로!)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS gld_kds_score_tbl (
        match_id STRING, window_start TIMESTAMP(3),
        event_score DOUBLE, chat_score INT, final_score DOUBLE
        -- 🌟 Kinesis는 Append-only라서 PRIMARY KEY가 필요 없음! 삭제!
    ) WITH (
        'connector' = 'kinesis',  
        'stream' = 'de-ai-05-lol-dev-an2-kds-gld-score',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json'
    )
""")

# (5) 🥇 Kinesis 골드 서빙용 채팅 싱크 (OpenSearch 대신 Kinesis로!)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS gld_kds_chat_tbl (
        nickname STRING, content STRING, platform STRING, ts TIMESTAMP(3), priority INT
        -- 🌟 여기도 PRIMARY KEY 삭제!
    ) WITH (
        'connector' = 'kinesis',  
        'stream' = 'de-ai-05-lol-dev-an2-kds-gld-chat',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json'
    )
""")

# ==========================================
# 🌟 3. 통합 연산 로직 (시간 핀포인트 타겟팅!)
# ==========================================
unified_stream = f"""
    -- 1. 채팅 데이터
    SELECT CAST(NULL AS STRING) as match_id, row_time as ts, CAST(0 AS DOUBLE) as e_score, 1 as c_count
    FROM brz_chat_tbl
    WHERE nickname NOT IN ('@nightbot', 'System') 
      AND content NOT LIKE '%[warning]%'
      AND REGEXP_REPLACE(content, '슼갈|젠첩|즙|쵸독|티준딱|개패자|쓰레기|폐기물|배신자|쪽팔린다|수치스럽다|망신|ㅆㅂ|ㅅㅂ|시[ㅣ]+바|ㅆㄱㅈ|ㅂㅅ|ㅈㄹ', '') = content
    
    UNION ALL
    
    -- 2. 게임 하트비트
    SELECT match_id, row_time as ts, CAST(0 AS DOUBLE) as e_score, 0 as c_count
    FROM brz_game_tbl
    
    UNION ALL
    
    -- 3. 게임 이벤트 데이터 
    SELECT match_id, row_time as ts, 
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
           END AS DOUBLE) as e_score, 0 as c_count
    FROM brz_game_tbl
    CROSS JOIN UNNEST(events) AS E (e_ts, event_type, killer_id, assisting_participant_ids, victim_id, team_id)
"""

calc_query = f"""
    SELECT 
        COALESCE(MAX(match_id), 'live_match') as match_id,
        HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND) as window_start,
        SUM(e_score) as event_score,
        CASE {chat_case} ELSE 0 END as chat_score,
        (SUM(e_score) * 0.6 + (CASE {chat_case} ELSE 0 END) * 0.4) as final_score
    FROM ({unified_stream})
    GROUP BY HOP(ts, INTERVAL '3' SECOND, INTERVAL '10' SECOND)
"""

# ==========================================
# 🚀 4. 파이프라인 가동 (Statement Set)
# ==========================================
statement_set = st_env.create_statement_set()

# [Track A] UI 서빙용 채팅 가공 -> Kinesis (chat)로 전송! (람다가 OS로 배달)
statement_set.add_insert_sql(f"""
    INSERT INTO gld_kds_chat_tbl
    SELECT 
        nickname,
        CASE 
            WHEN CHAR_LENGTH(content) >= 30 THEN SUBSTR(content, 1, 27) || '...'
            ELSE REGEXP_REPLACE(content, '([ㄱ-ㅎㅏ-ㅣ가-힣])\\1{{2,}}', '$1$1$1')
        END as content,
        platform,
        row_time as ts,
        CASE 
            WHEN REGEXP_REPLACE(content, '역전|우승|나이스|ㄴㅇㅅ|가즈아|이겼다|비상|한타|솔킬|크랙|슈퍼플레이|이니시|바론|용|유충|전령|골드차이|밸류', '') <> content THEN 1
            ELSE 0 
        END as priority
    FROM brz_chat_tbl
    WHERE 
        nickname NOT IN ('@nightbot', 'System')
        AND content NOT LIKE '!%' 
        AND content NOT LIKE '.%'
        AND content NOT LIKE '/%'
        AND REGEXP_REPLACE(content, '^[\\W_]+$', '') <> '' 
        AND REGEXP_REPLACE(content, '슼갈|젠첩|즙|쵸독|티준딱|개패자|쓰레기|폐기물|배신자|쪽팔린다|수치스럽다|망신|ㅆㅂ|ㅅㅂ|시[ㅣ]+바|ㅆㄱㅈ|ㅂㅅ|ㅈㄹ', '') = content
        AND (
            REGEXP_REPLACE(content, '역전|우승|나이스|ㄴㅇㅅ|가즈아|이겼다|비상|한타|솔킬|크랙|슈퍼플레이|이니시|바론|용|유충|전령|골드차이|밸류', '') <> content
            OR RAND() < 0.1 
        )
""")

# [Track B] 하이라이트 점수 계산 -> S3와 Kinesis(score)로 동시 전송!
statement_set.add_insert_sql(f"INSERT INTO gld_s3_score_tbl {calc_query}")
statement_set.add_insert_sql(f"INSERT INTO gld_kds_score_tbl {calc_query}")

# 엔진 점화! 🚀
statement_set.execute()
