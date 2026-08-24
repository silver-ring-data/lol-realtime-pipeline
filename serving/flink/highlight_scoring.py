%flink.pyflink

# 실시간 하이라이트 점수 산출 (Managed Flink Studio 노트북)
#
# Kinesis Bronze 스트림(게임/채팅)을 읽어 슬라이딩 윈도우로 하이라이트 점수를
# 계산하고, 서빙용 채팅과 함께 Gold 스트림 및 S3로 내보낸다.
#
# 점수 산출 방식
#   경기 지표만으로는 "지표에 안 남는 슈퍼플레이"를 놓치고, 채팅만 보면 무관한
#   잡담에 반응한다. 두 신호를 가중 합산하여 양쪽의 약점을 보완한다.
#       final_score = event_score * 0.6 + chat_score * 0.4
#
# 윈도우 설계
#   HOP(슬라이딩) 윈도우를 사용해 5초 구간을 2초마다 갱신한다. 텀블링 윈도우는
#   하이라이트가 경계에 걸치면 점수가 두 구간으로 쪼개져 정점이 낮아지므로,
#   구간을 겹쳐 정점을 안정적으로 포착한다.
#
# 이벤트 시각 처리
#   세 프로듀서의 도착 순서가 어긋날 수 있어 이벤트 시각 기준으로 처리하고,
#   5초 워터마크로 지연 도착분을 흡수한다.
#
# 리소스 이름은 config/config.yaml과 일치해야 한다.

st_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "5000 ms")
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")

# ==========================================
# 1. 파라미터
# ==========================================

WINDOW_SIZE = 5      # 윈도우 크기 (초)
SLIDE_STEP = 2       # 슬라이딩 간격 (초)

CHAT_SAMPLING_RATE = 0.8    # 일반 채팅 노출 확률
EVENT_SCORE_RATIO = 0.6     # 최종 점수에서 경기 이벤트 반영 비율
CHAT_SCORE_RATIO = 0.4      # 최종 점수에서 채팅 화력 반영 비율

# 대시보드 노출 시 마스킹할 비속어 패턴
BAD_WORDS = '슼갈|젠첩|즙|쵸독|티준딱|개패자|쓰레기|폐기물|배신자|쪽팔린다|수치스럽다|망신|ㅆㅂ|ㅅㅂ|시[ㅣ]+바|ㅆㄱㅈ|ㅂㅅ|ㅈㄹ'
# 하이라이트 가능성이 높은 채팅을 우선 노출하기 위한 키워드
HL_WORDS = '역전|우승|나이스|ㄴㅇㅅ|가즈아|이겼다|비상|한타|솔킬|크랙|슈퍼플레이|이니시|바론|드래곤|장로|용스틸|용한타|유충|전령|골드차이|밸류'

# 경기 이벤트별 가중치. 경기 흐름에 미치는 영향이 클수록 높은 점수를 부여한다.
EVENT_WEIGHTS = {
    'CHAMPION_KILL': 20, 'TOWER_PLATE': 10, 'TURRET': 15,
    'INHIBITOR': 35, 'NEXUS_DESTROYED': 100, 'DRAGON': 25,
    'BARON_NASHOR': 45, 'RIFT_HERALD': 20, 'VOID_GRUB': 7
}

# 윈도우 내 채팅 수를 100점 만점 점수로 환산하는 구간 (임계값, 점수)
CHAT_LEVELS = [
    (50, 100), (45, 90), (40, 80), (35, 70), (30, 60),
    (25, 50), (20, 40), (15, 30), (10, 20), (5, 10)
]

chat_case = "\n".join([f"WHEN SUM(c_count) >= {count} THEN {score}" for count, score in CHAT_LEVELS])

# ==========================================
# 2. 소스 및 싱크 테이블 정의
# ==========================================

# (1) 게임 데이터 소스 (Bronze)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS spd_brz_game_tbl (
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

# (2) 채팅 데이터 소스 (Bronze)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS spd_brz_chat_tbl (
        `match_id` STRING,
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

# (3) S3 Gold 싱크 (배치 분석과의 대조 및 이력 보관용)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS spd_gld_s3_score_tbl (
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

# (4) Kinesis Gold 싱크 - 하이라이트 점수 (대시보드 서빙 경로)
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS spd_gld_kds_score_tbl (
        match_id STRING, window_start TIMESTAMP(3),
        event_score DOUBLE, chat_score INT, final_score DOUBLE
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'de-ai-05-lol-dev-an2-kds-gld-score',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json'
    )
""")

# (5) Kinesis Gold 싱크 - 서빙용 채팅
st_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS spd_gld_kds_chat_tbl (
        match_id STRING, nickname STRING, content STRING, platform STRING, ts TIMESTAMP(3), priority INT
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'de-ai-05-lol-dev-an2-kds-gld-chat',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json'
    )
""")

# ==========================================
# 3. 통합 연산 로직
# ==========================================

# 채팅 건수와 이벤트 점수를 한 스트림으로 합쳐 단일 윈도우에서 집계한다.
# 게임 하트비트(2번)를 함께 넣는 이유: 채팅이 전혀 없는 구간에서도 워터마크가
# 진행되어야 윈도우가 정상적으로 닫히기 때문이다.
unified_stream = f"""
    -- 1. 채팅 데이터 (건수 집계용)
    SELECT match_id, row_time as ts, CAST(0 AS DOUBLE) as e_score, 1 as c_count
    FROM spd_brz_chat_tbl
    WHERE nickname NOT IN ('@nightbot', 'System')
      AND content NOT LIKE '%[warning]%'

    UNION ALL

    -- 2. 게임 하트비트 (윈도우 진행을 보장하기 위한 빈 레코드)
    SELECT match_id, row_time as ts, CAST(0 AS DOUBLE) as e_score, 0 as c_count
    FROM spd_brz_game_tbl

    UNION ALL

    -- 3. 게임 이벤트 (가중치 점수 부여)
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
    FROM spd_brz_game_tbl
    CROSS JOIN UNNEST(events) AS E (e_ts, event_type, killer_id, assisting_participant_ids, victim_id, team_id)
"""

calc_query = f"""
    SELECT
        match_id,
        HOP_START(ts, INTERVAL '{SLIDE_STEP}' SECOND, INTERVAL '{WINDOW_SIZE}' SECOND) as window_start,
        SUM(e_score) as event_score,
        CASE {chat_case} ELSE 0 END as chat_score,
        (SUM(e_score) * {EVENT_SCORE_RATIO} + (CASE {chat_case} ELSE 0 END) * {CHAT_SCORE_RATIO}) as final_score
    FROM ({unified_stream})
    GROUP BY match_id, HOP(ts, INTERVAL '{SLIDE_STEP}' SECOND, INTERVAL '{WINDOW_SIZE}' SECOND)
"""

# ==========================================
# 4. 파이프라인 실행 (StatementSet)
# ==========================================

# 세 INSERT를 하나의 StatementSet으로 묶어 소스를 한 번만 읽도록 한다.
# 개별 실행하면 같은 Kinesis 스트림을 세 번 읽어 비용과 부하가 늘어난다.
statement_set = st_env.create_statement_set()

# [Track A] 서빙용 채팅 가공 -> Kinesis Gold
#   - 30자 초과 메시지는 말줄임 처리
#   - 비속어 마스킹
#   - 하이라이트 키워드 포함 채팅은 priority=1로 우선 노출
#   - 그 외 일반 채팅은 샘플링하여 화면 과부하를 방지
statement_set.add_insert_sql(f"""
    INSERT INTO spd_gld_kds_chat_tbl
    SELECT
        match_id,
        nickname,
        REGEXP_REPLACE(
            CASE
                WHEN CHAR_LENGTH(content) >= 30 THEN SUBSTR(content, 1, 27) || '...'
                ELSE content
            END,
            '{BAD_WORDS}',
            '****'
        ) as content,
        platform,
        row_time as ts,
        CASE
            WHEN REGEXP_REPLACE(content, '{HL_WORDS}', '') <> content THEN 1
            ELSE 0
        END as priority
    FROM spd_brz_chat_tbl
    WHERE
        nickname NOT IN ('@nightbot', 'System')
        AND content NOT LIKE '!%'
        AND content NOT LIKE '.%'
        AND content NOT LIKE '/%'
        AND (
            REGEXP_REPLACE(content, '{HL_WORDS}', '') <> content
            OR RAND() < {CHAT_SAMPLING_RATE}
        )
""")

# [Track B] 하이라이트 점수 -> S3(이력)와 Kinesis(서빙)로 동시 전송
statement_set.add_insert_sql(f"INSERT INTO spd_gld_s3_score_tbl {calc_query}")
statement_set.add_insert_sql(f"INSERT INTO spd_gld_kds_score_tbl {calc_query}")

statement_set.execute()
