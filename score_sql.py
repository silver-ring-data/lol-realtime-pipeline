%flink.pyflink

# 1. 체크포인트 및 환경 설정
st_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "60000")

# ==========================================
# ⚙️ 1. 하이라이트 탐지 설정 (은비가 고치는 곳)
# ==========================================
WINDOW_SIZE = 10
SLIDE_STEP = 3
CHAT_LOOKBACK = 2
CHAT_LOOKAHEAD = 8

EVENT_WEIGHTS = {
    'CHAMPION_KILL': 20,
    'TOWER_BUILDING_KILL': 15,
    'DRAGON_KILL': 25,
    'BARON_KILL': 40
}

CHAT_LEVELS = [
    (100, 100),
    (51, 50),
    (21, 20)
]

RATIO_EVENT = 0.6
RATIO_CHAT = 0.4

# 매치별 싱크 보정 (ms)
MATCH_OFFSETS = {
    'T1_BLG_G4': 55000,
    'T1_BLG_G5': 3182000
}

# ==========================================
# ⚙️ 2. SQL 구문 자동 생성
# ==========================================
event_case = "\n".join([f"WHEN G.event_type = '{k}' THEN {v}" for k, v in EVENT_WEIGHTS.items()])
chat_case = "\n".join([f"WHEN COUNT(C.content) >= {count} THEN {score}" for count, score in CHAT_LEVELS])
# 싱크 보정을 위한 오프셋 구문
offset_logic = "\n".join([f"WHEN match_id = '{k}' THEN {v}" for k, v in MATCH_OFFSETS.items()])

# ==========================================
# 🚀 3. Flink SQL 실행 (최적화 버전)
# ==========================================

# 메인 계산 로직 (서브쿼리용)
# 여기서 row_time에 오프셋을 더해 '보정된 시간'을 기준으로 조인과 윈도우를 잡아야 해!
calculation_logic = f"""
    SELECT 
        G.match_id,
        -- 게임 시간 보정 (오프셋 반영)
        HOP_START(G.row_time, INTERVAL '{SLIDE_STEP}' SECOND, INTERVAL '{WINDOW_SIZE}' SECOND) as window_start,
        COALESCE(SUM(CASE {event_case} ELSE 0 END), 0) as event_score,
        CASE {chat_case} ELSE 0 END as chat_score
    FROM slv_game_strm G
    LEFT JOIN slv_chat_strm C ON 
        C.row_time BETWEEN G.row_time - INTERVAL '{CHAT_LOOKBACK}' SECOND 
                       AND G.row_time + INTERVAL '{CHAT_LOOKAHEAD}' SECOND
    GROUP BY 
        G.match_id, 
        HOP(G.row_time, INTERVAL '{SLIDE_STEP}' SECOND, INTERVAL '{WINDOW_SIZE}' SECOND)
"""

# 최종 쿼리 (비율 계산 및 타입 변환)
final_query = f"""
    SELECT 
        match_id,
        window_start,
        CAST(event_score AS DOUBLE) as event_score,
        CAST(chat_score AS INT) as chat_score,
        (event_score * {RATIO_EVENT} + chat_score * {RATIO_CHAT}) as final_score
    FROM ({calculation_logic}) AS T
"""

# 1. 골드 레이어 싱크 테이블 생성
st_env.execute_sql(f"""
    CREATE TABLE IF NOT EXISTS gold_highlight_sink (
        match_id STRING,
        window_start TIMESTAMP(3),
        event_score DOUBLE,
        chat_score INT,
        final_score DOUBLE
    ) WITH (
        'connector' = 'kinesis',
        'stream' = 'lol-highlighter-dev-an2-kds-gld-score',
        'aws.region' = 'ap-northeast-2',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

# 2. 데이터 주입 (INSERT INTO 한 번만 실행!)
st_env.execute_sql(f"INSERT INTO gold_highlight_sink {final_query}")
