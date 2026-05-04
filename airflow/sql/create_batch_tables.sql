-- (1) 배치용 실버 게임 테이블
CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.ATHENA_DB }}.btch_slv_game_table (
    match_id STRING,
    ts BIGINT,
    events ARRAY<STRUCT<
        timestamp: BIGINT,
        event_type: STRING,
        killer_id: INT,
        victim_id: STRING,
        team_id: INT
    >>
)
PARTITIONED BY (p_match_id STRING)
STORED AS PARQUET
LOCATION 's3://{{ params.BUCKET_NAME }}/silver/game/';

-- (2) 배치용 실버 채팅 테이블
CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.ATHENA_DB }}.btch_slv_chat_table (
    nickname STRING,
    content STRING,
    platform STRING,
    ts BIGINT
)
PARTITIONED BY (p_match_id STRING)
STORED AS PARQUET
LOCATION 's3://{{ params.BUCKET_NAME }}/silver/chat/';

-- (3) 배치용 골드 리포트 테이블
CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.ATHENA_DB }}.btch_gld_match_reports (
    match_id STRING, 
    team STRING, 
    total_kills INT, 
    total_gold INT, 
    chat_count INT
)
STORED AS PARQUET
LOCATION 's3://{{ params.BUCKET_NAME }}/gold/match_reports/';