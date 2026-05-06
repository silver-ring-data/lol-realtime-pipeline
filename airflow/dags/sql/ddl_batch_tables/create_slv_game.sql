CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.DB }}.{{ params.TABLE_NAME }} (
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
LOCATION 's3://{{ params.BUCKET }}/silver/game/';