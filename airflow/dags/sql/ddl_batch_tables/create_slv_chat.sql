CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.DB }}.{{ params.TABLE_NAME }} (
    nickname STRING,
    content STRING,
    platform STRING,
    ts BIGINT)
PARTITIONED BY (p_match_id STRING)
STORED AS PARQUET
LOCATION 's3://{{ params.BUCKET }}/silver/chat/';