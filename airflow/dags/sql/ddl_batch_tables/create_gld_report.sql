-- sql/ddl_batch_tables/create_gld_report.sql

CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.DB }}.{{ params.TABLE_NAME }} (
    match_id STRING, 
    team STRING, 
    total_kills INT, 
    baron_kills INT,
    dragon_kills INT,
    void_grub_kills INT,
    herald_kills INT,
    tower_plate_kills INT,
    turret_kills INT,
    inhibitor_kills INT,
    nexus_destroyed INT,
    total_gold INT, 
    chat_count INT
)
STORED AS PARQUET
LOCATION 's3://{{ params.BUCKET }}/gold/match_reports/';