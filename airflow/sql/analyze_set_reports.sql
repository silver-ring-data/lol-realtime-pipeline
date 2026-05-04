-- 파이썬 f-string의 {match_id}가 Airflow Jinja의 {{ params.match_id }}로 바뀜!
INSERT INTO {{ params.ATHENA_DB }}.btch_gld_match_reports
WITH game_sum AS (
    SELECT 
        match_id, 
        CAST(e.team_id AS STRING) as team, 
        COUNT(CASE WHEN e.event_type = 'CHAMPION_KILL' THEN 1 END) as total_kills, 
        0 as total_gold
    FROM {{ params.ATHENA_DB }}.btch_slv_game_table
    CROSS JOIN UNNEST(events) AS t(e)
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY match_id, e.team_id
),
chat_sum AS (
    SELECT 
        '{{ params.match_id }}' as match_id,
        count(*) as chat_count
    FROM {{ params.ATHENA_DB }}.btch_slv_chat_table
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY 1
)
SELECT 
    g.match_id, g.team, g.total_kills, g.total_gold, c.chat_count
FROM game_sum g
JOIN chat_sum c ON g.match_id = c.match_id;