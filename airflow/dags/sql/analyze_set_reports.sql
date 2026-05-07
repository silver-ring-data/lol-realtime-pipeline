INSERT INTO {{ params.DB }}.{{ params.REPORT_TABLE_NAME }}
WITH event_stats AS (
    SELECT 
        p_match_id as match_id, 
        CAST(e.team_id AS VARCHAR) as team, 
        COUNT(CASE WHEN e.event_type = 'CHAMPION_KILL' THEN 1 END) as total_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%BARON%' THEN 1 END) as baron_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%DRAGON%' THEN 1 END) as dragon_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%VOID GRUB%' THEN 1 END) as void_grub_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%HERALD%' THEN 1 END) as herald_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%PLATE%' THEN 1 END) as tower_plate_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND (e.victim_id LIKE '%TOWER%' OR e.victim_id LIKE '%TURRET%') THEN 1 END) as turret_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%INHIBITOR%' THEN 1 END) as inhibitor_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%DESTROYED%' THEN 1 END) as nexus_destroyed
    FROM {{ params.DB }}.{{ params.GAME_TABLE_NAME }}
    CROSS JOIN UNNEST(events) AS t(e)
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY p_match_id, e.team_id
),
gold_stats AS (
    SELECT 
        p_match_id as match_id,
        CASE WHEN CAST(player_id AS INTEGER) <= 5 THEN '100' ELSE '200' END as team,
        SUM(max_gold) as total_gold
    FROM (
        SELECT 
            match_id,
            player_id,
            MAX(stats.total_gold) as max_gold
        FROM {{ params.DB }}.{{ params.GAME_TABLE_NAME }}
        CROSS JOIN UNNEST(participant_frames) AS t(player_id, stats)
        WHERE p_match_id = '{{ params.match_id }}'
        GROUP BY match_id, player_id
    )
    GROUP BY match_id, CASE WHEN CAST(player_id AS INTEGER) <= 5 THEN '100' ELSE '200' END
),
chat_sum AS (
    SELECT 
        p_match_id as match_id,
        count(*) as chat_count
    FROM {{ params.DB }}.{{ params.CHAT_TABLE_NAME }}
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY 1
)
SELECT 
    e.match_id, 
    e.team, 
    e.total_kills, 
    e.baron_kills, 
    e.dragon_kills,
    e.void_grub_kills, 
    e.herald_kills, 
    e.tower_plate_kills, 
    e.turret_kills,
    e.inhibitor_kills, 
    e.nexus_destroyed, 
    COALESCE(g.total_gold, 0) as total_gold, 
    COALESCE(c.chat_count, 0) as chat_count
FROM event_stats e
LEFT JOIN gold_stats g ON e.match_id = g.match_id AND e.team = g.team
LEFT JOIN chat_sum c ON e.match_id = c.match_id;