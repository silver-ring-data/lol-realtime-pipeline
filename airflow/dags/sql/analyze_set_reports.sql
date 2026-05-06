-- sql/analyze_set_reports.sql

INSERT INTO {{ params.DB }}.{{ params.REPORT_TABLE_NAME }}
WITH game_sum AS (
    SELECT 
        match_id, 
        CAST(e.team_id AS STRING) as team, 
        
        -- ⚔️ 챔피언 킬
        COUNT(CASE WHEN e.event_type = 'CHAMPION_KILL' THEN 1 END) as total_kills,
        
        -- 🐉 에픽 몬스터 (오브젝트)
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%BARON%' THEN 1 END) as baron_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%DRAGON%' THEN 1 END) as dragon_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%VOID GRUB%' THEN 1 END) as void_grub_kills,
        COUNT(CASE WHEN e.event_type = 'ELITE_MONSTER' AND e.victim_id LIKE '%HERALD%' THEN 1 END) as herald_kills,
        
        -- 🏰 철거 (포탑, 억제기 등)
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%PLATE%' THEN 1 END) as tower_plate_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND (e.victim_id LIKE '%TOWER%' OR e.victim_id LIKE '%TURRET%') THEN 1 END) as turret_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%INHIBITOR%' THEN 1 END) as inhibitor_kills,
        COUNT(CASE WHEN e.event_type = 'BUILDING_KILL' AND e.victim_id LIKE '%DESTROYED%' THEN 1 END) as nexus_destroyed,
        
        0 as total_gold -- (골드는 추후 업데이트)
    FROM {{ params.DB }}.{{ params.GAME_TABLE_NAME }}
    CROSS JOIN UNNEST(events) AS t(e)
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY match_id, e.team_id
),
chat_sum AS (
    SELECT 
        '{{ params.match_id }}' as match_id,
        count(*) as chat_count
    FROM {{ params.DB }}.{{ params.CHAT_TABLE_NAME }}
    WHERE p_match_id = '{{ params.match_id }}'
    GROUP BY 1
)
SELECT 
    g.match_id, 
    g.team, 
    g.total_kills,
    g.baron_kills,
    g.dragon_kills,
    g.void_grub_kills,
    g.herald_kills,
    g.tower_plate_kills,
    g.turret_kills,
    g.inhibitor_kills,
    g.nexus_destroyed,
    g.total_gold, 
    c.chat_count
FROM game_sum g
JOIN chat_sum c ON g.match_id = c.match_id;