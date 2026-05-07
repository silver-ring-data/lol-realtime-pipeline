SELECT 
    '{{ params.match_key }}' as series_id,
    team,
    SUM(total_kills) as series_total_kills,
    SUM(baron_kills) as series_total_barons,
    SUM(dragon_kills) as series_total_dragons,
    SUM(turret_kills) as series_total_turrets,
    SUM(total_gold) as series_total_gold,
    SUM(chat_count) as series_total_chats,
    ROUND(AVG(total_kills), 2) as avg_kills_per_set
FROM {{ params.ATHENA_DB }}.{{ params.REPORT_TABLE_NAME }}
WHERE match_id IN ({{ params.target_match_ids_str }}) 
GROUP BY team;