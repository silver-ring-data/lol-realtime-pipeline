SELECT 
    '{{ params.match_key }}' as series_id,
    team,
    sum(total_kills) as series_total_kills,
    sum(total_gold) as series_total_gold,
    sum(chat_count) as series_total_chats,
    count(distinct match_id) as total_games_played
FROM {{ params.ATHENA_DB }}.btch_gld_match_reports
WHERE match_id IN ({{ params.target_match_ids_str }})
GROUP BY team;