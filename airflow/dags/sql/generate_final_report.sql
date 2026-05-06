-- sql/generate_final_report.sql 예시
INSERT INTO {{ params.REPORT_TABLE_NAME }}
SELECT 
    '{{ params.match_key }}' as match_id, -- 시리즈 전체를 묶는 키
    team,
    SUM(total_kills) as series_total_kills,
    SUM(total_gold) as series_total_gold,
    SUM(chat_count) as series_total_chats,
    AVG(total_kills) as avg_kills_per_set
FROM {{ params.ATHENA_DB }}.{{ params.REPORT_TABLE_NAME }}
WHERE match_id IN ({{ params.target_match_ids_str }}) -- 'G4', 'G5' 등의 개별 매치 ID들
GROUP BY team;