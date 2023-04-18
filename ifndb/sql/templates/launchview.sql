CREATE OR REPLACE VIEW pollster_health_status_{{year}} AS 
 SELECT w.id AS pollster_results_weekly_id, 
        CASE true
            WHEN w."Q1_0" THEN 'NO-SYMPTOMS'::text
            WHEN (w."Q5" = 0 OR w."Q6b" = 0) AND (w."Q1_1" OR w."Q1_2" OR w."Q6d" = 3 OR w."Q6d" = 4 OR w."Q6d" = 5 OR w."Q1_11" OR w."Q1_8" OR w."Q1_9") AND (w."Q1_5" OR w."Q1_6" OR w."Q1_7") THEN 'ILI'::text
            WHEN NOT w."Q1_1" AND NOT w."Q1_2" AND (w."Q6d" = 0 OR w."Q6d" IS NULL) AND (w."Q1_3" OR w."Q1_4" OR w."Q1_14") AND w."Q11" = 2 AND (
            CASE true
                WHEN w."Q1_17" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_15" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_16" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_18" THEN 1
                ELSE 0
            END) >= 2 THEN 'ALLERGY-or-HAY-FEVER-and-GASTROINTESTINAL'::text
            WHEN NOT w."Q1_1" AND NOT w."Q1_2" AND (w."Q6d" = 0 OR w."Q6d" IS NULL) AND (w."Q1_3" OR w."Q1_4" OR w."Q1_14") AND w."Q11" = 2 THEN 'ALLERGY-or-HAY-FEVER'::text
            WHEN (
            CASE true
                WHEN w."Q1_3" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_4" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_6" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_5" THEN 1
                ELSE 0
            END) >= 2 AND (
            CASE true
                WHEN w."Q1_17" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_15" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_16" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_18" THEN 1
                ELSE 0
            END) >= 2 THEN 'COMMON-COLD-and-GASTROINTESTINAL'::text
            WHEN (
            CASE true
                WHEN w."Q1_3" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_4" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_6" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_5" THEN 1
                ELSE 0
            END) >= 2 THEN 'COMMON-COLD'::text
            WHEN (
            CASE true
                WHEN w."Q1_17" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_15" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_16" THEN 1
                ELSE 0
            END + 
            CASE true
                WHEN w."Q1_18" THEN 1
                ELSE 0
            END) >= 2 THEN 'GASTROINTESTINAL'::text
            ELSE 'NON-SPECIFIC-SYMPTOMS'::text
        END AS status
   FROM pollster_results_weekly_{{year}} w;
