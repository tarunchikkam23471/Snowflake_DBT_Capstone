{{ config(materialized='table', schema = 'silver') }}
 
WITH years AS (
    SELECT
        YEAR(DATEADD('year', SEQ4(), '2020-01-01'::DATE)) AS yr
    FROM TABLE(GENERATOR(rowcount => 11))
),
fixed_holidays AS (
    SELECT yr, DATE_FROM_PARTS(yr, 1,  1)  AS date_day, 'New Year''s Day'  AS holiday_name FROM years UNION ALL
    SELECT yr, DATE_FROM_PARTS(yr, 7,  4),               'Independence Day'                 FROM years UNION ALL
    SELECT yr, DATE_FROM_PARTS(yr, 11, 11),              'Veterans Day'                     FROM years UNION ALL
    SELECT yr, DATE_FROM_PARTS(yr, 12, 25),              'Christmas Day'                    FROM years UNION ALL
    SELECT yr, DATE_FROM_PARTS(yr, 12, 31),              'New Year''s Eve'                  FROM years
),
 
floating_holidays AS (
    SELECT
        yr,
        DATEADD('day', 21,
            DATEADD('day',
                (5 - DAYOFWEEK(DATE_FROM_PARTS(yr, 11, 1)) + 7) % 7,  -- days until first Thursday
                DATE_FROM_PARTS(yr, 11, 1)
            )
        ) AS date_day,
        'Thanksgiving' AS holiday_name
    FROM years
 
    UNION ALL
 
    SELECT
        yr,
        DATEADD('day', 14,
            DATEADD('day',
                (1 - DAYOFWEEK(DATE_FROM_PARTS(yr, 1, 1)) + 7) % 7,  -- days until first Monday
                DATE_FROM_PARTS(yr, 1, 1)
            )
        ) AS date_day,
        'MLK Day' AS holiday_name
    FROM years
 
    UNION ALL
 
    SELECT
        yr,
        DATEADD('day',
            (1 - DAYOFWEEK(DATE_FROM_PARTS(yr, 9, 1)) + 7) % 7,
            DATE_FROM_PARTS(yr, 9, 1)
        ) AS date_day,
        'Labor Day' AS holiday_name
    FROM years
),
 
final AS (
    SELECT date_day, holiday_name FROM fixed_holidays
    UNION ALL
    SELECT date_day, holiday_name FROM floating_holidays
)
 
SELECT * FROM final
ORDER BY date_day