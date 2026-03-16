WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),
us_holidays AS (
    SELECT date_day
    FROM {{ ref('us_holidays') }}
),
transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['d.date_day'])}} AS DateKey,
        d.date_day AS Full_Date,
        YEAR(d.date_day) AS year,
        QUARTER(d.date_day) AS quarter,
        MONTH(d.date_day) AS month,
        WEEK(d.date_day) AS week,
        DAYOFWEEK(d.date_day) AS day_of_week,
        CASE
            WHEN h.date_day IS NOT NULL THEN true
            ELSE false
        END AS Holiday_Flag,
        CASE
            WHEN MONTH(d.date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(d.date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(d.date_day) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(d.date_day) IN (9, 10, 11) THEN 'Fall'
        END AS season,
    FROM date_spine d
    LEFT JOIN us_holidays h ON d.date_day = try_to_date(h.date_day)
)
SELECT *
FROM transformed