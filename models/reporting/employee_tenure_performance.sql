SELECT
    tenure,
    AVG(Target_Achievement_Percentage) AS Avg_Achievement_Percentage,
    AVG(Performance_Metrics) AS Avg_Performance_Rating
FROM {{ ref('dim_employee') }}
GROUP BY tenure