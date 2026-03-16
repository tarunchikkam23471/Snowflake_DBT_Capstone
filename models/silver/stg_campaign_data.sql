WITH source_data AS (
SELECT *
FROM {{ ref('snp_campaign_data') }}
WHERE dbt_valid_to IS NULL
),

cleaned AS (
SELECT

TRIM(campaign_id) AS campaign_id,
INITCAP(TRIM(campaign_name)) AS campaign_name,
INITCAP(TRIM(campaign_type)) AS campaign_type,
INITCAP(TRIM(channel)) AS channel,
TRIM(description) AS description,
TRY_TO_TIMESTAMP(start_date) AS start_date,
TRY_TO_TIMESTAMP(end_date) AS end_date,
TRY_TO_DATE(last_modified_date) AS last_modified_date,
INITCAP(TRIM(target_audience)) AS target_audience,
CASE
WHEN LOWER(target_audience) LIKE '%students%' THEN 'Students'
WHEN LOWER(target_audience) LIKE '%families%' THEN 'Families'
WHEN LOWER(target_audience) LIKE '%professionals%' THEN 'Professionals'
WHEN LOWER(target_audience) LIKE '%seniors%' THEN 'Seniors'
ELSE 'Other'
END AS audience_segment,
TRIM(SPLIT_PART(target_audience, ',', 2)) AS age_range,
INITCAP(TRIM(SPLIT_PART(target_audience, ',', 3))) AS location_type,
TRY_TO_NUMBER(REGEXP_REPLACE(TRIM(budget),'[^0-9.]','')) AS budget,
TRY_TO_NUMBER(REGEXP_REPLACE(TRIM(total_cost),'[^0-9.]','')) AS total_cost,
TRY_TO_NUMBER(REGEXP_REPLACE(TRIM(total_revenue),'[^0-9.]','')) AS total_revenue,
roi_calculation::NUMERIC(5,2) AS roi_calculation,
dbt_valid_from,
dbt_valid_to,
dbt_updated_at

FROM source_data
),

campaign_metrics AS (
SELECT

*,
DATEDIFF(day,start_date,end_date) AS campaign_duration_days,
CASE
WHEN total_cost > 0
THEN (total_revenue - total_cost) / total_cost
ELSE NULL
END AS expected_roi,
CASE
WHEN total_cost > 0
AND ABS(roi_calculation - ((total_revenue - total_cost) / total_cost)) < 0.01
THEN TRUE
ELSE FALSE
END AS roi_validation_flag

FROM cleaned
)

SELECT *
FROM campaign_metrics