WITH source_data AS (
SELECT *
FROM {{ ref('stg_campaign_data') }}
),

dim_campaign AS (
SELECT

{{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS CampaignKey,
campaign_id AS CampaignID,
campaign_name,
campaign_type,
audience_segment AS Target_Audience_Segment,
budget AS Budget,
campaign_duration_days AS Duration,
roi_calculation AS Actual_ROI,
expected_roi AS Expected_ROI,
start_date AS Start_Date,
end_date AS End_Date

FROM source_data
)

SELECT *
FROM dim_campaign