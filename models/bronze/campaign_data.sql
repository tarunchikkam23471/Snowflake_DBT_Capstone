{{ config(materialized='table') }}

SELECT

    campaign.value:campaign_id::STRING         AS campaign_id,
    campaign.value:campaign_name::STRING       AS campaign_name,
    campaign.value:campaign_type::STRING       AS campaign_type,
    campaign.value:channel::STRING             AS channel,
    campaign.value:description::STRING         AS description,
    campaign.value:start_date::STRING          AS start_date,
    campaign.value:end_date::STRING            AS end_date,
    campaign.value:last_modified_date::STRING  AS last_modified_date,
    campaign.value:target_audience::STRING     AS target_audience,
    campaign.value:budget::STRING              AS budget,
    campaign.value:total_cost::STRING          AS total_cost,
    campaign.value:total_revenue::STRING       AS total_revenue,
    campaign.value:roi_calculation::STRING     AS roi_calculation,

    CURRENT_TIMESTAMP()                        AS load_timestamp

FROM {{ source('bronze_ext','ext_campaign_data') }},
LATERAL FLATTEN(input => VALUE:campaigns_data) campaign