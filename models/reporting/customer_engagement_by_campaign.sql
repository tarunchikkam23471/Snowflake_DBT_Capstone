
SELECT

    dc.CampaignID,
    dc.Campaign_Name,
    New_Customers_Acquired,
    Repeat_Purchase_Rate

FROM {{ ref('fact_marketingperformance') }} fmp
JOIN {{ ref('dim_marketingcampaign') }} dc
    ON fmp.CampaignKey = dc.CampaignKey
