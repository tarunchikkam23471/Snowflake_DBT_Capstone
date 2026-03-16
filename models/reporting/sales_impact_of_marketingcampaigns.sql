
SELECT

    dc.CampaignID,
    dc.Campaign_Name,
    fmp.Total_Sales_Influenced_By_Campaign AS Total_Sales_Influenced

FROM {{ ref('fact_marketingperformance') }} fmp
JOIN {{ ref('dim_marketingcampaign') }} dc
    ON fmp.CampaignKey = dc.CampaignKey
ORDER BY
    Total_Sales_Influenced DESC