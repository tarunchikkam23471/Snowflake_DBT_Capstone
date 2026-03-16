
SELECT

    dc.Campaign_Type,

    COUNT(DISTINCT dc.CampaignKey) AS Total_Campaigns,

    SUM(fmp.Total_Sales_Influenced_By_Campaign) AS Total_Sales_Influenced,

    AVG(fmp.Return_On_Investment_For_Campaign) AS Avg_ROI

FROM {{ ref('fact_marketingperformance') }} fmp

JOIN {{ ref('dim_marketingcampaign') }} dc
    ON fmp.CampaignKey = dc.CampaignKey

GROUP BY
    dc.Campaign_Type

ORDER BY
    Avg_ROI DESC