
WITH campaign_sales AS (
SELECT

    dc.CampaignKey,
    fs.CustomerKey,
    fs.Total_Sales_Amount,
    dd.full_date

FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_date') }} dd
    ON dd.DateKey = fs.DateKey
JOIN {{ ref('dim_marketingcampaign') }} dc
    ON fs.CampaignKey = dc.CampaignKey
    AND dd.full_date BETWEEN dc.Start_Date AND dc.End_Date
),

total_sales AS (
SELECT

    CampaignKey,
    SUM(Total_Sales_Amount) AS TotalSalesInfluenced

FROM campaign_sales
GROUP BY CampaignKey
),

pre_campaign_buyers AS (
SELECT DISTINCT

    fs.CustomerKey,
    dc.CampaignKey

FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_date') }} dd
    ON dd.DateKey = fs.DateKey
JOIN {{ ref('dim_marketingcampaign') }} dc
    ON fs.CampaignKey = dc.CampaignKey
    AND dd.full_date < dc.Start_Date
),

new_customers AS (
SELECT

    cs.CampaignKey,
    COUNT(DISTINCT cs.CustomerKey) AS NewCustomersAcquired

FROM campaign_sales cs
LEFT JOIN pre_campaign_buyers pcb
    ON pcb.CustomerKey = cs.CustomerKey
    AND pcb.CampaignKey = cs.CampaignKey
WHERE pcb.CustomerKey IS NULL
GROUP BY cs.CampaignKey
),

customer_purchase_counts AS (
SELECT

    CampaignKey,
    CustomerKey,
    COUNT(*) AS PurchaseCount

FROM campaign_sales
GROUP BY CampaignKey, CustomerKey
),

repeat_rate AS (
SELECT

    CampaignKey,
    COUNT(DISTINCT CASE
        WHEN PurchaseCount > 1 THEN CustomerKey
    END) AS RepeatBuyers,
    COUNT(DISTINCT CASE
        WHEN PurchaseCount = 1 THEN CustomerKey
    END) AS FirstTimeBuyers,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN PurchaseCount > 1 THEN CustomerKey
        END) * 100.0
        / NULLIF(COUNT(DISTINCT CustomerKey),0),
        2
    ) AS RepeatPurchaseRate_Pct

FROM customer_purchase_counts
GROUP BY CampaignKey
),

final AS (
SELECT

    dc.CampaignKey,
    ts.TotalSalesInfluenced AS Total_Sales_Influenced_By_Campaign,
    nc.NewCustomersAcquired AS New_Customers_Acquired,
    rr.RepeatPurchaseRate_Pct AS Repeat_Purchase_Rate,
    dc.Actual_ROI AS Return_On_Investment_For_Campaign

FROM {{ ref('dim_marketingcampaign') }} dc
LEFT JOIN total_sales ts
    ON ts.CampaignKey = dc.CampaignKey
LEFT JOIN new_customers nc
    ON nc.CampaignKey = dc.CampaignKey
LEFT JOIN repeat_rate rr
    ON rr.CampaignKey = dc.CampaignKey
)

SELECT *
FROM final