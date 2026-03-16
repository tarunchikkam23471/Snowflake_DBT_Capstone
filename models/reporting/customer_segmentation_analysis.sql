
SELECT

    dc.Segment,

    COUNT(DISTINCT fs.CustomerKey) AS Total_Customers,

    SUM(fs.Total_Sales_Amount) AS Total_Revenue,

    SUM(fs.Profit_Amount) AS Total_Profit,

    AVG(fs.Total_Sales_Amount) AS Avg_Sales_Per_Order,

    SUM(fs.Quantity_Sold) AS Total_Items_Purchased

FROM {{ ref('fact_sales') }} fs

JOIN {{ ref('dim_customer') }} dc
    ON fs.CustomerKey = dc.CustomerKey

GROUP BY
    dc.Segment

ORDER BY
    Total_Revenue DESC