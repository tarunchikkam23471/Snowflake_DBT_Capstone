
SELECT

    dc.CustomerID,
    dc.Full_Name,
    dc.Segment,

    COUNT(DISTINCT fs.OrderID) AS Total_Orders,

    SUM(fs.Total_Sales_Amount) AS Lifetime_Revenue,

    SUM(fs.Profit_Amount) AS Lifetime_Profit,

    AVG(fs.Total_Sales_Amount) AS Avg_Order_Value

FROM {{ ref('fact_sales') }} fs

JOIN {{ ref('dim_customer') }} dc
    ON fs.CustomerKey = dc.CustomerKey

GROUP BY
    dc.CustomerID,
    dc.Full_Name,
    dc.Segment

ORDER BY
    Lifetime_Revenue DESC