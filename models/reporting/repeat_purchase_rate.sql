
WITH customer_orders AS (
SELECT
    CustomerKey,
    COUNT(DISTINCT OrderID) AS Order_Count
FROM {{ ref('fact_sales') }}
GROUP BY CustomerKey
),

summary AS (
SELECT

    COUNT(CASE WHEN Order_Count > 1 THEN 1 END) AS Repeat_Customers,
    COUNT(*) AS Total_Customers

FROM customer_orders
)

SELECT

    Repeat_Customers,
    Total_Customers,
    (Repeat_Customers * 100.0) / NULLIF(Total_Customers,0)
        AS Repeat_Purchase_Rate_Percentage

FROM summary