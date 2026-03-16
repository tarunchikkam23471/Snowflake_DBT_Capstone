SELECT

    MAX(customers.Full_Name) AS CustomerName,
    MAX(customers.Segment) AS AgeSegment,
    MAX(customers.Income_Bracket) AS IncomeBracket,
    COUNT(DISTINCT CASE
            WHEN sales.Sales_Channel = 'In-Store' THEN OrderID
    END) AS OfflinePurchases,
    COUNT(DISTINCT CASE
        WHEN sales.Sales_Channel = 'Online' THEN OrderID
    END) AS OnlinePurchases
    
FROM {{ ref('dim_customer') }} customers
JOIN {{ ref('fact_sales') }} sales
ON customers.CustomerKey = sales.CustomerKey
GROUP BY customers.CustomerKey
 