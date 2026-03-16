
SELECT

    dp.ProductID,
    dp.Product_Name,
    dp.Category,
    dp.Brand,

    SUM(fs.Quantity_Sold) AS Total_Quantity_Sold,

    SUM(fs.Total_Sales_Amount) AS Total_Revenue

FROM {{ ref('fact_sales') }} fs

JOIN {{ ref('dim_product') }} dp
    ON fs.ProductKey = dp.ProductKey

GROUP BY
    dp.ProductID,
    dp.Product_Name,
    dp.Category,
    dp.Brand

ORDER BY
    Total_Revenue DESC