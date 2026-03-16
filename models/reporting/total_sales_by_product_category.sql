
SELECT

    dp.Category,
    dp.Subcategory,
    SUM(fs.Total_Sales_Amount) AS Total_Sales

FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_product') }} dp
    ON fs.ProductKey = dp.ProductKey
GROUP BY
    dp.Category,
    dp.Subcategory
ORDER BY
    Total_Sales DESC