
SELECT

    dd.Year,
    dd.Month,
    fs.Region,
    SUM(fs.Total_Sales_Amount) AS Total_Sales,
    SUM(fs.Quantity_Sold) AS Total_Units_Sold

FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_date') }} dd
    ON fs.DateKey = dd.DateKey
GROUP BY
    dd.Year,
    dd.Month,
    fs.Region
ORDER BY
    dd.Year,
    dd.Month