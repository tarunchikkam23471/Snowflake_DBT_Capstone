
SELECT

    de.Role,
    COUNT(DISTINCT fs.EmployeeKey) AS Total_Employees,
    SUM(fs.Total_Sales_Amount) AS Total_Sales,
    SUM(fs.Profit_Amount) AS Total_Profit,
    SUM(fs.Quantity_Sold) AS Total_Items_Sold

FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_employee') }} de
    ON fs.EmployeeKey = de.EmployeeKey
GROUP BY
    de.Role
ORDER BY
    Total_Sales DESC