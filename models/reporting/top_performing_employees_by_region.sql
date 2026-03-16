WITH rankingPerformance AS (
    SELECT

        sales.Region,
        MAX(employees.Full_Name),
        SUM(Total_Sales_Amount) AS SalesByEmployee,
        RANK() OVER (PARTITION BY sales.Region ORDER BY SUM(Total_Sales_Amount)) AS rnk
        
    FROM {{ ref('fact_sales') }} sales
    JOIN {{ ref('dim_employee') }} employees
    ON sales.EmployeeKey = employees.EmployeeKey
    GROUP BY sales.Region, employees.EmployeeKey
)
SELECT *
FROM rankingPerformance
WHERE rnk <= 5