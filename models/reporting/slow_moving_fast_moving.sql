
WITH turnover_calc AS (

SELECT

    dp.ProductID,
    dp.Product_Name,
    dp.Category,

    SUM(fi.SoldQuantity) AS Total_Sold,

    AVG(fi.AverageInventory) AS Avg_Inventory,

    CASE
        WHEN AVG(fi.AverageInventory) > 0
        THEN SUM(fi.SoldQuantity) / AVG(fi.AverageInventory)
        ELSE NULL
    END AS TurnoverRatio

FROM {{ ref('fact_inventory') }} fi

JOIN {{ ref('dim_product') }} dp
    ON fi.ProductKey = dp.ProductKey

GROUP BY
    dp.ProductID,
    dp.Product_Name,
    dp.Category

)

SELECT

    ProductID,
    Product_Name,
    Category,
    Total_Sold,
    Avg_Inventory,
    TurnoverRatio,

    CASE
        WHEN TurnoverRatio >= 5 THEN 'Fast Moving'
        WHEN TurnoverRatio BETWEEN 2 AND 5 THEN 'Moderate'
        ELSE 'Slow Moving'
    END AS Product_Movement_Type

FROM turnover_calc