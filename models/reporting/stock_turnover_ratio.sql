SELECT

    products.Product_Name,
    stores.Store_Name,
    dates.Full_Date,
    inventory.stockturnoverratio
    
FROM {{ ref('fact_inventory') }} inventory
JOIN {{ ref('dim_product') }} products
ON inventory.ProductKey = products.ProductKey
JOIN {{ ref('dim_date') }} dates
ON inventory.DateKey = dates.DateKey
JOIN {{ ref('dim_store') }} stores
ON inventory.StoreKey = stores.StoreKey
 