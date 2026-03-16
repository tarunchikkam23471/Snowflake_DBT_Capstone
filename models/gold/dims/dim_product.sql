WITH source_data AS (

SELECT *
FROM {{ ref('stg_product_data') }}

),

dim_product AS (

SELECT

{{ dbt_utils.generate_surrogate_key(['product_id']) }} AS ProductKey,

product_id AS ProductID,

product_name AS Product_Name,

category AS Category,

subcategory AS Subcategory,

brand AS Brand,

color AS Color,

size AS Size,

unit_price AS Unit_Price,

cost_price AS Cost_Price,

{{ dbt_utils.generate_surrogate_key(['supplier_id']) }} AS SupplierKey,

stock_quantity,

reorder_level

FROM source_data

)

SELECT *
FROM dim_product