
WITH source_data AS (
    SELECT *
    FROM {{ ref('snp_product_data') }}
    WHERE dbt_valid_to IS NULL
),

cleaned AS (
SELECT

TRIM(product_id) AS product_id,
INITCAP(TRIM(product_name)) AS product_name,
INITCAP(TRIM(brand)) AS brand,
INITCAP(TRIM(category)) AS category,
INITCAP(TRIM(subcategory)) AS subcategory,
INITCAP(TRIM(product_line)) AS product_line,
INITCAP(TRIM(category)) || ' > ' ||
INITCAP(TRIM(subcategory)) || ' > ' ||
INITCAP(TRIM(product_line)) AS category_hierarchy,
INITCAP(TRIM(color)) AS color,
INITCAP(TRIM(size)) AS size,
TRY_TO_NUMBER(REGEXP_REPLACE(weight,'[^0-9.]','')) AS weight_kg,
TRIM(dimensions) AS dimensions,
COALESCE(TRY_TO_NUMBER(unit_price),0) AS unit_price,
COALESCE(TRY_TO_NUMBER(cost_price),0) AS cost_price,
CASE
    WHEN TRY_TO_NUMBER(unit_price) > 0
    THEN ROUND(
        ((TRY_TO_NUMBER(unit_price) - TRY_TO_NUMBER(cost_price))
        / TRY_TO_NUMBER(unit_price)) * 100,2
    )
    ELSE NULL
END AS profit_margin_percentage,
COALESCE(TRY_TO_NUMBER(stock_quantity),0) AS stock_quantity,
COALESCE(TRY_TO_NUMBER(reorder_level),0) AS reorder_level,
CASE
    WHEN COALESCE(TRY_TO_NUMBER(stock_quantity),0)
         < COALESCE(TRY_TO_NUMBER(reorder_level),0)
    THEN TRUE
    ELSE FALSE
END AS low_stock_flag,
TRIM(supplier_id) AS supplier_id,
TRY_TO_BOOLEAN(is_featured) AS is_featured,
TRY_TO_DATE(launch_date) AS launch_date,
TRY_TO_DATE(last_modified_date) AS last_modified_date,
LOWER(TRIM(warranty_period)) AS warranty_period,
INITCAP(TRIM(short_description)) AS short_description,
TRIM(technical_specs) AS technical_specs,
INITCAP(TRIM(product_name))
|| ' - ' ||
INITCAP(TRIM(short_description))
|| ' - ' ||
TRIM(technical_specs) AS product_full_description,
dbt_valid_from,
dbt_valid_to,
dbt_updated_at

FROM source_data
)

SELECT *
FROM cleaned