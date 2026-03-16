{{ config(materialized='table') }}

SELECT

    prod.value:product_id::STRING            AS product_id,
    prod.value:name::STRING                  AS product_name,
    prod.value:brand::STRING                 AS brand,
    prod.value:category::STRING              AS category,
    prod.value:subcategory::STRING           AS subcategory,
    prod.value:product_line::STRING          AS product_line,
    prod.value:color::STRING                 AS color,
    prod.value:size::STRING                  AS size,
    prod.value:weight::STRING                AS weight,
    prod.value:dimensions::STRING            AS dimensions,
    prod.value:unit_price::STRING            AS unit_price,
    prod.value:cost_price::STRING            AS cost_price,
    prod.value:stock_quantity::STRING        AS stock_quantity,
    prod.value:reorder_level::STRING         AS reorder_level,
    prod.value:supplier_id::STRING           AS supplier_id,
    prod.value:is_featured::STRING           AS is_featured,
    prod.value:launch_date::STRING           AS launch_date,
    prod.value:last_modified_date::STRING    AS last_modified_date,
    prod.value:warranty_period::STRING       AS warranty_period,
    prod.value:short_description::STRING     AS short_description,
    prod.value:technical_specs::STRING       AS technical_specs,

    CURRENT_TIMESTAMP()                      AS load_timestamp

FROM {{ source('bronze_ext','ext_product_data') }},
LATERAL FLATTEN(input => VALUE:products_data) prod