{{ config(materialized='table') }}

SELECT

    ord.value:order_id::STRING                     AS order_id,
    ord.value:customer_id::STRING                  AS customer_id,
    ord.value:employee_id::STRING                  AS employee_id,
    ord.value:campaign_id::STRING                  AS campaign_id,
    ord.value:store_id::STRING                     AS store_id,
    ord.value:order_date::STRING                   AS order_date,
    ord.value:created_at::STRING                   AS created_at,
    ord.value:shipping_date::STRING                AS shipping_date,
    ord.value:delivery_date::STRING                AS delivery_date,
    ord.value:estimated_delivery_date::STRING      AS estimated_delivery_date,
    ord.value:order_source::STRING                 AS order_source,
    ord.value:order_status::STRING                 AS order_status,
    ord.value:payment_method::STRING               AS payment_method,
    ord.value:shipping_method::STRING              AS shipping_method,
    ord.value:discount_amount::STRING              AS order_discount_amount,
    ord.value:shipping_cost::STRING                AS shipping_cost,
    ord.value:tax_amount::STRING                   AS tax_amount,
    ord.value:total_amount::STRING                 AS total_amount,
    ord.value:billing_address.street::STRING       AS billing_street,
    ord.value:billing_address.city::STRING         AS billing_city,
    ord.value:billing_address.state::STRING        AS billing_state,
    ord.value:billing_address.zip_code::STRING     AS billing_zip_code,
    ord.value:shipping_address.street::STRING      AS shipping_street,
    ord.value:shipping_address.city::STRING        AS shipping_city,
    ord.value:shipping_address.state::STRING       AS shipping_state,
    ord.value:shipping_address.zip_code::STRING    AS shipping_zip_code,
    item.value:product_id::STRING                  AS product_id,
    item.value:quantity::STRING                    AS quantity,
    item.value:unit_price::STRING                  AS unit_price,
    item.value:cost_price::STRING                  AS cost_price,
    item.value:discount_amount::STRING             AS item_discount_amount,

    CURRENT_TIMESTAMP()                            AS load_timestamp

FROM {{ source('bronze_ext','ext_orders_data') }} src
     , LATERAL FLATTEN(input => src.VALUE:orders_data) ord
     , LATERAL FLATTEN(input => ord.value:order_items) item