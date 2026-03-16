WITH source_data AS (

SELECT *
FROM {{ ref('orders_data') }}

),

cleaned AS (

SELECT

MAX(TRIM(order_id)) AS order_id,
MAX(TRIM(customer_id)) AS customer_id,
MAX(TRIM(employee_id)) AS employee_id,
MAX(TRIM(campaign_id)) AS campaign_id,
MAX(TRIM(store_id)) AS store_id,

MAX(order_date::timestamp_ntz) AS order_date,
MAX(created_at::timestamp_ntz) AS created_at,
MAX(shipping_date::timestamp_ntz) AS shipping_date,
MAX(delivery_date::timestamp_ntz) AS delivery_date,
MAX(estimated_delivery_date::timestamp_ntz) AS estimated_delivery_date,

MAX(LOWER(TRIM(order_source))) AS order_source,
MAX(LOWER(TRIM(order_status))) AS order_status,
MAX(LOWER(TRIM(payment_method))) AS payment_method,
MAX(LOWER(TRIM(shipping_method))) AS shipping_method,

MAX(TRY_TO_NUMBER(order_discount_amount)) AS order_discount_amount,
MAX(TRY_TO_NUMBER(shipping_cost)) AS shipping_cost,
MAX(TRY_TO_NUMBER(tax_amount)) AS tax_amount,
MAX(TRY_TO_NUMBER(total_amount)) AS total_amount,

MAX(INITCAP(TRIM(billing_street))) AS billing_street,
MAX(INITCAP(TRIM(billing_city))) AS billing_city,
MAX(UPPER(TRIM(billing_state))) AS billing_state,
MAX(TRIM(billing_zip_code)) AS billing_zip_code,

MAX(INITCAP(TRIM(shipping_street))) AS shipping_street,
MAX(INITCAP(TRIM(shipping_city))) AS shipping_city,
MAX(UPPER(TRIM(shipping_state))) AS shipping_state,
MAX(TRIM(shipping_zip_code)) AS shipping_zip_code,

MAX(TRIM(product_id)) AS product_id,
SUM(TRY_TO_NUMBER(quantity)) AS quantity,
MAX(TRY_TO_NUMBER(unit_price)) AS unit_price,
MAX(TRY_TO_NUMBER(cost_price)) AS cost_price,
MAX(TRY_TO_NUMBER(item_discount_amount)) AS item_discount_amount,

MAX(load_timestamp::timestamp_ntz) AS load_timestamp

FROM source_data

GROUP BY order_id, product_id
)

SELECT *
FROM cleaned