WITH source_data AS (

SELECT *
FROM {{ ref('orders_data') }}

),

cleaned AS (

SELECT

TRIM(order_id) AS order_id,
TRIM(customer_id) AS customer_id,
TRIM(employee_id) AS employee_id,
TRIM(campaign_id) AS campaign_id,
TRIM(store_id) AS store_id,

order_date::timestamp_ntz AS order_date,
created_at::timestamp_ntz AS created_at,
shipping_date::timestamp_ntz AS shipping_date,
delivery_date::timestamp_ntz AS delivery_date,
estimated_delivery_date::timestamp_ntz AS estimated_delivery_date,

LOWER(TRIM(order_source)) AS order_source,
LOWER(TRIM(order_status)) AS order_status,
LOWER(TRIM(payment_method)) AS payment_method,
LOWER(TRIM(shipping_method)) AS shipping_method,

TRY_TO_NUMBER(order_discount_amount) AS order_discount_amount,
TRY_TO_NUMBER(shipping_cost) AS shipping_cost,
TRY_TO_NUMBER(tax_amount) AS tax_amount,
TRY_TO_NUMBER(total_amount) AS total_amount,

INITCAP(TRIM(billing_street)) AS billing_street,
INITCAP(TRIM(billing_city)) AS billing_city,
UPPER(TRIM(billing_state)) AS billing_state,
TRIM(billing_zip_code) AS billing_zip_code,

INITCAP(TRIM(shipping_street)) AS shipping_street,
INITCAP(TRIM(shipping_city)) AS shipping_city,
UPPER(TRIM(shipping_state)) AS shipping_state,
TRIM(shipping_zip_code) AS shipping_zip_code,

TRIM(product_id) AS product_id,
TRY_TO_NUMBER(quantity) AS quantity,
TRY_TO_NUMBER(unit_price) AS unit_price,
TRY_TO_NUMBER(cost_price) AS cost_price,
TRY_TO_NUMBER(item_discount_amount) AS item_discount_amount,

load_timestamp::timestamp_ntz AS load_timestamp

FROM source_data

)

SELECT *
FROM cleaned