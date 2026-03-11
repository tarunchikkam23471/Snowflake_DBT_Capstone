{{ config(materialized='table') }}

SELECT

    cust.value:customer_id::STRING                AS customer_id,
    cust.value:first_name::STRING                 AS first_name,
    cust.value:last_name::STRING                  AS last_name,
    cust.value:email::STRING                      AS email,
    cust.value:phone::STRING                      AS phone,
    cust.value:birth_date::STRING                 AS birth_date,
    cust.value:registration_date::STRING          AS registration_date,
    cust.value:last_purchase_date::STRING         AS last_purchase_date,
    cust.value:last_modified_date::STRING         AS last_modified_date,
    cust.value:income_bracket::STRING             AS income_bracket,
    cust.value:loyalty_tier::STRING               AS loyalty_tier,
    cust.value:marketing_opt_in::STRING           AS marketing_opt_in,
    cust.value:occupation::STRING                 AS occupation,
    cust.value:preferred_payment_method::STRING   AS preferred_payment_method,
    cust.value:preferred_communication::STRING    AS preferred_communication,
    cust.value:total_purchases::STRING            AS total_purchases,
    cust.value:total_spend::STRING                AS total_spend,

    cust.value:address.street::STRING             AS street,
    cust.value:address.city::STRING               AS city,
    cust.value:address.state::STRING              AS state,
    cust.value:address.zip_code::STRING           AS zip_code,
    cust.value:address.country::STRING            AS country,

    CURRENT_TIMESTAMP()                           AS load_timestamp

FROM {{ source('bronze_ext','ext_customer_data') }},
LATERAL FLATTEN(input => VALUE:customers_data) cust