{{ config(materialized='table') }}

SELECT

    store.value:store_id::STRING               AS store_id,
    store.value:store_name::STRING             AS store_name,
    store.value:store_type::STRING             AS store_type,

    store.value:region::STRING                 AS region,

    store.value:phone_number::STRING           AS phone_number,
    store.value:email::STRING                  AS email,

    store.value:manager_id::STRING             AS manager_id,

    store.value:opening_date::STRING           AS opening_date,
    store.value:last_modified_date::STRING     AS last_modified_date,

    store.value:is_active::STRING              AS is_active,

    store.value:employee_count::STRING         AS employee_count,
    store.value:size_sq_ft::STRING             AS size_sq_ft,

    store.value:monthly_rent::STRING           AS monthly_rent,

    store.value:current_sales::STRING          AS current_sales,
    store.value:sales_target::STRING           AS sales_target,

    store.value:address.street::STRING         AS street,
    store.value:address.city::STRING           AS city,
    store.value:address.state::STRING          AS state,
    store.value:address.zip_code::STRING       AS zip_code,
    store.value:address.country::STRING        AS country,

    store.value:operating_hours.weekdays::STRING   AS weekday_hours,
    store.value:operating_hours.weekends::STRING   AS weekend_hours,
    store.value:operating_hours.holidays::STRING   AS holiday_hours,

    ARRAY_TO_STRING(store.value:services, ', ') AS services,

    CURRENT_TIMESTAMP()                        AS load_timestamp

FROM {{ source('bronze_ext','ext_store_data') }},
LATERAL FLATTEN(input => VALUE:stores_data) store