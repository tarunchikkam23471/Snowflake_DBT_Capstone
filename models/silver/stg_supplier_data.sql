WITH source_data AS (

SELECT *
FROM {{ ref('snp_supplier_data') }}
WHERE dbt_valid_to IS NULL

),

cleaned AS (

SELECT

TRIM(supplier_id) AS supplier_id,

INITCAP(TRIM(supplier_name)) AS supplier_name,

INITCAP(TRIM(supplier_type)) AS supplier_type,

LOWER(TRIM(website)) AS website,

TRIM(tax_id) AS tax_id,

TRY_TO_NUMBER(year_established) AS year_established,

UPPER(TRIM(credit_rating)) AS credit_rating,

TRY_TO_BOOLEAN(is_active) AS is_active,

TRY_TO_NUMBER(lead_time_days) AS lead_time_days,

TRY_TO_NUMBER(minimum_order_quantity) AS minimum_order_quantity,

INITCAP(TRIM(payment_terms)) AS payment_terms,

UPPER(TRIM(preferred_carrier)) AS preferred_carrier,

TRY_TO_DATE(last_order_date) AS last_order_date,

TRY_TO_DATE(last_modified_date) AS last_modified_date,

INITCAP(TRIM(categories_supplied)) AS categories_supplied,

INITCAP(TRIM(contact_person)) AS contact_person,

LOWER(TRIM(email)) AS email,

CASE
WHEN LOWER(TRIM(email)) RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
THEN LOWER(TRIM(email))
ELSE NULL
END AS valid_email,

REGEXP_REPLACE(phone,'[^0-9X]','') AS phone_number,

CASE
        WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9X]', '')) = 10
        THEN '+1' || REGEXP_REPLACE(phone, '[^0-9X]', '')
        WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9X]', '')) = 11
        THEN '+' || REGEXP_REPLACE(phone, '[^0-9X]', '')
    ELSE NULL
    END AS valid_phone,

INITCAP(TRIM(address)) AS address,

TRIM(contract_id) AS contract_id,

TRY_TO_DATE(contract_start_date) AS contract_start_date,

TRY_TO_DATE(contract_end_date) AS contract_end_date,

TRY_TO_BOOLEAN(exclusivity) AS exclusivity,

TRY_TO_BOOLEAN(renewal_option) AS renewal_option,

TRY_TO_NUMBER(on_time_delivery_rate) AS on_time_delivery_rate,

TRY_TO_NUMBER(defect_rate) AS defect_rate,

TRY_TO_NUMBER(average_delay_days) AS average_delay_days,

TRY_TO_NUMBER(response_time_hours) AS response_time_hours,

TRY_TO_NUMBER(returns_percentage) AS returns_percentage,

LOWER(TRIM(quality_rating)) AS quality_rating,

dbt_valid_from,
dbt_valid_to,
dbt_updated_at

FROM source_data

)

SELECT *
FROM cleaned