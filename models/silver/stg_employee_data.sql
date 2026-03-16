WITH source_data AS (

SELECT *
FROM {{ ref('snp_employee_data') }}
WHERE dbt_valid_to IS NULL

),

cleaned AS (

SELECT


TRIM(employee_id) AS employee_id,
TRIM(manager_id) AS manager_id,
TRIM(work_location) AS work_location,


INITCAP(TRIM(first_name)) AS first_name,
INITCAP(TRIM(last_name)) AS last_name,

INITCAP(TRIM(first_name)) || ' ' || INITCAP(TRIM(last_name))
    AS full_name,


LOWER(TRIM(email)) AS email,

CASE
WHEN LOWER(TRIM(email)) RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
THEN LOWER(TRIM(email))
ELSE NULL
END AS valid_email,


REGEXP_REPLACE(phone,'[^0-9X]','') AS phone_number,

'+1' || REGEXP_REPLACE(REGEXP_REPLACE(phone, '[^0-9]', ''), '^1555', '') AS valid_phone,


TRY_TO_DATE(date_of_birth) AS date_of_birth,
TRY_TO_DATE(hire_date) AS hire_date,
TRY_TO_DATE(last_modified_date) AS last_modified_date,


DATEDIFF(year, TRY_TO_DATE(hire_date), CURRENT_DATE)
    AS tenure_years,


INITCAP(TRIM(department)) AS department,

INITCAP(TRIM(role)) AS role_raw,

CASE
WHEN LOWER(role) LIKE '%sales associate%' THEN 'Associate'
WHEN LOWER(role) LIKE '%store manager%' THEN 'Manager'
WHEN LOWER(role) LIKE '%senior manager%' THEN 'Senior Manager'
WHEN LOWER(role) LIKE '%inventory specialist%' THEN 'Specialist'
WHEN LOWER(role) LIKE '%cashier%' THEN 'Associate'
WHEN LOWER(role) LIKE '%customer service%' THEN 'Associate'
ELSE INITCAP(TRIM(role))
END AS role_standardized,

LOWER(TRIM(employment_status)) AS employment_status,

INITCAP(TRIM(education)) AS education,


TRY_TO_NUMBER(salary) AS salary,
TRY_TO_NUMBER(sales_target) AS sales_target,
TRY_TO_NUMBER(current_sales) AS current_sales,
TRY_TO_NUMBER(performance_rating) AS performance_rating,


CASE
WHEN sales_target > 0
THEN (current_sales / sales_target) * 100
END AS target_achievement_percentage,


INITCAP(TRIM(street)) AS street,
INITCAP(TRIM(city)) AS city,
UPPER(TRIM(state)) AS state,
TRIM(zip_code) AS zip_code,


TRIM(certifications) AS certifications,

load_timestamp::timestamp_ntz AS load_timestamp

FROM source_data

),


employee_orders AS (

SELECT

employee_id,

COUNT(order_id) AS orders_processed,

SUM(total_amount) AS total_sales_amount

FROM {{ ref('stg_orders_data') }}

GROUP BY employee_id

)

SELECT

c.*,

COALESCE(e.orders_processed,0) AS orders_processed,
COALESCE(e.total_sales_amount,0) AS total_sales_amount

FROM cleaned c
LEFT JOIN employee_orders e
ON c.employee_id = e.employee_id