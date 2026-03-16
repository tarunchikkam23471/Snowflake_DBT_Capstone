WITH source_data AS (

SELECT *
FROM {{ ref('snp_store_data') }}
WHERE dbt_valid_to IS NULL

),

cleaned AS (

SELECT

TRIM(store_id) AS store_id,

INITCAP(TRIM(store_name)) AS store_name,
LOWER(TRIM(store_type)) AS store_type,
LOWER(TRIM(region)) AS region,

LOWER(TRIM(email)) AS email,

CASE
    WHEN LOWER(TRIM(email)) RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    THEN LOWER(TRIM(email))
    ELSE NULL
END AS valid_email,

REGEXP_REPLACE(phone_number,'[^0-9X]','') AS phone_number,

'+1' || REGEXP_REPLACE(REGEXP_REPLACE(phone_number, '[^0-9]', ''), '^1555', '') AS valid_phone,

TRIM(manager_id) AS manager_id,

TRY_TO_DATE(opening_date) AS opening_date,
TRY_TO_DATE(last_modified_date) AS last_modified_date,

TRY_TO_BOOLEAN(is_active) AS is_active,

TRY_TO_NUMBER(employee_count) AS employee_count,
TRY_TO_NUMBER(size_sq_ft) AS size_sq_ft,

TRY_TO_NUMBER(monthly_rent) AS monthly_rent,
TRY_TO_NUMBER(current_sales) AS current_sales,
TRY_TO_NUMBER(sales_target) AS sales_target,

INITCAP(TRIM(street)) AS street,
INITCAP(TRIM(city)) AS city,
UPPER(TRIM(state)) AS state,
TRIM(zip_code) AS zip_code,
UPPER(TRIM(country)) AS country,

TRIM(weekday_hours) AS weekday_hours,
TRIM(weekend_hours) AS weekend_hours,
TRIM(holiday_hours) AS holiday_hours,

LOWER(TRIM(services)) AS services,

dbt_valid_from,
dbt_valid_to,
dbt_updated_at

FROM source_data

),

store_metrics AS (

SELECT

*,
CASE
WHEN size_sq_ft < 5000 THEN 'Small'
WHEN size_sq_ft BETWEEN 5000 AND 10000 THEN 'Medium'
WHEN size_sq_ft > 10000 THEN 'Large'
END AS store_size_category,

DATEDIFF(year, opening_date, CURRENT_DATE) AS store_age_years,

CASE
WHEN sales_target > 0
THEN (current_sales / sales_target) * 100
ELSE NULL
END AS sales_target_achievement_percentage,

CASE
WHEN size_sq_ft > 0
THEN current_sales / size_sq_ft
ELSE NULL
END AS revenue_per_sq_ft,

CASE
WHEN employee_count > 0
THEN current_sales / employee_count
ELSE NULL
END AS employee_efficiency,

CASE
WHEN sales_target > 0
AND (current_sales / sales_target) * 100 < 90
THEN TRUE
ELSE FALSE
END AS performance_issue_flag

FROM cleaned

)

SELECT *
FROM store_metrics