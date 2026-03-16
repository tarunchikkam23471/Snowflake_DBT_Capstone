WITH source_data AS (
SELECT *
FROM {{ ref('stg_employee_data') }}
),

dim_employee AS (
SELECT

{{ dbt_utils.generate_surrogate_key(['employee_id']) }} AS EmployeeKey,
employee_id AS EmployeeID,
full_name AS Full_Name,
role_standardized AS Role,
work_location AS Work_Location,
tenure_years AS Tenure,
valid_email AS Email_ID,
valid_phone AS Phone_Number,
performance_rating AS Performance_Metrics,
target_achievement_percentage,

FROM source_data
)

SELECT *
FROM dim_employee