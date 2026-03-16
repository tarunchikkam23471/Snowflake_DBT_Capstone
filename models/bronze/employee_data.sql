{{ config(materialized='table') }}

SELECT

    emp.value:employee_id::STRING              AS employee_id,
    emp.value:first_name::STRING               AS first_name,
    emp.value:last_name::STRING                AS last_name,
    emp.value:email::STRING                    AS email,
    emp.value:phone::STRING                    AS phone,
    emp.value:date_of_birth::STRING            AS date_of_birth,
    emp.value:hire_date::STRING                AS hire_date,
    emp.value:last_modified_date::STRING       AS last_modified_date,
    emp.value:department::STRING               AS department,
    emp.value:role::STRING                     AS role,
    emp.value:employment_status::STRING        AS employment_status,
    emp.value:education::STRING                AS education,
    emp.value:manager_id::STRING               AS manager_id,
    emp.value:work_location::STRING            AS work_location,
    emp.value:salary::STRING                   AS salary,
    emp.value:sales_target::STRING             AS sales_target,
    emp.value:current_sales::STRING            AS current_sales,
    emp.value:performance_rating::STRING       AS performance_rating,
    emp.value:address.street::STRING           AS street,
    emp.value:address.city::STRING             AS city,
    emp.value:address.state::STRING            AS state,
    emp.value:address.zip_code::STRING         AS zip_code,
    ARRAY_TO_STRING(emp.value:certifications, ', ') 
                                              AS certifications,

    CURRENT_TIMESTAMP()                        AS load_timestamp

FROM {{ source('bronze_ext','ext_employee_data') }} ,
LATERAL FLATTEN(input => VALUE:employees_data) emp