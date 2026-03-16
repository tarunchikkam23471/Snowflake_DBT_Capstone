WITH source_data AS (

SELECT *
FROM {{ ref('stg_customer_data') }}

),

dim_customer AS (

SELECT

{{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CustomerKey,

customer_id AS CustomerID,

full_name AS Full_Name,

valid_email AS Email,

valid_phone AS Phone,

street AS Street,
city AS City,
state AS State,
zip_code AS ZipCode,
country AS Country,

customer_age AS Age,
income_bracket AS Income_Bracket,
occupation AS Occupation,

customer_segment AS Segment,

registration_date AS Registration_Date,

dbt_valid_from,
dbt_valid_to,
dbt_updated_at

FROM source_data

)

SELECT *
FROM dim_customer