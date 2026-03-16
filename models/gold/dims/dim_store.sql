WITH source_data AS (

SELECT *
FROM {{ ref('stg_store_data') }}

),

dim_store AS (

SELECT

{{ dbt_utils.generate_surrogate_key(['store_id']) }} AS StoreKey,

store_id AS StoreID,

store_name AS Store_Name,

CONCAT(street, ', ', city, ', ', state, ', ', zip_code, ', ', country) AS Address,

street as Street,

city as City,

state as State,

zip_code as ZipCode,

country as Country,

region AS Region,

store_type AS Store_Type,

opening_date AS Opening_Date,

store_size_category AS Size_Category

FROM source_data

)

SELECT *
FROM dim_store