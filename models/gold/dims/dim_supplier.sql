WITH source_data AS (
SELECT *
FROM {{ ref('stg_supplier_data') }}
),

dim_supplier AS (
SELECT
{{ dbt_utils.generate_surrogate_key(['supplier_id']) }} AS SupplierKey,
supplier_id as SupplierID,
supplier_name as Supplier_Name,
contact_person as ContactPerson,
valid_email AS Email,
valid_phone AS Phone,
payment_terms AS Payment_Terms,
supplier_type AS Supplier_Type
FROM source_data
)

SELECT *
FROM dim_supplier