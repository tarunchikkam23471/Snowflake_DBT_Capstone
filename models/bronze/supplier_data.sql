{{ config(materialized='table') }}

SELECT

    sup.value:supplier_id::STRING                     AS supplier_id,
    sup.value:supplier_name::STRING                   AS supplier_name,
    sup.value:supplier_type::STRING                   AS supplier_type,
    sup.value:website::STRING                         AS website,
    sup.value:tax_id::STRING                          AS tax_id,
    sup.value:year_established::STRING                AS year_established,
    sup.value:credit_rating::STRING                   AS credit_rating,
    sup.value:is_active::STRING                       AS is_active,
    sup.value:lead_time_days::STRING                  AS lead_time_days,
    sup.value:minimum_order_quantity::STRING          AS minimum_order_quantity,
    sup.value:payment_terms::STRING                   AS payment_terms,
    sup.value:preferred_carrier::STRING               AS preferred_carrier,
    sup.value:last_order_date::STRING                 AS last_order_date,
    sup.value:last_modified_date::STRING              AS last_modified_date,
    ARRAY_TO_STRING(sup.value:categories_supplied, ', ') 
                                                    AS categories_supplied,
    sup.value:contact_information.contact_person::STRING  AS contact_person,
    sup.value:contact_information.email::STRING           AS email,
    sup.value:contact_information.phone::STRING           AS phone,
    sup.value:contact_information.address::STRING         AS address,
    sup.value:contract_details.contract_id::STRING        AS contract_id,
    sup.value:contract_details.start_date::STRING         AS contract_start_date,
    sup.value:contract_details.end_date::STRING           AS contract_end_date,
    sup.value:contract_details.exclusivity::STRING        AS exclusivity,
    sup.value:contract_details.renewal_option::STRING     AS renewal_option,
    sup.value:performance_metrics.on_time_delivery_rate::STRING   AS on_time_delivery_rate,
    sup.value:performance_metrics.defect_rate::STRING             AS defect_rate,
    sup.value:performance_metrics.average_delay_days::STRING      AS average_delay_days,
    sup.value:performance_metrics.response_time_hours::STRING     AS response_time_hours,
    sup.value:performance_metrics.returns_percentage::STRING      AS returns_percentage,
    sup.value:performance_metrics.quality_rating::STRING          AS quality_rating,

    CURRENT_TIMESTAMP()                                     AS load_timestamp

FROM {{ source('bronze_ext','ext_supplier_data') }},
LATERAL FLATTEN(input => VALUE:suppliers_data) sup