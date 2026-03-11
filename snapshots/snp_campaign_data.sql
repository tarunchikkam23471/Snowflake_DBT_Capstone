{% snapshot snp_campaign_data %}

{{
config(
target_database='SNOWFLAKE_DBT_CAPSTONE',
target_schema='BRONZE',
unique_key='campaign_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}

SELECT *
FROM {{ ref('campaign_data') }}

QUALIFY ROW_NUMBER() OVER(
PARTITION BY campaign_id
ORDER BY last_modified_date DESC
)=1

{% endsnapshot %}
