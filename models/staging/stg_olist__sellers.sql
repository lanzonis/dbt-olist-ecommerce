{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_sellers_dataset') }}
),

renamed AS (
    SELECT
        seller_id,
        seller_zip_code_prefix AS zip_code,
        seller_city AS city,
        seller_state AS state,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
)

SELECT * FROM renamed
