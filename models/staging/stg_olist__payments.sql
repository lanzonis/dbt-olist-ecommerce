{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_order_payments_dataset') }}
),

cleaned AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value::DECIMAL(10,2) AS payment_value,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
)

SELECT * FROM cleaned
