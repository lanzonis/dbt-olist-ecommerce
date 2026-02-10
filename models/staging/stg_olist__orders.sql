{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_orders_dataset') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp::TIMESTAMP AS purchased_at,
        order_approved_at::TIMESTAMP AS approved_at,
        order_delivered_carrier_date::TIMESTAMP AS carrier_date,
        order_delivered_customer_date::TIMESTAMP AS delivered_at,
        order_estimated_delivery_date::TIMESTAMP AS estimated_delivery_at,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE order_purchase_timestamp IS NOT NULL
)

SELECT * FROM cleaned
