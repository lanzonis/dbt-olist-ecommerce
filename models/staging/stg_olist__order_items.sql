{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_order_items_dataset') }}
),

cleaned AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::TIMESTAMP AS shipping_limit_date,
        price::DECIMAL(10,2) AS price,
        freight_value::DECIMAL(10,2) AS freight_value,
        (price + freight_value)::DECIMAL(10,2) AS total_item_value,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
)

SELECT * FROM cleaned
