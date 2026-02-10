{{ config(materialized='view') }}

WITH products AS (
    SELECT * FROM {{ source('olist_raw', 'olist_products_dataset') }}
),

categories AS (
    SELECT * FROM {{ source('olist_raw', 'olist_product_category_name_traslation') }}
),

joined AS (
    SELECT
        p.product_id,
        p.product_category_name,
        COALESCE(c.product_category_name_english, 'uncategorized') AS category_english,
        p.product_name_lenght AS name_length,
        p.product_description_lenght AS description_length,
        p.product_photos_qty AS photos_qty,
        p.product_weight_g AS weight_grams,
        p.product_length_cm AS length_cm,
        p.product_height_cm AS height_cm,
        p.product_width_cm AS width_cm,
        (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS volume_cm3,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM products p
    LEFT JOIN categories c ON p.product_category_name = c.product_category_name
)

SELECT * FROM joined
