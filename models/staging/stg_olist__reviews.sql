{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_order_reviews_dataset') }}
),

cleaned AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::TIMESTAMP AS created_at,
        review_answer_timestamp::TIMESTAMP AS answered_at,
        LENGTH(review_comment_message) AS comment_length,
        CASE 
            WHEN review_score >= 4 THEN 'positive'
            WHEN review_score = 3 THEN 'neutral'
            ELSE 'negative'
        END AS review_sentiment,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
)

SELECT * FROM cleaned
