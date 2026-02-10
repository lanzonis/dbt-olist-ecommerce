{{ config(
    materialized='table',
    schema='intermediate'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_olist__orders') }}
),

items AS (
    SELECT 
        order_id,
        COUNT(DISTINCT product_id) AS unique_products,
        COUNT(*) AS total_items,
        SUM(price) AS subtotal,
        SUM(freight_value) AS total_freight,
        SUM(total_item_value) AS total_value
    FROM {{ ref('stg_olist__order_items') }}
    GROUP BY order_id
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_paid,
        COUNT(*) AS payment_count,
        MAX(payment_type) AS primary_payment_method,
        MAX(payment_installments) AS max_installments
    FROM {{ ref('stg_olist__payments') }}
    GROUP BY order_id
),

reviews AS (
    SELECT
        order_id,
        AVG(review_score) AS avg_review_score,
        MAX(review_sentiment) AS review_sentiment,
        MAX(created_at) AS review_created_at
    FROM {{ ref('stg_olist__reviews') }}
    GROUP BY order_id
),

customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        city AS customer_city,
        state AS customer_state
    FROM {{ ref('stg_olist__customers') }}
),

final AS (
    SELECT
        -- Order info
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.carrier_date,
        o.delivered_at,
        o.estimated_delivery_at,
        
        -- Customer info
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        
        -- Items aggregation
        i.unique_products,
        i.total_items,
        i.subtotal,
        i.total_freight,
        i.total_value,
        
        -- Payment info
        p.total_paid,
        p.payment_count,
        p.primary_payment_method,
        p.max_installments,
        
        -- Review info
        r.avg_review_score,
        r.review_sentiment,
        r.review_created_at,
        
        -- Calculated metrics
        CASE 
            WHEN o.delivered_at IS NOT NULL 
            THEN o.delivered_at::DATE - o.estimated_delivery_at::DATE
            ELSE NULL
        END AS delivery_sla_days,
        
        CASE 
            WHEN o.delivered_at IS NOT NULL AND o.purchased_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (o.delivered_at - o.purchased_at))/86400
            ELSE NULL
        END AS delivery_time_days,
        
        CURRENT_TIMESTAMP AS dbt_loaded_at
        
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN items i ON o.order_id = i.order_id
    LEFT JOIN payments p ON o.order_id = p.order_id
    LEFT JOIN reviews r ON o.order_id = r.order_id
)

SELECT * FROM final
