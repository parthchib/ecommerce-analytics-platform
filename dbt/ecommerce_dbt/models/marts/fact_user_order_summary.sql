{{ config(
    materialized='table',
    schema='main'
) }}
WITH order_summary AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        MIN(o.order_number) AS first_order_number,
        MAX(o.order_number) AS last_order_number,
        COUNT(DISTINCT op.product_id) AS unique_products_purchased,
        SUM(op.reordered) AS total_reordered_items,
        AVG(o.days_since_prior_order) AS average_days_between_orders
    FROM {{ source('ecommerce', 'stg_orders') }} o
    LEFT JOIN {{ source('ecommerce', 'stg_order_products') }} op
        ON o.order_id = op.order_id
    GROUP BY o.user_id
)
SELECT
    user_id,
    total_orders,
    first_order_number,
    last_order_number,
    unique_products_purchased,
    total_reordered_items,
    average_days_between_orders
    from order_summary

