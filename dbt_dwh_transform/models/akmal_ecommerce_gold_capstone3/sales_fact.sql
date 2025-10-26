{{ config(
    materialized='incremental',
    description='Fact table for sales data in the Star Schema, partitioned by date',
    partition_by={
        "field": "DATE(last_updated)",
        "data_type": "date",
        "granularity": "day"
    },
) }}

SELECT
    oi.order_item_id AS sale_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    t.time_id,
    oi.quantity,
    oi.subtotal,
    o.status AS order_status,
    oi.created_at AS last_updated
FROM {{ source('akmal_ecommerce_silver_capstone3', 'order_items') }} oi
JOIN {{ source('akmal_ecommerce_silver_capstone3', 'orders') }} o
    ON oi.order_id = o.order_id
JOIN {{ ref('time_dim') }} t
    ON DATE(o.order_timestamp) = t.date
{% if is_incremental() %}
WHERE oi.created_at > (SELECT MAX(last_updated) FROM {{ this }})
{% endif %}