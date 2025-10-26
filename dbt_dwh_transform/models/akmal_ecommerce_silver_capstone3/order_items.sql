{{ config(
    materialized='incremental',
    description='Cleansed order_items table with renamed columns and removed redundant product price',
    partition_by={
        "field": "DATE(created_at)",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    order_item_id,
    order_item_order_id AS order_id,
    order_item_product_id AS product_id,
    order_item_quantity AS quantity,
    order_item_subtotal AS subtotal,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'order_items') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}