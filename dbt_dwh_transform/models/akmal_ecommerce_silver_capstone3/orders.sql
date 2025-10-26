{{ config(
    materialized='incremental',
    description='Cleansed orders table with renamed columns for consistency',
    partition_by={
        "field": "DATE(created_at)",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    order_id,
    order_date AS order_timestamp,
    order_customer_id AS customer_id,
    order_status AS status,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'orders') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}