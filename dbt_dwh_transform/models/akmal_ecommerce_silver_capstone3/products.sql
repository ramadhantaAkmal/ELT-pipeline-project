{{ config(
    materialized='incremental',
    description='Cleansed products table with fixed data type for product_name and renamed foreign key',
    partition_by={
        "field": "DATE(created_at)",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    product_id,
    product_category_id AS category_id,
    product_name AS name,
    product_description AS description,
    product_price AS price,
    product_image AS image,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'products') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}