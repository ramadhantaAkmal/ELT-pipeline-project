{{ config(
    materialized='table',
    description='Dimension table for products in the Star Schema',
) }}

SELECT
    p.product_id,
    p.name AS product_name,
    p.description,
    p.price,
    p.category_id,
    c.name AS category_name,
    c.department_id,
    d.name AS department_name
FROM {{ source('akmal_ecommerce_silver_capstone3', 'products') }} p
JOIN {{ source('akmal_ecommerce_silver_capstone3', 'categories') }} c
    ON p.category_id = c.category_id
JOIN {{ source('akmal_ecommerce_silver_capstone3', 'departments') }} d
    ON c.department_id = d.department_id