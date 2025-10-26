{{ config(
    materialized='table',
    description='Cleansed categories table with renamed foreign key and standardized column names'
) }}

SELECT
    category_id,
    category_department_id AS department_id,
    category_name AS name,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'categories') }}