{{ config(
    materialized='table',
    description='Cleansed departments table with standardized column names',
) }}

SELECT
    department_id,
    department_name AS name,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'departments') }}