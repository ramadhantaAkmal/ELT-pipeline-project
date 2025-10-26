{{ config(
    materialized='table',
    description='Dimension table for customers in the Star Schema',
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    address.street,
    address.city,
    address.state,
    address.zipcode
FROM {{ source('akmal_ecommerce_silver_capstone3', 'customers') }}