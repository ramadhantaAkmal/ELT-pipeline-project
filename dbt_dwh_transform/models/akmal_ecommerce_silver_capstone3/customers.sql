{{ config(
    materialized='incremental',
    description='Cleansed customers table with standardized column names, structured address, and removed sensitive data',
    partition_by={
        "field": "DATE(created_at)",
        "data_type": "date",
        "granularity": "day"
    },
) }}

SELECT
    customer_id,
    customer_fname AS first_name,
    customer_lname AS last_name,
    customer_email AS email,
    STRUCT(
        customer_street AS street,
        customer_city AS city,
        customer_state AS state,
        customer_zipcode AS zipcode
    ) AS address,
    created_at
FROM {{ source('akmal_ecommerce_bronze_capstone3', 'customers') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}