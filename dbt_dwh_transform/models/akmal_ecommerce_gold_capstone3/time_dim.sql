{{ config(
    materialized='table',
    description='Dimension table for time in the Star Schema',
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS time_id,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_of_week
FROM (
    SELECT DISTINCT DATE(order_timestamp) AS date
    FROM {{ source('akmal_ecommerce_silver_capstone3', 'orders') }}
) dates