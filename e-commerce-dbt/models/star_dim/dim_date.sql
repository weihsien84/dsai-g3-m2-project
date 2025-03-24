{{ config(materialized='table') }}

WITH order_details AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ source('gcs_ingestion', 'olist_orders_dataset') }}
)

SELECT
    order_id,
    order_purchase_timestamp,
    CAST(FORMAT_DATE('%Y%m%d', DATE(order_purchase_timestamp)) AS INT64) AS date_id,
    DATE(order_purchase_timestamp) AS date,
    EXTRACT(WEEK FROM order_purchase_timestamp) AS week,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year
FROM order_details