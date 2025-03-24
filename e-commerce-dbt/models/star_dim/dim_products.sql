{{ config(materialized='table') }}

WITH product_details AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght AS product_name_length,
        product_description_lenght AS product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        (product_length_cm * product_height_cm * product_width_cm) AS product_volume_cm3,
    FROM {{ source('gcs_ingestion', 'olist_products_dataset') }}
),

category_name_translation AS (
    SELECT
        string_field_0 AS product_category_name_pt,
        string_field_1 AS product_category_name_en
    FROM {{ source('gcs_ingestion', 'product_category_name_translation') }}
)

SELECT 
    p.product_id,
    COALESCE(cnt.product_category_name_en, 'others') AS product_category_name,
    COALESCE(p.product_name_length, 0) AS product_name_length,
    COALESCE(p.product_description_length, 0) AS product_description_length,
    COALESCE(p.product_photos_qty, 0) AS product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
FROM product_details p LEFT JOIN category_name_translation cnt
ON p.product_category_name = cnt.product_category_name_pt
WHERE p.product_weight_g IS NOT NULL
AND p.product_length_cm IS NOT NULL
AND p.product_height_cm IS NOT NULL
AND p.product_width_cm IS NOT NULL
AND p.product_volume_cm3 IS NOT NULL