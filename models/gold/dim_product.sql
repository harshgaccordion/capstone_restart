{{ config(
    materialized='table'
) }}

WITH products AS (

    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        color,
        size,
        unit_price,
        cost_price,
        supplier_id

    FROM {{ ref('silver_product_data') }}

),

suppliers AS (

    SELECT
        supplier_id,
        supplier_name

    FROM {{ ref('silver_supplier_data') }}

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key([
            'p.product_id'
        ]) }} AS product_key,

        p.product_id,

        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.color,
        p.size,

        p.unit_price,
        p.cost_price,

        p.supplier_id,
        s.supplier_name

    FROM products p

    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id

)

SELECT *

FROM final