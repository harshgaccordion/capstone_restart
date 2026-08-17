{{ config(
    materialized = 'view',
    schema = 'reporting'
) }}

WITH inventory_base AS (

    SELECT
        date_key,
        product_key,
        store_key,
        supplier_key,
        ending_stock,
        inventory_value
    FROM {{ ref('fact_inventory') }}

),

date_lookup AS (

    SELECT
        date_key,
        full_date
    FROM {{ ref('dim_date') }}

),

product_lookup AS (

    SELECT
        product_key,
        product_id,
        product_name,
        category,
        subcategory
    FROM {{ ref('dim_product') }}

),

store_lookup AS (

    SELECT
        store_key,
        store_id,
        store_name
    FROM {{ ref('dim_stores') }}

),

supplier_lookup AS (

    SELECT
        supplier_key,
        supplier_id,
        supplier_name
    FROM {{ ref('dim_supplier') }}

)

SELECT
    inv.date_key,
    dt.full_date,

    inv.product_key,
    prod.product_id,
    prod.product_name,
    prod.category,
    prod.subcategory,

    inv.store_key,
    st.store_id,
    st.store_name,

    inv.supplier_key,
    sup.supplier_id,
    sup.supplier_name,

    inv.ending_stock,
    inv.inventory_value

FROM inventory_base AS inv

LEFT JOIN date_lookup AS dt
    ON dt.date_key = inv.date_key

LEFT JOIN product_lookup AS prod
    ON prod.product_key = inv.product_key

LEFT JOIN store_lookup AS st
    ON st.store_key = inv.store_key

LEFT JOIN supplier_lookup AS sup
    ON sup.supplier_key = inv.supplier_key