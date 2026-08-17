{{ config(
    materialized = 'table'
) }}

WITH inventory AS (

    SELECT
        inventory_key,
        product_id,
        store_id,
        inventory_date,
        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,
        inventory_value,
        stock_turnover_ratio,
        supplier_contribution_percentage,
        supplier_id,
        snapshot_gap_flag,
        snapshot_gap_days

    FROM {{ ref('silver_inventory_data') }}

),

products AS (

    SELECT
        product_key,
        product_id

    FROM {{ ref('dim_product') }}

),

stores AS (

    SELECT
        store_key,
        store_id

    FROM {{ ref('dim_stores') }}

),

suppliers AS (

    SELECT
        supplier_key,
        supplier_id

    FROM {{ ref('dim_supplier') }}

),

dates AS (

    SELECT
        date_key,
        full_date

    FROM {{ ref('dim_date') }}

),

final AS (

    SELECT

        i.inventory_key,

        p.product_key,
        d.date_key,
        st.store_key,
        s.supplier_key,

        i.beginning_stock,
        i.purchased_quantity,
        i.sold_quantity,
        i.ending_stock,

        i.inventory_value,
        i.stock_turnover_ratio,
        i.supplier_contribution_percentage,

        i.snapshot_gap_flag,
        i.snapshot_gap_days

    FROM inventory i

    LEFT JOIN products p
        ON i.product_id = p.product_id

    LEFT JOIN stores st
        ON i.store_id = st.store_id

    LEFT JOIN suppliers s
        ON i.supplier_id = s.supplier_id

    LEFT JOIN dates d
        ON i.inventory_date = d.full_date

)

SELECT

    inventory_key,

    product_key,
    date_key,
    store_key,
    supplier_key,

    beginning_stock,
    purchased_quantity,
    sold_quantity,
    ending_stock,

    inventory_value,
    stock_turnover_ratio,
    supplier_contribution_percentage,

    snapshot_gap_flag,
    snapshot_gap_days

FROM final