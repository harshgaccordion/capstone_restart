{{ config(
    materialized = 'view',
    schema = 'reporting'
) }}

WITH inventory_records AS (

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
        snapshot_gap_flag,
        snapshot_gap_days
    FROM {{ ref('fact_inventory') }}

),

supplier_details AS (

    SELECT
        supplier_key,
        supplier_id,
        supplier_name,
        supplier_type
    FROM {{ ref('dim_supplier') }}

),

store_details AS (

    SELECT
        store_key,
        store_id,
        store_name,
        region,
        store_type
    FROM {{ ref('dim_stores') }}

),

calendar AS (

    SELECT
        date_key,
        full_date,
        year,
        month,
        quarter
    FROM {{ ref('dim_date') }}

),

enriched_inventory AS (

    SELECT
        inv.inventory_key,
        inv.product_key,
        inv.date_key,
        inv.store_key,
        inv.supplier_key,

        cal.full_date,
        cal.year,
        cal.month,
        cal.quarter,

        sup.supplier_id,
        sup.supplier_name,
        sup.supplier_type,

        str.store_id,
        str.store_name,
        str.region,
        str.store_type,

        inv.beginning_stock,
        inv.purchased_quantity,
        inv.sold_quantity,
        inv.ending_stock,
        inv.inventory_value,

        inv.snapshot_gap_flag,
        inv.snapshot_gap_days,

        IFF(
            COALESCE(inv.snapshot_gap_flag, FALSE),
            'Delayed',
            'On Time'
        ) AS supply_status

    FROM inventory_records inv

    LEFT JOIN supplier_details sup
        ON sup.supplier_key = inv.supplier_key

    LEFT JOIN store_details str
        ON str.store_key = inv.store_key

    LEFT JOIN calendar cal
        ON cal.date_key = inv.date_key

)

SELECT
    supplier_key,
    supplier_id,
    supplier_name,
    supplier_type,

    store_key,
    store_id,
    store_name,
    region,
    store_type,

    date_key,
    full_date,
    year,
    month,
    quarter,

    supply_status,

    COUNT(DISTINCT inventory_key) AS inventory_snapshot_count,

    SUM(purchased_quantity) AS total_purchased_quantity,

    SUM(sold_quantity) AS total_sold_quantity,

    SUM(ending_stock) AS total_ending_inventory,

    SUM(inventory_value) AS total_inventory_value,

    AVG(snapshot_gap_days) AS average_snapshot_gap_days,

    COUNT(
        DISTINCT IFF(
            supply_status = 'On Time',
            inventory_key,
            NULL
        )
    ) AS on_time_snapshot_count,

    COUNT(
        DISTINCT IFF(
            supply_status = 'Delayed',
            inventory_key,
            NULL
        )
    ) AS delayed_snapshot_count,

    ROUND(
        100.0
        * COUNT(
            DISTINCT IFF(
                supply_status = 'On Time',
                inventory_key,
                NULL
            )
        )
        / NULLIF(
            COUNT(DISTINCT inventory_key),
            0
        ),
        2
    ) AS on_time_percentage,

    ROUND(
        100.0
        * COUNT(
            DISTINCT IFF(
                supply_status = 'Delayed',
                inventory_key,
                NULL
            )
        )
        / NULLIF(
            COUNT(DISTINCT inventory_key),
            0
        ),
        2
    ) AS delayed_percentage

FROM enriched_inventory

GROUP BY
    supplier_key,
    supplier_id,
    supplier_name,
    supplier_type,
    store_key,
    store_id,
    store_name,
    region,
    store_type,
    date_key,
    full_date,
    year,
    month,
    quarter,
    supply_status