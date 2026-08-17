{{ config(
    materialized = 'table'
) }}

WITH product_store AS (

    SELECT DISTINCT
        product_id,
        store_id
    FROM {{ ref('silver_order_item_data') }}
    WHERE product_id IS NOT NULL
      AND store_id IS NOT NULL

),

product_history AS (

    SELECT
        product_history_key,
        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        supplier_id,
        cost_price
    FROM {{ ref('silver_product_history_data') }}

),

inventory_snapshots AS (

    SELECT
        history.product_id,
        relation.store_id,
        history.source_snapshot_date AS inventory_date,
        history.stock_quantity AS ending_stock,
        history.reorder_level,
        history.supplier_id,
        history.cost_price
    FROM product_history AS history
    JOIN product_store AS relation
        ON history.product_id = relation.product_id

),

with_beginning_inventory AS (

    SELECT
        product_id,
        store_id,
        inventory_date,

        LAG(ending_stock) OVER (
            PARTITION BY product_id, store_id
            ORDER BY inventory_date
        ) AS beginning_stock,

        ending_stock,
        reorder_level,
        supplier_id,
        cost_price
    FROM inventory_snapshots

),

completed_sales AS (

    SELECT
        product_id,
        store_id,
        order_date AS inventory_date,
        SUM(quantity) AS sold_quantity
    FROM {{ ref('silver_order_item_data') }}
    WHERE LOWER(order_status) IN ('completed', 'delivered')
      AND product_id IS NOT NULL
      AND store_id IS NOT NULL
      AND order_date IS NOT NULL
    GROUP BY
        product_id,
        store_id,
        order_date

),

combined AS (

    SELECT
        inventory.product_id,
        inventory.store_id,
        inventory.inventory_date,
        inventory.beginning_stock,
        COALESCE(sales.sold_quantity, 0) AS sold_quantity,
        inventory.ending_stock,
        inventory.reorder_level,
        inventory.supplier_id,
        inventory.cost_price
    FROM with_beginning_inventory AS inventory
    LEFT JOIN completed_sales AS sales
        ON inventory.product_id = sales.product_id
        AND inventory.store_id = sales.store_id
        AND inventory.inventory_date = sales.inventory_date

),

calculated AS (

    SELECT
        product_id,
        store_id,
        inventory_date,
        beginning_stock,
        sold_quantity,
        ending_stock,

        COALESCE(ending_stock, 0)
        - COALESCE(beginning_stock, 0)
        + COALESCE(sold_quantity, 0) AS purchased_quantity,

        CASE
            WHEN ending_stock IS NOT NULL
             AND cost_price IS NOT NULL
            THEN ending_stock * cost_price
            ELSE NULL
        END AS inventory_value,

        (
            COALESCE(beginning_stock, 0)
            + COALESCE(ending_stock, 0)
        ) / 2.0 AS average_inventory,

        reorder_level,
        supplier_id,
        cost_price
    FROM combined

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'product_id',
            'store_id',
            'inventory_date'
        ]) }} AS inventory_key,

        product_id,
        store_id,
        inventory_date,
        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,
        inventory_value,

        CASE
            WHEN average_inventory > 0
            THEN sold_quantity / average_inventory
            ELSE NULL
        END AS stock_turnover_ratio,

        CASE
            WHEN purchased_quantity > 0
            THEN 100.0
            ELSE 0.0
        END AS supplier_contribution_percentage,

        reorder_level,
        supplier_id,

        CASE
            WHEN LAG(inventory_date) OVER (
                PARTITION BY product_id, store_id
                ORDER BY inventory_date
            ) IS NULL
            THEN FALSE

            WHEN DATEDIFF(
                DAY,
                LAG(inventory_date) OVER (
                    PARTITION BY product_id, store_id
                    ORDER BY inventory_date
                ),
                inventory_date
            ) > 1
            THEN TRUE

            ELSE FALSE
        END AS snapshot_gap_flag,

        CASE
            WHEN LAG(inventory_date) OVER (
                PARTITION BY product_id, store_id
                ORDER BY inventory_date
            ) IS NULL
            THEN 0

            ELSE DATEDIFF(
                DAY,
                LAG(inventory_date) OVER (
                    PARTITION BY product_id, store_id
                    ORDER BY inventory_date
                ),
                inventory_date
            )
        END AS snapshot_gap_days,

        CASE
            WHEN ending_stock IS NOT NULL
             AND reorder_level IS NOT NULL
             AND ending_stock < reorder_level
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag,

        CASE
            WHEN purchased_quantity < 0
            THEN TRUE
            ELSE FALSE
        END AS negative_inferred_purchase_flag

    FROM calculated

)

SELECT *
FROM final