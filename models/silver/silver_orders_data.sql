{{ config(
    materialized = 'table'
) }}

WITH flattened_orders AS (

    SELECT
        src.SOURCE_FILE,
        src.ROW_NUMBER,
        src.LOADED_AT,
        src.BATCH_ID,
        order_obj.value AS order_data
    FROM {{ ref('stg_bronze__order_data') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.RAW_DATA:orders_data
    ) AS order_obj

),

order_header AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(TRIM(order_data:order_id::VARCHAR), '') AS order_id,

        NULLIF(TRIM(order_data:customer_id::VARCHAR), '') AS customer_id,

        NULLIF(TRIM(order_data:store_id::VARCHAR), '') AS store_id,

        NULLIF(TRIM(order_data:employee_id::VARCHAR), '') AS employee_id,

        NULLIF(TRIM(order_data:campaign_id::VARCHAR), '') AS campaign_id,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(TRIM(order_data:order_date::VARCHAR), '')
        ) AS order_datetime,

        TRY_TO_DATE(
            NULLIF(TRIM(order_data:order_date::VARCHAR), '')
        ) AS order_date,

        TRY_TO_DATE(
            NULLIF(TRIM(order_data:shipping_date::VARCHAR), '')
        ) AS shipping_date,

        TRY_TO_DATE(
            NULLIF(TRIM(order_data:delivery_date::VARCHAR), '')
        ) AS delivery_date,

        TRY_TO_DATE(
            NULLIF(TRIM(order_data:estimated_delivery_date::VARCHAR), '')
        ) AS estimated_delivery_date,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(TRIM(order_data:discount_amount::VARCHAR), ''),
                18,
                6
            ),
            0
        ) AS order_discount_amount,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(order_data:shipping_cost::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS shipping_cost,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(order_data:tax_amount::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS tax_amount,

        order_data:order_items AS order_items

    FROM flattened_orders

),

cleaned_items AS (

    SELECT
        orders.SOURCE_FILE,
        orders.ROW_NUMBER,
        orders.LOADED_AT,
        orders.BATCH_ID,
        orders.order_id,

        NULLIF(
            TRIM(item.value:product_id::VARCHAR),
            ''
        ) AS product_id,

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(item.value:quantity::VARCHAR),
                    ''
                )
            ),
            0
        ) AS quantity,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(item.value:unit_price::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS unit_price,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(item.value:cost_price::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS cost_price,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(item.value:discount_amount::VARCHAR),
                    ''
                ),
                18,
                6
            ),
            0
        ) AS item_discount_amount

    FROM order_header AS orders,
    LATERAL FLATTEN(
        INPUT => orders.order_items
    ) AS item

),

order_item_aggregates AS (

    SELECT
        order_id,

        COUNT(product_id) AS total_items,

        SUM(quantity) AS total_quantity,

        SUM(quantity * unit_price) AS total_amount,

        SUM(quantity * cost_price) AS total_cost,

        SUM(item_discount_amount) AS total_discount,

        SUM(
            quantity * unit_price * (1 - item_discount_amount)
        ) AS line_revenue,

        SUM(quantity * cost_price) AS line_cost

    FROM cleaned_items

    GROUP BY order_id

),

combined AS (

    SELECT
        header.SOURCE_FILE,
        header.ROW_NUMBER,
        header.LOADED_AT,
        header.BATCH_ID,

        header.order_id,
        header.customer_id,
        header.store_id,
        header.employee_id,
        header.campaign_id,

        header.order_datetime,
        header.order_date,
        header.shipping_date,
        header.delivery_date,
        header.estimated_delivery_date,

        header.order_discount_amount,
        header.shipping_cost,
        header.tax_amount,

        COALESCE(items.total_items, 0) AS total_items,

        COALESCE(items.total_quantity, 0) AS total_quantity,

        COALESCE(items.total_amount, 0.00) AS total_amount,

        COALESCE(items.total_cost, 0.00) AS total_cost,

        COALESCE(items.total_discount, 0.00) AS total_discount,

        COALESCE(items.line_revenue, 0.00) AS line_revenue,

        COALESCE(items.line_cost, 0.00) AS line_cost

    FROM order_header AS header

    LEFT JOIN order_item_aggregates AS items
        ON header.order_id = items.order_id

),

derived AS (

    SELECT
        combined.*,

        EXTRACT(
            HOUR FROM order_datetime
        ) AS order_hour,

        CASE
            WHEN EXTRACT(HOUR FROM order_datetime) >= 5
             AND EXTRACT(HOUR FROM order_datetime) < 12
                THEN 'Morning'

            WHEN EXTRACT(HOUR FROM order_datetime) >= 12
             AND EXTRACT(HOUR FROM order_datetime) < 17
                THEN 'Afternoon'

            WHEN EXTRACT(HOUR FROM order_datetime) >= 17
             AND EXTRACT(HOUR FROM order_datetime) < 22
                THEN 'Evening'

            ELSE 'Night'
        END AS order_time_of_day,

        WEEK(order_date) AS order_week,

        MONTH(order_date) AS order_month,

        QUARTER(order_date) AS order_quarter,

        YEAR(order_date) AS order_year,

        (
            line_revenue * (1 - order_discount_amount)
            - line_cost
            - shipping_cost
            - tax_amount
        ) AS profit_amount,

        CASE
            WHEN line_revenue > 0
            THEN (
                (
                    line_revenue * (1 - order_discount_amount)
                    - line_cost
                    - shipping_cost
                    - tax_amount
                ) / line_revenue
            ) * 100
            ELSE NULL
        END AS profit_margin_percentage,

        DATEDIFF(
            DAY,
            order_date,
            shipping_date
        ) AS processing_days,

        DATEDIFF(
            DAY,
            shipping_date,
            delivery_date
        ) AS shipping_days,

        CASE
            WHEN delivery_date IS NOT NULL
             AND delivery_date <= estimated_delivery_date
                THEN 'On Time'

            WHEN delivery_date IS NOT NULL
             AND delivery_date > estimated_delivery_date
                THEN 'Delayed'

            WHEN delivery_date IS NULL
             AND CURRENT_DATE() > estimated_delivery_date
                THEN 'Potentially Delayed'

            ELSE 'In Transit'
        END AS delivery_status

    FROM combined

),

deduplicated AS (

    SELECT *
    FROM derived

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
            order_id,
            CONCAT(
                '_NULL_',
                SOURCE_FILE,
                '_',
                ROW_NUMBER
            )
        )
        ORDER BY
            order_datetime DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC
    ) = 1

)

SELECT *
FROM deduplicated