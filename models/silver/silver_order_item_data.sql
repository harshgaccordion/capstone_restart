{{ config(
    materialized = 'table'
) }}

WITH flattened_orders AS (

    SELECT
        src.SOURCE_FILE,
        src.ROW_NUMBER,
        src.LOADED_AT,
        src.BATCH_ID,
        ord.value AS order_data
    FROM {{ ref('stg_bronze__order_data') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.RAW_DATA:orders_data
    ) AS ord

),

order_header AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(order_data:order_id::VARCHAR),
            ''
        ) AS order_id,

        NULLIF(
            TRIM(order_data:customer_id::VARCHAR),
            ''
        ) AS customer_id,

        NULLIF(
            TRIM(order_data:store_id::VARCHAR),
            ''
        ) AS store_id,

        TRY_TO_DATE(
            NULLIF(
                TRIM(order_data:order_date::VARCHAR),
                ''
            )
        ) AS order_date,

        NULLIF(
            TRIM(order_data:order_status::VARCHAR),
            ''
        ) AS order_status,

        order_data:order_items AS order_items

    FROM flattened_orders

),

flattened_items AS (

    SELECT
        header.SOURCE_FILE,
        header.ROW_NUMBER,
        header.LOADED_AT,
        header.BATCH_ID,
        header.order_id,
        header.customer_id,
        header.store_id,
        header.order_date,
        header.order_status,
        item.index + 1 AS item_number,
        item.value AS item_data
    FROM order_header AS header,
    LATERAL FLATTEN(
        INPUT => header.order_items
    ) AS item

),

cleaned AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'order_id',
            'item_number'
        ]) }} AS order_item_key,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        order_id,
        item_number,
        order_date,
        order_status,
        customer_id,
        store_id,

        NULLIF(
            TRIM(item_data:product_id::VARCHAR),
            ''
        ) AS product_id,

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(item_data:quantity::VARCHAR),
                    ''
                )
            ),
            0
        ) AS quantity,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(item_data:unit_price::VARCHAR),
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
                        TRIM(item_data:cost_price::VARCHAR),
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
                    TRIM(item_data:discount_amount::VARCHAR),
                    ''
                ),
                18,
                6
            ),
            0
        ) AS discount_percentage

    FROM flattened_items

),

derived AS (

    SELECT
        cleaned.*,

        COALESCE(
            discount_percentage / 100,
            0
        ) AS discount_rate

    FROM cleaned

),

deduplicated AS (

    SELECT *
    FROM derived

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            order_id,
            item_number
        ORDER BY
            order_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC
    ) = 1

)

SELECT
    order_item_key,
    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID,
    order_id,
    item_number,
    order_date,
    order_status,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    cost_price,
    discount_percentage,
    discount_rate
FROM deduplicated