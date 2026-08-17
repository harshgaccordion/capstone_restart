{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'product_history_key',
    on_schema_change = 'sync_all_columns'
) }}

WITH source_files AS (

    SELECT
        SOURCE_FILE,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__product_data') }}

    {% if is_incremental() %}

        WHERE TRY_TO_DATE(
            REGEXP_SUBSTR(
                SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) > (
            SELECT COALESCE(
                MAX(source_snapshot_date),
                DATE '1900-01-01'
            )
            FROM {{ this }}
        )

    {% endif %}

),

flattened AS (

    SELECT
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS source_snapshot_date,

        entry.value AS product_data

    FROM source_files,
    LATERAL FLATTEN(
        INPUT => RAW_DATA:products_data
    ) AS entry

),

cleaned AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'product_data:product_id::VARCHAR',
            'source_snapshot_date'
        ]) }} AS product_history_key,

        SOURCE_FILE,
        source_snapshot_date,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(product_data:product_id::VARCHAR),
            ''
        ) AS product_id,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:name::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_name,

        TRIM(
            CONCAT_WS(
                ' - ',
                NULLIF(
                    TRIM(product_data:name::VARCHAR),
                    ''
                ),
                NULLIF(
                    TRIM(product_data:short_description::VARCHAR),
                    ''
                ),
                NULLIF(
                    TRIM(product_data:technical_specs::VARCHAR),
                    ''
                )
            )
        ) AS full_description,

        TRIM(
            REGEXP_REPLACE(
                product_data:short_description::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%-]',
                ''
            )
        ) AS short_description,

        TRIM(
            REGEXP_REPLACE(
                product_data:technical_specs::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%_=-]',
                ''
            )
        ) AS technical_specs,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:category::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS category,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:subcategory::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS subcategory,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(product_data:product_line::VARCHAR)
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_line,

        INITCAP(
            TRIM(product_data:brand::VARCHAR)
        ) AS brand,

        INITCAP(
            TRIM(product_data:color::VARCHAR)
        ) AS color,

        INITCAP(
            TRIM(product_data:size::VARCHAR)
        ) AS size,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(product_data:unit_price::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS unit_price,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(product_data:cost_price::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS cost_price,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(product_data:stock_quantity::VARCHAR),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(product_data:reorder_level::VARCHAR),
                ''
            )
        ) AS reorder_level,

        NULLIF(
            TRIM(product_data:supplier_id::VARCHAR),
            ''
        ) AS supplier_id,

        TRIM(
            product_data:dimensions::VARCHAR
        ) AS dimensions,

        TRIM(
            product_data:warranty_period::VARCHAR
        ) AS warranty_period,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    LOWER(
                        TRIM(product_data:weight::VARCHAR)
                    ),
                    '[^0-9.\-]',
                    ''
                ),
                ''
            ),
            10,
            2
        ) AS weight_kg,

        TRY_TO_DATE(
            NULLIF(
                TRIM(product_data:launch_date::VARCHAR),
                ''
            )
        ) AS launch_date,

        COALESCE(
            product_data:is_featured::BOOLEAN,
            FALSE
        ) AS is_featured,

        TRY_TO_DATE(
            NULLIF(
                TRIM(product_data:last_modified_date::VARCHAR),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

derived AS (

    SELECT
        cleaned.*,

        TRIM(
            CONCAT_WS(
                ' > ',
                NULLIF(category, ''),
                NULLIF(subcategory, ''),
                NULLIF(product_line, '')
            )
        ) AS product_hierarchy,

        CASE
            WHEN unit_price > 0
            THEN (
                (unit_price - cost_price)
                / unit_price
            ) * 100
            ELSE NULL
        END AS profit_margin_percentage,

        CASE
            WHEN stock_quantity IS NULL
              OR reorder_level IS NULL
                THEN NULL
            ELSE stock_quantity < reorder_level
        END AS low_stock_flag

    FROM cleaned

),

deduplicated AS (

    SELECT *
    FROM derived

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            product_id,
            source_snapshot_date
        ORDER BY
            SOURCE_FILE DESC,
            LOADED_AT DESC
    ) = 1

)

SELECT
    product_history_key,
    SOURCE_FILE,
    source_snapshot_date,
    LOADED_AT,
    BATCH_ID,
    product_id,
    product_name,
    full_description,
    short_description,
    technical_specs,
    category,
    subcategory,
    product_line,
    product_hierarchy,
    brand,
    color,
    size,
    unit_price,
    cost_price,
    profit_margin_percentage,
    stock_quantity,
    reorder_level,
    low_stock_flag,
    supplier_id,
    dimensions,
    weight_kg,
    warranty_period,
    is_featured,
    launch_date,
    last_modified_date
FROM deduplicated