{{ config(
    materialized = 'table'
) }}

WITH flattened AS (

    SELECT
        src.SOURCE_FILE,
        src.ROW_NUMBER,
        src.LOADED_AT,
        src.BATCH_ID,
        item.value AS product_data
    FROM {{ ref('stg_bronze__product_data') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.RAW_DATA:products_data
    ) AS item

),

cleaned AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
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
            REGEXP_REPLACE(
                TRIM(product_data:brand::VARCHAR),
                '[^A-Za-z0-9 ''&.-]',
                ''
            )
        ) AS brand,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(product_data:color::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS color,

        TRIM(
            REGEXP_REPLACE(
                product_data:size::VARCHAR,
                '[^A-Za-z0-9 ''.-]',
                ''
            )
        ) AS size,

        NULLIF(
            TRIM(product_data:supplier_id::VARCHAR),
            ''
        ) AS supplier_id,

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

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(product_data:stock_quantity::VARCHAR),
                    ''
                )
            ),
            0
        ) AS stock_quantity,

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(product_data:reorder_level::VARCHAR),
                    ''
                )
            ),
            0
        ) AS reorder_level,

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

        CONCAT_WS(
            ' - ',
            NULLIF(TRIM(product_name), ''),
            NULLIF(TRIM(short_description), ''),
            NULLIF(TRIM(technical_specs), '')
        ) AS product_full_description,

        CONCAT_WS(
            ' > ',
            NULLIF(TRIM(category), ''),
            NULLIF(TRIM(subcategory), ''),
            NULLIF(TRIM(product_line), '')
        ) AS product_hierarchy,

        CASE
            WHEN unit_price > 0
            THEN (
                (unit_price - cost_price) / unit_price
            ) * 100
            ELSE NULL
        END AS profit_margin_percentage,

        stock_quantity < reorder_level AS low_stock_flag

    FROM cleaned

),

deduplicated AS (

    SELECT *
    FROM derived

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
            product_id,
            CONCAT(
                '_NULL_',
                SOURCE_FILE,
                '_',
                ROW_NUMBER
            )
        )
        ORDER BY
            last_modified_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC
    ) = 1

)

SELECT *
FROM deduplicated