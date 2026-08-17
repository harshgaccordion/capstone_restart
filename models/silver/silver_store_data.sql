{{ config(
    materialized = 'table'
) }}

WITH source_data AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__store_data') }}

),

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        store.value AS store_data
    FROM source_data s,
    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:stores_data
    ) AS store

),

cleaned AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(store_data:store_id::VARCHAR),
            ''
        ) AS store_id,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    COALESCE(
                        store_data:store_name::VARCHAR,
                        store_data:name::VARCHAR
                    )
                )
            ),
            '[^A-Za-z0-9]+',
            ''
        ) AS store_name,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    COALESCE(
                        store_data:address:street::VARCHAR,
                        store_data:street::VARCHAR
                    )
                ),
                '[^A-Za-z0-9 ''#.,/-]',
                ''
            )
        ) AS street,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    COALESCE(
                        store_data:address:city::VARCHAR,
                        store_data:city::VARCHAR
                    )
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS city,

        UPPER(
            TRIM(
                COALESCE(
                    store_data:address:state::VARCHAR,
                    store_data:state::VARCHAR
                )
            )
        ) AS state,

        UPPER(
            TRIM(
                COALESCE(
                    store_data:address:country::VARCHAR,
                    store_data:country::VARCHAR
                )
            )
        ) AS country,

        CASE
            WHEN REGEXP_LIKE(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN TRIM(
                COALESCE(
                    store_data:address:zip_code::VARCHAR,
                    store_data:address:postal_code::VARCHAR,
                    store_data:zip_code::VARCHAR,
                    store_data:postal_code::VARCHAR
                )
            )
            ELSE NULL
        END AS postal_code,

        CASE
            WHEN REGEXP_LIKE(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_postal_code_flag,

        CONCAT_WS(
            ', ',
            NULLIF(
                INITCAP(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE(
                                store_data:address:street::VARCHAR,
                                store_data:street::VARCHAR
                            )
                        ),
                        '[^A-Za-z0-9 ''#.,/-]',
                        ''
                    )
                ),
                ''
            ),
            NULLIF(
                INITCAP(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE(
                                store_data:address:city::VARCHAR,
                                store_data:city::VARCHAR
                            )
                        ),
                        '[^A-Za-z0-9 ''-]',
                        ''
                    )
                ),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(
                        COALESCE(
                            store_data:address:state::VARCHAR,
                            store_data:state::VARCHAR
                        )
                    )
                ),
                ''
            ),
            NULLIF(
                TRIM(
                    COALESCE(
                        store_data:address:zip_code::VARCHAR,
                        store_data:address:postal_code::VARCHAR,
                        store_data:zip_code::VARCHAR,
                        store_data:postal_code::VARCHAR
                    )
                ),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(
                        COALESCE(
                            store_data:address:country::VARCHAR,
                            store_data:country::VARCHAR
                        )
                    )
                ),
                ''
            )
        ) AS standardized_address,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(store_data:region::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS region,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(store_data:store_type::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS store_type,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    TRIM(store_data:size_sq_ft::VARCHAR),
                    ''
                ),
                18,
                2
            ),
            0
        ) AS size_sq_ft,

        TRY_TO_DATE(
            NULLIF(
                TRIM(store_data:opening_date::VARCHAR),
                ''
            )
        ) AS opening_date,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(store_data:sales_target::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS sales_target,

        COALESCE(
            TRY_TO_DECIMAL(
                NULLIF(
                    REGEXP_REPLACE(
                        TRIM(store_data:current_sales::VARCHAR),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) AS current_sales,

        COALESCE(
            TRY_TO_NUMBER(
                NULLIF(
                    TRIM(store_data:employee_count::VARCHAR),
                    ''
                )
            ),
            0
        ) AS employee_count,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(store_data:last_modified_date::VARCHAR),
                ''
            )
        ) AS last_modified_date

    FROM flattened

),

derived AS (

    SELECT
        s.*,

        CASE
            WHEN s.size_sq_ft < 5000
                THEN 'Small'
            WHEN s.size_sq_ft BETWEEN 5000 AND 10000
                THEN 'Medium'
            WHEN s.size_sq_ft > 10000
                THEN 'Large'
            ELSE NULL
        END AS store_size_category,

        CASE
            WHEN s.opening_date IS NOT NULL
             AND s.opening_date <= CURRENT_DATE()
            THEN DATEDIFF(
                YEAR,
                s.opening_date,
                CURRENT_DATE()
            )
            ELSE NULL
        END AS store_age_years,

        CASE
            WHEN s.sales_target > 0
            THEN (
                s.current_sales / s.sales_target
            ) * 100
            ELSE NULL
        END AS sales_target_achievement_percentage,

        CASE
            WHEN s.size_sq_ft > 0
            THEN s.current_sales / s.size_sq_ft
            ELSE NULL
        END AS revenue_per_sq_ft,

        CASE
            WHEN s.employee_count > 0
            THEN s.current_sales / s.employee_count
            ELSE NULL
        END AS employee_efficiency

    FROM cleaned s

),

performance_flagged AS (

    SELECT
        d.*,

        CASE
            WHEN d.sales_target_achievement_percentage < 90
                THEN TRUE
            ELSE FALSE
        END AS performance_issue_flag

    FROM derived d

),

deduplicated AS (

    SELECT *
    FROM performance_flagged

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            CASE
                WHEN store_id IS NOT NULL
                    THEN store_id
                ELSE CONCAT(
                    '_NULL_',
                    SOURCE_FILE,
                    '_',
                    ROW_NUMBER
                )
            END
        ORDER BY
            last_modified_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC
    ) = 1

)

SELECT *
FROM deduplicated