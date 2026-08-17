{{ config(
    materialized = 'table'
) }}

WITH flattened AS (

    SELECT
        src.SOURCE_FILE,
        src.ROW_NUMBER,
        src.LOADED_AT,
        src.BATCH_ID,
        emp.value AS employee_data
    FROM {{ ref('stg_bronze__employee_data') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.RAW_DATA:employees_data
    ) AS emp

),

cleaned AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(employee_data:employee_id::VARCHAR),
            ''
        ) AS employee_id,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(employee_data:first_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS first_name,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(employee_data:last_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS last_name,

        IFF(
            REGEXP_LIKE(
                LOWER(TRIM(employee_data:email::VARCHAR)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            ),
            LOWER(TRIM(employee_data:email::VARCHAR)),
            NULL
        ) AS email,

        NOT REGEXP_LIKE(
            LOWER(TRIM(employee_data:email::VARCHAR)),
            '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
            'i'
        ) AS invalid_email_flag,

        CASE
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(employee_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    8,
                    4
                )
            )

            ELSE NULL
        END AS phone,

        CASE
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(employee_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
            THEN FALSE

            ELSE TRUE
        END AS invalid_phone_flag,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(employee_data:role::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS job_title,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(employee_data:department::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS department,

        NULLIF(
            TRIM(employee_data:work_location::VARCHAR),
            ''
        ) AS store_id,

        TRY_TO_DATE(
            NULLIF(
                TRIM(employee_data:hire_date::VARCHAR),
                ''
            )
        ) AS hire_date,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(employee_data:salary::VARCHAR),
                ''
            ),
            18,
            2
        ) AS salary,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(employee_data:last_modified_date::VARCHAR),
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
                ' ',
                NULLIF(first_name, ''),
                NULLIF(last_name, '')
            )
        ) AS full_name

    FROM cleaned

),

deduplicated AS (

    SELECT *
    FROM derived

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
            employee_id,
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