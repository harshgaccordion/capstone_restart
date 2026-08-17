{{ config(
    materialized = 'table'
) }}

WITH flattened AS (

    SELECT
        src.SOURCE_FILE,
        src.ROW_NUMBER,
        src.LOADED_AT,
        src.BATCH_ID,
        cust.value AS customer_data
    FROM {{ ref('snp_customer') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.RAW_customer_DATA:customers_data
    ) AS cust

),

cleaned AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(customer_data:customer_id::VARCHAR),
            ''
        ) AS customer_id,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(customer_data:first_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS first_name,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(customer_data:last_name::VARCHAR),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS last_name,

        IFF(
            REGEXP_LIKE(
                LOWER(TRIM(customer_data:email::VARCHAR)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            ),
            LOWER(TRIM(customer_data:email::VARCHAR)),
            NULL
        ) AS email,

        NOT REGEXP_LIKE(
            LOWER(TRIM(customer_data:email::VARCHAR)),
            '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
            'i'
        ) AS invalid_email_flag,

        CASE
            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(customer_data:phone::VARCHAR),
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
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 10
            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                )
            ) = 11
            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(customer_data:phone::VARCHAR),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'
            THEN FALSE

            ELSE TRUE
        END AS invalid_phone_flag,

        INITCAP(
            TRIM(customer_data:address:street::VARCHAR)
        ) AS street,

        INITCAP(
            TRIM(customer_data:address:city::VARCHAR)
        ) AS city,

        UPPER(
            TRIM(customer_data:address:state::VARCHAR)
        ) AS state,

        UPPER(
            TRIM(customer_data:address:country::VARCHAR)
        ) AS country,

        TRIM(
            customer_data:address:zip_code::VARCHAR
        ) AS zip_code,

        CONCAT_WS(
            ', ',
            NULLIF(
                INITCAP(TRIM(customer_data:address:street::VARCHAR)),
                ''
            ),
            NULLIF(
                INITCAP(TRIM(customer_data:address:city::VARCHAR)),
                ''
            ),
            NULLIF(
                UPPER(TRIM(customer_data:address:state::VARCHAR)),
                ''
            ),
            NULLIF(
                TRIM(customer_data:address:zip_code::VARCHAR),
                ''
            ),
            NULLIF(
                UPPER(TRIM(customer_data:address:country::VARCHAR)),
                ''
            )
        ) AS standardized_address,

        UPPER(
            TRIM(customer_data:income_bracket::VARCHAR)
        ) AS income_bracket,

        INITCAP(
            TRIM(customer_data:occupation::VARCHAR)
        ) AS occupation,

        UPPER(
            TRIM(customer_data:loyalty_tier::VARCHAR)
        ) AS loyalty_tier,

        UPPER(
            TRIM(customer_data:preferred_communication::VARCHAR)
        ) AS preferred_communication,

        UPPER(
            TRIM(customer_data:preferred_payment_method::VARCHAR)
        ) AS preferred_payment_method,

        COALESCE(
            customer_data:marketing_opt_in::BOOLEAN,
            FALSE
        ) AS marketing_opt_in,

        TRY_TO_DATE(
            NULLIF(TRIM(customer_data:birth_date::VARCHAR), '')
        ) AS birth_date,

        TRY_TO_DATE(
            NULLIF(TRIM(customer_data:registration_date::VARCHAR), '')
        ) AS registration_date,

        TRY_TO_DATE(
            NULLIF(TRIM(customer_data:last_purchase_date::VARCHAR), '')
        ) AS last_purchase_date,

        TRY_TO_DATE(
            NULLIF(TRIM(customer_data:last_modified_date::VARCHAR), '')
        ) AS last_modified_date,

        COALESCE(
            TRY_TO_NUMBER(customer_data:total_purchases::VARCHAR),
            0
        ) AS total_purchases,

        COALESCE(
            TRY_TO_DECIMAL(
                customer_data:total_spend::VARCHAR,
                18,
                2
            ),
            0.00
        ) AS total_spend

    FROM flattened

),

derived AS (

    SELECT
        c.*,

        TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(c.first_name, ''),
                NULLIF(c.last_name, '')
            )
        ) AS full_name,

        CASE
            WHEN c.birth_date IS NOT NULL
            THEN DATEDIFF(
                YEAR,
                c.birth_date,
                CURRENT_DATE()
            )
            ELSE NULL
        END AS customer_age

    FROM cleaned AS c

),

segmented AS (

    SELECT
        d.*,

        CASE
            WHEN customer_age BETWEEN 18 AND 35
                THEN 'Young'
            WHEN customer_age BETWEEN 36 AND 55
                THEN 'Middle-aged'
            WHEN customer_age >= 56
                THEN 'Senior'
            ELSE NULL
        END AS customer_segment

    FROM derived AS d

),

deduplicated AS (

    SELECT *
    FROM segmented

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
            customer_id,
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