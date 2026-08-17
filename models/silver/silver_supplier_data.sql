{{ config(
    materialized='table'
) }}

WITH source_data AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID

    FROM {{ ref('stg_bronze__supplier_data') }}

),

flattened_suppliers AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        supplier.value AS supplier_data

    FROM source_data s,

    LATERAL FLATTEN(
        INPUT => s.RAW_DATA:suppliers_data
    ) AS supplier

),

cleaned_suppliers AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        NULLIF(
            TRIM(
                supplier_data:supplier_id::VARCHAR
            ),
            ''
        ) AS supplier_id,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:supplier_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''&.,/-]',
                ''
            )
        ) AS supplier_name,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:contact_information:contact_person::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS contact_name,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    supplier_data:contact_information:email::VARCHAR
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )

            THEN LOWER(
                TRIM(
                    supplier_data:contact_information:email::VARCHAR
                )
            )

            ELSE NULL

        END AS email,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    supplier_data:contact_information:email::VARCHAR
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )

            THEN FALSE

            ELSE TRUE

        END AS invalid_email_flag,

        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            supplier_data:contact_information:phone::VARCHAR
                        ),
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
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        supplier_data:contact_information:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN FALSE

            ELSE TRUE

        END AS invalid_phone_flag,

        TRIM(
            supplier_data:contact_information:address::VARCHAR
        ) AS raw_address,

        TRIM(
            SPLIT_PART(
                supplier_data:contact_information:address::VARCHAR,
                ',',
                1
            )
        ) AS street,

        INITCAP(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    2
                )
            )
        ) AS city,

        UPPER(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    3
                )
            )
        ) AS state,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )

            THEN TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    4
                )
            )

            ELSE NULL

        END AS postal_code,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                '^[0-9]{5}(-[0-9]{4})?$'
            )

            THEN FALSE

            ELSE TRUE

        END AS invalid_postal_code_flag,

        UPPER(
            TRIM(
                SPLIT_PART(
                    supplier_data:contact_information:address::VARCHAR,
                    ',',
                    5
                )
            )
        ) AS country,

        CONCAT_WS(
            ', ',

            NULLIF(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        1
                    )
                ),
                ''
            ),

            NULLIF(
                INITCAP(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            2
                        )
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            3
                        )
                    )
                ),
                ''
            ),

            NULLIF(
                TRIM(
                    SPLIT_PART(
                        supplier_data:contact_information:address::VARCHAR,
                        ',',
                        4
                    )
                ),
                ''
            ),

            NULLIF(
                UPPER(
                    TRIM(
                        SPLIT_PART(
                            supplier_data:contact_information:address::VARCHAR,
                            ',',
                            5
                        )
                    )
                ),
                ''
            )

        ) AS standardized_address,

        INITCAP(
            TRIM(
                supplier_data:payment_terms::VARCHAR
            )
        ) AS payment_terms,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    supplier_data:supplier_type::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS supplier_type,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    supplier_data:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM flattened_suppliers

),

deduplicated_suppliers AS (

    SELECT *

    FROM cleaned_suppliers

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY

            CASE

                WHEN supplier_id IS NOT NULL

                    THEN supplier_id

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

FROM deduplicated_suppliers