{{ config(materialized = 'table') }}

WITH flattened_campaigns AS (

    SELECT
        src.source_file,
        src.row_number,
        src.loaded_at,
        src.batch_id,
        item.value AS campaign_data
    FROM {{ ref('stg_bronze__campaign_data') }} AS src,
    LATERAL FLATTEN(
        INPUT => src.raw_data:campaigns_data
    ) AS item

),

standardized AS (

    SELECT
        source_file,
        row_number,
        loaded_at,
        batch_id,

        NULLIF(
            TRIM(campaign_data:campaign_id::VARCHAR),
            ''
        ) AS campaign_id,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(campaign_data:campaign_name::VARCHAR),
                '[^A-Za-z0-9 ''&-]',
                ''
            )
        ) AS campaign_name,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(campaign_data:campaign_type::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS campaign_type,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(campaign_data:target_audience::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS target_audience_segmentation,

        TRY_TO_DATE(
            NULLIF(
                TRIM(campaign_data:start_date::VARCHAR),
                ''
            )
        ) AS start_date,

        TRY_TO_DATE(
            NULLIF(
                TRIM(campaign_data:end_date::VARCHAR),
                ''
            )
        ) AS end_date,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(campaign_data:budget::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS budget,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(campaign_data:total_cost::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS total_cost,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(campaign_data:total_revenue::VARCHAR),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS total_revenue,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(campaign_data:roi_calculation::VARCHAR),
                ''
            ),
            18,
            4
        ) AS roi_calculation,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(campaign_data:last_modified_date::VARCHAR),
                ''
            )
        ) AS last_modified_date

    FROM flattened_campaigns

),

with_duration AS (

    SELECT
        standardized.*,

        IFF(
            start_date IS NOT NULL
            AND end_date IS NOT NULL,
            DATEDIFF(DAY, start_date, end_date),
            NULL
        ) AS campaign_duration_days

    FROM standardized

),

latest_campaigns AS (

    SELECT
        *
    FROM with_duration

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            COALESCE(
                campaign_id,
                CONCAT(
                    '_NULL_',
                    source_file,
                    '_',
                    row_number
                )
            )
        ORDER BY
            last_modified_date DESC NULLS LAST,
            loaded_at DESC,
            source_file DESC,
            row_number DESC
    ) = 1

)

SELECT *
FROM latest_campaigns