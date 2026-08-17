{% snapshot snp_customer %}

{{
    config(
        target_schema='snapshot',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH flattened AS (

    SELECT
        customer.value:customer_id::VARCHAR AS customer_id,

        TRY_TO_TIMESTAMP_NTZ(
            customer.value:last_modified_date::STRING
        ) AS last_modified_date,

        customer.value AS raw_customer_data,

        cust.SOURCE_FILE,
        cust.LOADED_AT,
        cust.BATCH_ID

    FROM {{ ref('stg_bronze__customer_data') }} AS cust,

    LATERAL FLATTEN(
        INPUT => cust.RAW_DATA:customers_data
    ) AS customer

),

latest_customer AS (

    SELECT
        customer_id,
        last_modified_date,
        raw_customer_data,
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY
            last_modified_date DESC,
            LOADED_AT DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    customer_id,
    last_modified_date,
    raw_customer_data,
    SOURCE_FILE,
    LOADED_AT,
    BATCH_ID

FROM latest_customer

{% endsnapshot %}