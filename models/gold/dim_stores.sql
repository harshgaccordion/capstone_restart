{{ config(
    materialized='table'
) }}

WITH stores AS (

    SELECT
        store_id,
        store_name,
        standardized_address,
        region,
        store_type,
        opening_date,
        store_size_category

    FROM {{ ref('silver_store_data') }}

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key([
            'store_id'
        ]) }} AS store_key,

        store_id,

        store_name,

        standardized_address AS address,

        region,

        store_type,

        opening_date,

        store_size_category AS size_category

    FROM stores

)

SELECT *

FROM final