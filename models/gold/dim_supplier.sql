{{ config(
    materialized='table'
) }}

WITH suppliers AS (

    SELECT
        supplier_id,
        supplier_name,
        contact_name,
        email,
        phone,
        standardized_address,
        payment_terms,
        supplier_type

    FROM {{ ref('silver_supplier_data') }}

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key([
            'supplier_id'
        ]) }} AS supplier_key,

        supplier_id,

        supplier_name,

        CONCAT_WS(
            ' | ',
            NULLIF(
                TRIM(contact_name),
                ''
            ),
            NULLIF(
                TRIM(email),
                ''
            ),
            NULLIF(
                TRIM(phone),
                ''
            )
        ) AS contact_information,

        payment_terms,

        supplier_type

    FROM suppliers

)

SELECT *

FROM final