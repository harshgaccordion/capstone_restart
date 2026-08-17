{{ config(
    materialized='table'
) }}

WITH campaigns AS (

    SELECT
        campaign_id,
        campaign_name,
        campaign_type,
        target_audience_segmentation,
        budget,
        campaign_duration_days,
        roi_calculation,
        start_date,
        end_date

    FROM {{ ref('silver_campaign_data') }}

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key([
            'campaign_id'
        ]) }} AS campaign_key,

        campaign_id,

        campaign_name,

        campaign_type,

        target_audience_segmentation
            AS target_audience_segment,

        budget,

        campaign_duration_days
            AS duration,

        roi_calculation
            AS roi,

        start_date,
        end_date

    FROM campaigns

)

SELECT *

FROM final