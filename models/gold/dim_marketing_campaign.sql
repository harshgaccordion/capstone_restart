{{ config(
    materialized='table'
) }}

WITH campaigns AS (

    SELECT

        campaign_id,
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

        /*
           SURROGATE KEY

           Generated from the natural Campaign ID
           using dbt_utils.
        */

        {{ dbt_utils.generate_surrogate_key([
            'campaign_id'
        ]) }} AS campaign_key,


        /*
           NATURAL KEY
        */

        campaign_id,


        /*
           TARGET AUDIENCE
        */

        target_audience_segmentation
            AS target_audience_segment,


        /*
           CAMPAIGN BUDGET
        */

        budget,


        /*
           CAMPAIGN DURATION

           Number of days between start and end dates.
        */

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