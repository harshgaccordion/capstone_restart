{{ config(
    materialized='table'
) }}

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-04-01' as date)",
        end_date="cast('2024-09-28' as date)"
    ) }}

),

date_attributes AS (

    SELECT

        TO_NUMBER(
            TO_CHAR(
                DATE_DAY,
                'YYYYMMDD'
            )
        ) AS date_key,

        DATE_DAY AS full_date,



        YEAR(DATE_DAY) AS year,

        QUARTER(DATE_DAY) AS quarter,

        MONTH(DATE_DAY) AS month,


        WEEK(DATE_DAY) AS week,

        DAYOFWEEK(DATE_DAY) AS day_of_week,


        DAYNAME(DATE_DAY) AS day_name,


        CASE

            WHEN DATE_DAY IN (
                DATE '2024-05-27',
                DATE '2024-06-19',
                DATE '2024-07-04',
                DATE '2024-09-02'
            )

            THEN TRUE

            ELSE FALSE

        END AS holiday_flag,

        CASE

            WHEN MONTH(DATE_DAY) IN (12, 1, 2)
                THEN 'Winter'

            WHEN MONTH(DATE_DAY) IN (3, 4, 5)
                THEN 'Spring'

            WHEN MONTH(DATE_DAY) IN (6, 7, 8)
                THEN 'Summer'

            WHEN MONTH(DATE_DAY) IN (9, 10, 11)
                THEN 'Fall'

        END AS season

    FROM date_spine

)

SELECT

    date_key,
    full_date,
    year,
    quarter,
    month,
    week,
    day_of_week,
    day_name,
    holiday_flag,
    season

FROM date_attributes