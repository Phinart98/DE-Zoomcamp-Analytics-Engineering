{{ config(
    materialized='table'
) }}

WITH quarterly_revenue AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(CAST(EXTRACT(YEAR FROM pickup_datetime) AS STRING), '/Q', CAST(EXTRACT(QUARTER FROM pickup_datetime) AS STRING)) AS year_quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2, 3, 4
),

quarterly_revenue_with_prev_year AS (
    SELECT
        current_year.service_type,
        current_year.year,
        current_year.quarter,
        current_year.year_quarter,
        current_year.quarterly_revenue AS current_revenue,
        prev_year.quarterly_revenue AS prev_year_revenue
    FROM quarterly_revenue AS current_year
    LEFT JOIN quarterly_revenue AS prev_year
        ON current_year.service_type = prev_year.service_type
        AND current_year.quarter = prev_year.quarter
        AND current_year.year = prev_year.year + 1
)

SELECT
    service_type,
    year,
    quarter,
    year_quarter,
    current_revenue,
    prev_year_revenue,
    CASE 
        WHEN prev_year_revenue IS NULL OR prev_year_revenue = 0 THEN NULL
        ELSE (current_revenue - prev_year_revenue) / prev_year_revenue * 100 
    END AS yoy_growth_percentage
FROM quarterly_revenue_with_prev_year
ORDER BY service_type, year, quarter