{{ config(materialized='incremental', unique_key='date_key', tags=['incremental']) }}

WITH calendar AS (
  SELECT
    d AS date_key,
    EXTRACT(YEAR FROM d) AS year,
    DATE(EXTRACT(YEAR FROM d), 1, 1) AS year_start,
    DATE(EXTRACT(YEAR FROM d), 12, 31) AS year_end,
    EXTRACT(MONTH FROM d) AS month_number,
    DATE(EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), 1) AS month_start,
    DATE_ADD(DATE(EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), 1), INTERVAL 1 MONTH) - INTERVAL 1 DAY AS month_end,
    DATE_DIFF(
      DATE_ADD(DATE(EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), 1), INTERVAL 1 MONTH),
      DATE(EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), 1),
      DAY
    ) + 1 AS days_in_month,
    CAST(FORMAT_DATE('%Y%m', d) AS INT64) AS year_month_number,
    FORMAT_DATE('%Y-%b', d) AS year_month_name,
    EXTRACT(DAY FROM d) AS day_number,
    FORMAT_DATE('%A', d) AS day_name,
    FORMAT_DATE('%a', d) AS day_name_short,
    EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
    FORMAT_DATE('%B', d) AS month_name,
    FORMAT_DATE('%b', d) AS month_name_short,
    EXTRACT(QUARTER FROM d) AS quarter,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM d) AS STRING)) AS quarter_name,
    CAST(FORMAT_DATE('%Y', d) || CAST(EXTRACT(QUARTER FROM d) AS STRING) AS INT64) AS year_quarter_number,
    CONCAT(FORMAT_DATE('%Y', d), ' Q', CAST(EXTRACT(QUARTER FROM d) AS STRING)) AS year_quarter_name,
    DATE(EXTRACT(YEAR FROM d), ((EXTRACT(QUARTER FROM d) - 1) * 3 + 1), 1) AS quarter_start,
    DATE_ADD(DATE(EXTRACT(YEAR FROM d), (EXTRACT(QUARTER FROM d) * 3), 1), INTERVAL 1 MONTH) - INTERVAL 1 DAY AS quarter_end,
    EXTRACT(WEEK FROM d) AS week_number,
    DATE_SUB(d, INTERVAL EXTRACT(DAYOFWEEK FROM d) - 1 DAY) AS week_start,
    DATE_ADD(d, INTERVAL 7 - EXTRACT(DAYOFWEEK FROM d) DAY) AS week_end
  FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE(2020, 1, 1), DATE_ADD(CURRENT_DATE(), INTERVAL 2 YEAR))
  ) AS d
)

SELECT *
FROM calendar
{% if is_incremental() %}
WHERE date_key > (SELECT MAX(date_key) FROM {{ this }})
{% endif %}
