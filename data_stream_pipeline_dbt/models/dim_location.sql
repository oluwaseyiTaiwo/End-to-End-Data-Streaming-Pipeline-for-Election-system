{{ config(materialized='incremental', unique_key='location_key', tags=['incremental']) }}

SELECT DISTINCT
  address.postcode AS location_key,
  address.country,
  address.state,
  address.city
FROM `{{ target.project }}.raw_data.raw_vote_events`
WHERE address.postcode IS NOT NULL
  AND address.country IS NOT NULL
  AND address.state IS NOT NULL
  AND address.city IS NOT NULL
  AND address.street IS NOT NULL
  AND address.postcode != ''
  AND address.country != ''
  AND address.state != ''
  AND address.city != ''
  AND address.street != ''
{% if is_incremental() %}
  AND address.postcode NOT IN (SELECT location_key FROM {{ this }})
{% endif %}
