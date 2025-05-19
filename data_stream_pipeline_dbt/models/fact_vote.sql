{{ config(materialized='incremental', unique_key='voter_id', tags=['incremental']) }}

SELECT
  voter_id,
  age,
  address.postcode AS location_key,
  candidate_id,
  party_affiliation,
  TIMESTAMP(voting_time) AS voting_time_utc,
  1 AS vote
FROM `{{ target.project }}.raw_data.raw_vote_events`
{% if is_incremental() %}
WHERE voting_time > (
    SELECT COALESCE(MAX(voting_time_utc), '2020-12-30')
    FROM {{ this }}
)
{% endif %}
