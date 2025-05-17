{{ config(materialized='incremental', unique_key='candidate_id', tags=['incremental']) }}

SELECT DISTINCT
  candidate_id,
  candidate_name,
  party_affiliation,
  biography,
  campaign_platform,
  photo_url
FROM {{ source('raw_data', 'raw_vote_events') }}

{% if is_incremental() %}
WHERE candidate_id NOT IN (SELECT candidate_id FROM {{ this }})
{% endif %}