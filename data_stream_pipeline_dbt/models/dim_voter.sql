{{ config(materialized='incremental', unique_key='voter_id', tags=['incremental']) }}

SELECT
  voter_id,
  voter_name,
  date_of_birth,
  gender,
  nationality,
  registration_number,
  address.street   AS address_street,
  address.city     AS address_city,
  address.state    AS address_state,
  address.country  AS address_country,
  address.postcode AS address_postcode,
  email,
  phone_number,
  cell_number,
  picture,
  registered_age,
  voting_time  -- include this if using for incremental logic
FROM `{{ target.project }}.raw_data.raw_vote_events`
{% if is_incremental() %}
WHERE voting_time > (
    SELECT COALESCE(MAX(voting_time), '2020-12-30')
    FROM {{ this }}
)
{% endif %}
