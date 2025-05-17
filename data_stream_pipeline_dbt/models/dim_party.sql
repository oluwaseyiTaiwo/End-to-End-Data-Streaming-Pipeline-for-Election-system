-- models/dim_party.sql
{{ config(materialized='incremental', unique_key='party_id', tags=['incremental']) }}

SELECT DISTINCT
  party_affiliation AS party_id,
  party_affiliation AS party_name
FROM {{ ref('dim_candidate') }}
{% if is_incremental() %}
WHERE party_affiliation NOT IN (SELECT party_id FROM {{ this }})
{% endif %}
