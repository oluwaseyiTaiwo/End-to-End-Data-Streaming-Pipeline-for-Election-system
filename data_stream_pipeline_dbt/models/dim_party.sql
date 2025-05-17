{{ config(materialized='table') }}

SELECT DISTINCT
  party_affiliation AS party_id,
  party_affiliation AS party_name
FROM {{ ref('dim_candidate') }}
