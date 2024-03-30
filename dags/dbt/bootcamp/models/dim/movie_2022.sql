{{
  config(
    materialized='view'
  )
}}

WITH movie_22 AS (
    SELECT
        *
    FROM {{ ref("movie_stage") }}
)

SELECT 
    distinct *
FROM movie_22
WHERE
     YEAR(release_date) = 2022