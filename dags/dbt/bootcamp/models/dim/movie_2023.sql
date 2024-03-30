{{
  config(
    materialized='view'
  )
}}

WITH movie_23 AS (
    SELECT
        *
    FROM {{ ref("movie_stage") }}
)

SELECT 
    distinct *
FROM movie_23
WHERE
     YEAR(release_date) = 2023