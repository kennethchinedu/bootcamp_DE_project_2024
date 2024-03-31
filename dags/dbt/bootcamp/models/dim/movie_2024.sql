-- This dbt model seperates the data into seperate years and stores them in the datalake


{{
  config(
    materialized='view'
  )
}}

WITH movie_24 AS (
    SELECT
        *
    FROM {{ ref("movie_stage") }}
)

SELECT 
    distinct *
FROM movie_24
WHERE
     YEAR(release_date) = 2024