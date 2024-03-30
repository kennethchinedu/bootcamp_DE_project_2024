-- renamed column
-- Generated unique ID to cover for column without imdb id
-- Casted the year column as date 
-- Use distinct statement to remove duplicate in the data
-- This is an ephemeral materialization and will not show in the data warehouse

{{
  config(
    materialized='ephemeral'
  )
}}

WITH stage_movie AS (
    SELECT
        md5(concat(year::string, title::string)) as unique_id ,
        title as movie_title, 
        rating as movie_rating, 
        year as movie_year, 
        runtime,
        top250,
        top250tv,
        cast(title_date as date) as release_date
    FROM {{ ref("movie_src") }}
)

SELECT 
    distinct *
FROM stage_movie
