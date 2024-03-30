{{
  config(
    materialized='view'
  )
}}

WITH
 a AS (
    SELECT 
        unique_id,
        movie_title,
        movie_rating , 
        movie_year, 
        runtime, 
        top250, 
        top250tv, 
        release_date
    FROM {{ ref('movie_2022')}}
 ), 
 b AS (
    SELECT 
        unique_id,
        movie_title,
        movie_rating, 
        movie_year, 
        runtime, 
        top250, 
        top250tv, 
        release_date
    FROM {{ ref('movie_2023')}}
 ),
 c AS (
    SELECT 
        unique_id,
        movie_title, 
        movie_rating, 
        movie_year, 
        runtime, 
        top250, 
        top250tv, 
        release_date
    FROM {{ ref('movie_2024')}}
 )

SELECT 
    unique_id,
    movie_title,
    movie_rating,
    movie_year,
    runtime,
    top250,
    top250tv,
    release_date

FROM 
    a 

UNION ALL

SELECT 
    unique_id,
    movie_title,
    movie_rating,
    movie_year,
    runtime,
    top250,
    top250tv,
    release_date

FROM 
    b 

UNION ALL

SELECT 
    unique_id,
    movie_title,
    movie_rating,
    movie_year,
    runtime,
    top250,
    top250tv,
    release_date

FROM 
    c
