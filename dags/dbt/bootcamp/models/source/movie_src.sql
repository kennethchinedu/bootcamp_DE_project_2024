
select
    *
from
{{ source('netflix', 'movie_raw') }}



