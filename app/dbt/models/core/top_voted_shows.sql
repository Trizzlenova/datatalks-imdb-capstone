with source as (
    SELECT 
        akas.title as title, 
        sum(ratings.numVotes) AS numVotes, 
        round(avg(ratings.averageRating),2) as averageRating 
    FROM {{ ref('stg_title_episode') }} episode
    LEFT JOIN {{ ref('stg_title_ratings') }} ratings
    ON ratings.tconst = episode.tconst
    LEFT JOIN {{ ref('stg_title_akas') }} akas
    ON episode.parentTconst = akas.titleId
    where akas.region = 'US'
    and akas.types = 'imdbDisplay'
    and akas.title <> 'Los Simpson'
    GROUP BY akas.titleId, akas.title
    ORDER BY 2 desc
    LIMIT 30
)

select 
    *,
    current_timestamp() as ingestion_timestamp
from source