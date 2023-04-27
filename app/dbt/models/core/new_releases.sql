with source as (
    SELECT 
        cast(startYear as int) as startYear,
        count(*) as newShows
    FROM {{ ref('stg_title_episode') }} episode
    LEFT JOIN {{ ref('stg_title_basics') }} basics
    ON basics.tconst = episode.tconst
    LEFT JOIN {{ ref('stg_title_akas') }} akas
    ON episode.parentTconst = akas.titleId
    where startYear is not null
    and episodeNumber = 1
    and seasonNumber = 1
    and region = 'US'
    and startYear between 1993 and 2023
    group by startYear
    order by startYear asc
)

select 
    *,
    current_timestamp() as ingestion_timestamp
from source