with source as (
    select
        tconst as title_id,
        titleType,
        primaryTitle,
        cast(startYear as int) as startYear,
        genres,
        current_timestamp() as insertion_timestamp,
    from {{ ref('stg_title_basics') }}
    where startYear is not null
    and startYear <= 2023
    order by startYear asc
),

unique_source as (
    select *,
            row_number() over(partition by title_id) as row_number
    from source
)

select * 
except (row_number),
from unique_source
where row_number = 1