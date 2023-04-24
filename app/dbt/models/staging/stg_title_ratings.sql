with source as (
    select * from {{ source('staging', 'title_ratings') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source