with source as (
    select * from {{ source('staging', 'title_basics') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source