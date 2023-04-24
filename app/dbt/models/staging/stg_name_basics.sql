with source as (
    select * from {{ source('staging', 'name_basics') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source