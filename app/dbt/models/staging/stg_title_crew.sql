with source as (

    select * from {{ source('staging', 'title_crew') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source