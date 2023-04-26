with source as (
    SELECT SUM(tbl.countEach) AS totalRowCount FROM (
        SELECT COUNT(*) as countEach FROM {{ ref('stg_name_basics') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_akas') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_basics') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_crew') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_episode') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_principals') }}
        UNION ALL
        SELECT COUNT(*) as countEach FROM {{ ref('stg_title_ratings') }}
    ) tbl
)

select 
    *,
    current_timestamp() as ingestion_timestamp
from source