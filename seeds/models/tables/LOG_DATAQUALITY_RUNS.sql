{{ config(materialized="table", transient=false) }}

select
    case
        when row_number() over (order by end_date desc) = 1 then 1 else 0
    end as last_run,
    *
from {{ source("QCK_SOURCE_TABLES", "LOG_DATAQUALITY_RUNS") }}
where status = 'Success'
