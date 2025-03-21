{{ config(materialized="table", transient=false) }}

select q1.*

from {{ source("QCK_SOURCE_TABLES", "LOG_DATA_QUALITY_BY_RULE") }} q1
join
    {{ source("QCK_SOURCE_TABLES", "LOG_DATAQUALITY_RUNS") }} q2
    on q1.run_id = q2.run_id and q2.status = 'Success'
qualify
    dense_rank() over (partition by q2.start_date::date order by q2.start_date desc) = 1
