{{ config(materialized="table", transient=false) }}

select tab1.*
from {{ source("QCK_SOURCE_TABLES", "FACT_DATAQUALITY_RESULTS_HISTO") }} tab1
join
    {{ ref("LOG_DATAQUALITY_RUNS") }} tab2 on tab1.run_id = tab2.run_id and last_run = 1
