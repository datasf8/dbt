{{
    config(
        materialized="incremental",
        unique_key="RUN_ID",
        transient=false,
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
    )
}}

select tab1.*
from {{ source("QCK_SOURCE_TABLES", "FACT_DATAQUALITY_RESULTS_HISTO") }} tab1
join {{ ref('LOG_DATAQUALITY_RUNS') }} tab2 on tab1.RUN_ID= tab2.RUN_ID AND LAST_RUN = 1
