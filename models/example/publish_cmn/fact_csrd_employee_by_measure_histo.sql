{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        cluster_by=["CSRD_EBMH_CALCULATION_DATE", "CSRD_MEASURE_ID"],
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;",
    )
}}

select *, 'CSR' as headcount_type_code
from {{ ref("csrd_employee_by_measure_histo") }}

union all
select *, 'STA' as headcount_type_code
from {{ ref("csrd_sta_employee_by_measure") }}
