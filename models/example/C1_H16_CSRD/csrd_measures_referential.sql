{{
    config(
        materialized="table",
        unique_key="1",
        transient=false,
        on_schema_change="sync_all_columns",
    )
}}
select
    hash(csrd_measure_code, csrd_measure_start_date) as csrd_measure_id,
    to_date(csrd_measure_start_date::string, 'yyyymmdd') as csrd_measure_start_date,
    to_date(csrd_measure_end_date::string, 'yyyymmdd') as csrd_measure_end_date,
    csrd_account_number,
    csrd_measure_code,
    csrd_measure_name,
    csrd_measure_sort,
    csrd_measure_group_code,
    csrd_measure_group_name,
    csrd_measure_group_sort,
    csrd_measure_subgroup_code,
    csrd_measure_subgroup_name,
    csrd_measure_subgroup_sort

from {{ ref("csrd_measures_referential_seed") }}
