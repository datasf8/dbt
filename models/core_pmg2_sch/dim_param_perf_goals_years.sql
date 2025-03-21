{{ config(materialized="table", transient=false) }}

with
    param_perf_goals_years as (
        select distinct dpfy_year_dpfy
        from {{ source("landing_tables_PMG", "PARAM_PERF_GOALS_YEARS") }}
    )
select dpfy_year_dpfy
from param_perf_goals_years
