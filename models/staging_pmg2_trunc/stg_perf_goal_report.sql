{{ config(materialized='table',unique_key="Goal_ID||'-'||ACTIVITY_ID",transient=false) }}

select
 *
FROM {{ source('landing_tables_PMG', 'PERF_GOAL_REPORT') }}
where Goal_ID is not null
