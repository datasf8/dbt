{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    goal_plan as (
        select id, name, right(name, 4) as year, displayorder
        from {{ ref("stg_goal_plan_template_flatten") }}
        where dbt_valid_to is null

    )

select
    id as dpgp_goal_plan_id_dpgp,
    name as dpgp_goal_plan_name_dpgp,
    year as dpgp_year_dpgp,
    displayorder as dpgp_displayorder_dpgp
from goal_plan
