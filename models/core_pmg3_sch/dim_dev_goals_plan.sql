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
    dev_goal_plan as (
        select id, name, duedate, try_to_number(right(name, 4)) as year, displayorder
        from {{ ref("stg_dev_goal_plan_template_flatten") }}
        where dbt_valid_to is null and year is not null

    )

select
    id as ddgp_dev_goal_template_id_ddgp,
    name as ddgp_dev_goal_plan_name_ddgp,
    year as ddgp_year_ddgp,
    displayorder as ddgp_display_order_ddgp,
    duedate as ddgp_due_date_ddgp
from dev_goal_plan
