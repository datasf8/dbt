{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','LEARNING_ACTIVITY');"
    )
}}
with
    cte_item as (

        select
            item_code, item_id, item_data_start_date, item_revision_date, item_type_id
        from {{ ref("item") }}
        qualify
            row_number() over (
                partition by item_code, item_type_code
                order by item_data_start_date desc, item_revision_date desc
            )
            = 1

    ),
    cte_schedule as (

        select schedule_code, schedule_id, schedule_data_start_date
        from {{ ref("schedule") }}
        qualify
            row_number() over (
                partition by schedule_code order by schedule_data_start_date desc
            )
            = 1
    )

select
    hash(
        stud_id, i2.item_id, s2.schedule_id, compl_dte, cmpl_stat_id, total_hrs
    ) as learning_activity_id,
    -- i.item_id,
    i2.item_id as item_id,
    c.completionstatus_id,
    stud_id as user_id,
    cpnt_id as item_code,
    schd_id as schedule_code,
    s2.schedule_id,
    compl_dte as completion_date,
    cmpl_stat_id as completion_status_code,
    nvl(total_hrs,0) as total_hours,
    lst_upd_tstmp as last_updated,
    lst_upd_usr as last_updated_by
from {{ ref("stg_pa_cpnt_evthst") }} spce
-- left outer join {{ ref("item") }} i on i.item_code = cpnt_id
left outer join {{ ref("item_type") }} itv on itv.item_type_code = spce.cpnt_typ_id
left outer join
    cte_item i2 on spce.cpnt_id = i2.item_code and itv.item_type_id = i2.item_type_id
left outer join
    {{ ref("completion_status") }} c on c.completion_status_code = cmpl_stat_id

left outer join cte_schedule s2 on spce.schd_id = s2.schedule_code

where dbt_valid_to is null
qualify
    row_number() over (
        partition by stud_id, cpnt_id, schd_id, compl_dte, cmpl_stat_id, total_hrs
        order by compl_dte desc, lst_upd_tstmp desc, c.completion_status_end_date desc
    )
    = 1
