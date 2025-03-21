select
    year,
    employee_key,
    -- GOAL_CATEGORY_KEY,
    -- GOAL_STATUS_KEY,
    -- GOAL_CASCADED_KEY,
    -- GOAL_TYPE_KEY,
    case when nb_goals > 10 then '10+' else nb_goals::varchar(20) end as nb_goals,
    goal_plan_id
from
    (

        select distinct
            null as year,
            fppg_employee_key_ddep as employee_key,
            fppg_goal_plan_id_fppg as goal_plan_id,
            -- FPPG_CATEGORY_KEY_DPGC                  As GOAL_CATEGORY_KEY,
            -- FPPG_STATUS_DPGS                        As GOAL_STATUS_KEY,
            -- FPPG_PERF_GOALS_CASCADED_KEY_DPGS       As GOAL_CASCADED_KEY,
            -- FPPG_PERF_GOALS_TYPE_KEY_DPGT           As GOAL_TYPE_KEY,
            count(distinct fppg_perf_goal_key_dppg) as nb_goals

        from {{ ref("fact_perf_goals_final") }}
        group by fppg_goal_plan_id_fppg, fppg_employee_key_ddep
        -- FPPG_CATEGORY_KEY_DPGC,             
        -- FPPG_STATUS_DPGS,                   
        -- FPPG_PERF_GOALS_CASCADED_KEY_DPGS,  
        -- FPPG_PERF_GOALS_TYPE_KEY_DPGT   
        union
        select distinct
            null as year,
            employee_key as employee_key,
            dpgp_goal_plan_id_dpgp as goal_plan_id,
            -- '-1'              As GOAL_CATEGORY_KEY,
            -- '-1'              As GOAL_STATUS_KEY,
            -- '-1'              As GOAL_CASCADED_KEY,
            -- '-1'              As GOAL_TYPE_KEY,
            0 as nb_goals
        from {{ ref("dim_employee_profile_vw") }}
        inner join
            {{ ref("dim_perf_goal_plan") }} plan
            on plan.dpgp_year_dpgp >= year(hire_date)
        where
            not exists (
                select 1
                from {{ ref("fact_perf_goals_final") }}

                where
                    fppg_employee_key_ddep = employee_key
                    and plan.dpgp_goal_plan_id_dpgp = fppg_goal_plan_id_fppg
            )
    )
join
    {{ ref("dim_employee_profile_final") }}
    on employee_key = ddep_employee_profile_sk_ddep
