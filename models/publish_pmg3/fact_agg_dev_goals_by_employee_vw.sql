select
    year,
    employee_key,
    -- GOAL_STATUS_KEY,
    case when nb_goals > 10 then '10+' else nb_goals::varchar(20) end as nb_goals,
    case when nb_goals > 10 then '11' else nb_goals end as goals_sort,
    dev_goal_plan_template_id
from
    (
        select distinct
            ddgp.ddgp_year_ddgp as year,
            fdpg_employee_profile_key_ddep as employee_key,
            -- FDPG_DEV_GOALs_STATUS_key_DDGS As GOAL_STATUS_KEY,
            count(distinct fdpg_dev_goal_key_ddpg) as nb_goals,
            dev_goal_plan_template_id
        from {{ ref("fact_dev_goals_final") }}
        inner join
            {{ ref("dim_dev_goals_plan") }} ddgp
            on dev_goal_plan_template_id = ddgp_dev_goal_template_id_ddgp
        group by
            ddgp.ddgp_year_ddgp,
            fdpg_employee_profile_key_ddep,
            dev_goal_plan_template_id
        -- FDPG_DEV_GOALS_STATUS_KEY_DDGS
        union
        select distinct
            ddgp_year_ddgp as year,
            employee_key as employee_key,
            -- '-1'             As GOAL_STATUS_KEY,
            0 as nb_goals,
            null
        from {{ ref("dim_employee_profile_vw") }}
        inner join
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.dim_dev_goals_plan years
            on years.ddgp_year_ddgp >= year(hire_date)
        where
            not exists (
                select 1
                from {{ ref("fact_dev_goals_final") }}
                inner join
                    {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.dim_dev_goals_plan
                    on dev_goal_plan_template_id = ddgp_dev_goal_template_id_ddgp
                where
                    fdpg_employee_profile_key_ddep = employee_key
                    and dim_dev_goals_plan.ddgp_year_ddgp = years.ddgp_year_ddgp
            )
    )
join
    {{ ref("dim_employee_profile_final") }}
    on employee_key = ddep_employee_profile_sk_ddep
