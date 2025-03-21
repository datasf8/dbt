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
    fact_goal_activities as (
        select distinct
            a.user_id,
            a.due_date as fppg_due_date_fppg,
            a.last_modified_date as fppg_last_modification_date_fppg,
            a.nb_goals_per_user as nb_goals_per_employee,
            -- B.CREATED_BY_USERNAME,
            b.nb_activities_per_goal_id as number_of_linked_activites,
            b.created_date as fppg_creation_date_fppg,
            b.active_deleted as fppg_fg_deleted_fppg,
            c.ddep_employee_profile_sk_ddep as fppg_employee_key_ddep,
            d.dppg_perf_goal_sk_dppg as fppg_perf_goal_key_dppg,
            e.dpgs_perf_goals_status_sk_dpgs as fppg_status_dpgs,
            f.dpgc_perf_goal_category_sk_dpgc as fppg_category_key_dpgc,
            g.dpgs_perf_goals_cascaded_sk_dpgs as fppg_perf_goals_cascaded_key_dpgs,
            i.dpgt_perf_goals_type_sk_dpgt as fppg_perf_goals_type_key_dpgt,
            to_char((a.last_modified_date), 'MM') as fppg_fg_updtaed_after_march_fppg,
            a.goal_template_id as fppg_goal_plan_id_fppg
        from {{ ref("stg_user_details_int") }} a
        inner join
            {{ ref("dim_perf_goal_plan") }} dpgp
            on to_char(a.goal_template_id) = to_char(dpgp.dpgp_goal_plan_id_dpgp)
        left join {{ ref("stg_goal_report_int") }} b on a.goal_id = b.goal_id
        -- AND  A.USER_ID = B.CREATED_BY_USERNAME
        left outer join
            {{ ref("dim_employee_profile") }} c on a.user_id = c.ddep_employee_id_ddep
        left outer join
            {{ ref("dim_perf_goals") }} d on a.goal_id = d.dppg_perf_goal_id_dppg
        left outer join
            {{ ref("dim_perf_goals_status") }} e
            on a.status = e.dpgs_perf_goals_status_label_dpgs
        left outer join
            {{ ref("dim_perf_goals_category") }} f
            on a.category = f.dpgc_perf_goal_category_label_dpgc
        left outer join
            {{ ref("dim_perf_goals_cascaded") }} g
            on b.goal_aligned_up = g.dpgs_perf_goals_cascaded_label_dpgs
        left outer join
            {{ ref("dim_perf_goals_type") }} i
            on a.goal_characteristic = i.dpgt_perf_goals_type_label_dpgt

    )

select
    user_id,
    fppg_goal_plan_id_fppg,
    fppg_employee_key_ddep,
    fppg_perf_goal_key_dppg,
    nb_goals_per_employee,
    number_of_linked_activites,
    fppg_creation_date_fppg,
    fppg_last_modification_date_fppg,
    fppg_due_date_fppg,
    fppg_category_key_dpgc,
    fppg_status_dpgs,
    fppg_perf_goals_cascaded_key_dpgs,
    fppg_fg_deleted_fppg,
    fppg_perf_goals_type_key_dpgt,
    fppg_fg_updtaed_after_march_fppg
from fact_goal_activities
