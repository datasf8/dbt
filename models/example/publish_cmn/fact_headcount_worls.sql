{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    {{
        dbt_utils.star(
            ref("fact_headcount_v1"),
            except=[
                "employee_sk",
                "user_id",
                "personal_id",
                "user_id_manager",
                "user_id_hr",
                "headcount_line_id",
            ],
        )
    }},
    legal_gender_name_label,
    all_players_status_name_en
from {{ ref("fact_headcount_v1") }}
left join {{ ref("dim_employee_v1") }} using (employee_sk)
