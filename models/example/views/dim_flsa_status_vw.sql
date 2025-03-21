{{ config(schema="cmn_pub_sch") }}

select
    flsa_status_sk,
    flsa_status_code,
    flsa_status_start_date,
    flsa_status_end_date,
    iff(
        rced.employee_upn is null,
        null,
        case
            when flsa_status_name_en = 'Nonexempt'
            then 'Non Exempt'
            else flsa_status_name_en
        end
    ) as flsa_status_name_en,
    flsa_status_name_fr,
    flsa_status_status
from {{ ref("dim_flsa_status") }}
left join
    {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_param_security_replication dpsr
    on current_user = copy_to_user
left join
    {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_cls_employee_dashboard rced
    on nvl(copy_from_user, current_user) = rced.employee_upn
    and field_name = 'FLSA_STATUS'
