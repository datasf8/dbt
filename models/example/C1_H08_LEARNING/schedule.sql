{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    schd_id as schedule_code,
    dbt_valid_from as schedule_data_start_date,
    nvl(dbt_valid_to, '9999-12-31') as schedule_data_end_date,
    description as schedule_description,
    dmn_id as security_domain_code,
    act_cpnt_id as item_code,
    cpnt_typ_id as item_type_code,
    notactive as is_inactive_flag,
    a.contact as contact,
    email_addr as email_address,
    is_sched_virtual as is_schedule_virtual_flag,
    closed_date as closed_date,
    facility_id as facility_id,
    cancelled as is_cancelled_flag,
    cancel_dte as cancelled_date,
    enrl_cut_dte as enrollment_cutoff_date,
    lst_upd_usr as last_updated_by,
    hash(schd_id,dbt_valid_from) as schedule_id,
    b.security_domain_id,
    d.item_id
from {{ ref("stg_pa_sched") }} a
left outer join 
(
select * from {{ ref('security_domain_v1') }}
qualify
            row_number() over (
                partition by security_domain_code
                order by security_domain_start_date desc
            )
            = 1
) b
on a.dmn_id =b.SECURITY_DOMAIN_CODE 
left outer join
    (
        select *
        from {{ ref('item_type_v1') }}
        qualify
            row_number() over (
                partition by item_type_code order by item_type_start_date desc
            )
            = 1
    ) c
    on a.cpnt_typ_id = c.item_type_code
    left outer join
    (
    select * from {{ ref('item_v2') }}
    qualify
            row_number() over (
                partition by item_code order by item_start_date desc
            )
            = 1
    
    ) d
   on a.ACT_CPNT_ID = d.item_code 
and a.CPNT_TYP_ID=c.item_type_code
and a.REV_DTE=d.item_revision_date
