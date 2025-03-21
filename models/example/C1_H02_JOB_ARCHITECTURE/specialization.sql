{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, effectivestartdate) as specialization_id,
    externalcode as specialization_code,
    effectivestartdate as specialization_start_date,
    iff(
        mdfsystemeffectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by effectivestartdate
        ),
        mdfsystemeffectiveenddate
    ) as specialization_end_date,
    externalname_defaultvalue as specialization_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as specialization_name_fr,
    pf.professional_field_id,
    cust_tojobfunction_externalcode as professional_field_code,
    mdfsystemstatus as specialization_status
from {{ ref("stg_cust_specialization_flatten") }} sp
left join
    {{ ref("professional_field") }} pf
    on sp.cust_tojobfunction_externalcode = pf.professional_field_code
    and specialization_start_date
    between pf.professional_field_start_date and pf.professional_field_end_date
where sp.dbt_valid_to is null 
