{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    requirement_type_id,
    spcs.rtyp_id as requirement_type_code,
    spcs.dbt_valid_from as requirement_type_start_date,
    nvl(spcs.dbt_valid_to, '9999-12-31') as requirement_type_end_date,
    spall.label_value as requirement_type_description,
    spcs.required as requirement_type_mandatory_flag
from
    (
        select
            hash(rtyp_id, dbt_valid_from) as requirement_type_id,
            rtyp_id,
            dbt_valid_from,
            dbt_valid_to,
            rtyp_desc,
            required
        from {{ ref("stg_pa_rqmt_type") }}
        where dbt_valid_to is null
    ) spcs
left outer join
    (
        select label_id, label_value
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where locale_id = 'English' and dbt_valid_to is null
    ) spall
    on rtyp_desc = label_id
