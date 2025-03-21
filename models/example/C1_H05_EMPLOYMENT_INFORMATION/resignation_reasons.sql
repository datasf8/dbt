{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    externalcode as resignation_reason_code,
    effectivestartdate as resignation_reason_start_date,
    iff(
        mdfsystemeffectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by effectivestartdate
        ),
        mdfsystemeffectiveenddate
    ) as resignation_reason_end_date,
    externalname_defaultvalue as resignation_reason_name
from {{ ref("stg_cust_subterminationreason_flatten") }}
where dbt_valid_to is null
