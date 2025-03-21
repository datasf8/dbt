{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(code, effectivestartdate) as country_id,
    code as country_code,
    effectivestartdate as country_start_date,
    iff(
        effectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by code order by effectivestartdate
        ),
        effectiveenddate
    ) as country_end_date,
    externalname_defaultvalue as country_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as country_name_fr,
    twocharcountrycode as country_iso2_code,
    numericcountrycode as country_iso3_numeric_code,
    cust_dialcode as country_inter_dialing_code,
    currency as currency_code,
    status as country_status,
    {% if target.name == "NP" %} lc.country_code is not null as is_ec_live_flag
    {% elif target.name == "PD" %} lc.country_code is not null as is_ec_live_flag
    {% else %} true as is_ec_live_flag
    {% endif %}
from {{ ref("stg_country_flatten") }} c
left join {{ ref("ec_live_country_seed") }} lc on c.code = lc.country_code
where dbt_valid_to is null
