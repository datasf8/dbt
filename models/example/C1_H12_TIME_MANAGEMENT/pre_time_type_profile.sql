{{
    config(
        materialized="table",
        transient=false,
    )
}}
select
    hash(externalcode, mdfsystemeffectivestartdate) as time_type_profile_id,
    externalcode as time_type_profile_code,
    mdfsystemeffectivestartdate as time_type_profile_start_date,
    mdfsystemeffectiveenddate as time_type_profile_end_date,
    externalname_defaultvalue as time_type_profile_name_en,
    externalname_fr_fr as time_type_profile_name_fr,
    country as country_code,
    mdfsystemstatus as time_type_profile_status
from {{ ref("stg_timetype_profile_flatten") }} sttp
where dbt_valid_to is null
