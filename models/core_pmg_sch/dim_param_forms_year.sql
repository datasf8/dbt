{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="append",
    )
}}

with
    forms_year as (
        select distinct formtemplateid, year(formreviewstartdate) as year
        from {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formheader_flatten
        qualify
            row_number() over (
                partition by formtemplateid order by formreviewstartdate asc
            )
            = 1
        minus
        select *
        from {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.dim_param_forms_year

    )

select formtemplateid as dpfy_form_template_id_dpfy, year as dpfy_year_dpfy
from forms_year
