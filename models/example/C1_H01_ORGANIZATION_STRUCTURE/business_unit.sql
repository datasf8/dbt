{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    hash(externalcode, startdate) business_unit_id,
    externalcode as business_unit_code,
    startdate as business_unit_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as business_unit_end_date,
    cust_short_code_defaultvalue as business_unit_short_code,
    name as business_unit_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as business_unit_name_fr,
    cust_todivision_externalcode as area_code,
    a.area_id as area_id,
    cust_legalentity as company_code,
    c.company_id as company_id,
    cust_type_bu as business_unit_type_code,
    but.business_unit_type_id as business_unit_type_id,
    cust_hub as hub_code,
    hub_id,
    status as business_unit_status
from {{ ref("stg_fo_department_flatten") }} sfdf
left join
    {{ ref("area") }} a
    on sfdf.cust_todivision_externalcode = a.area_code
    and sfdf.startdate between a.area_start_date and a.area_end_date
left join
    {{ ref("company") }} c
    on sfdf.cust_legalentity = c.company_code
    and sfdf.startdate between c.company_start_date and c.company_end_date
left join
    {{ ref("business_unit_type") }} but
    on sfdf.cust_type_bu = but.business_unit_type_code
    and sfdf.startdate
    between but.business_unit_type_start_date and but.business_unit_type_end_date
left join
    {{ ref("hub") }} h
    on sfdf.cust_hub = h.hub_code
    and sfdf.startdate between h.hub_start_date and h.hub_end_date
where sfdf.dbt_valid_to is null
