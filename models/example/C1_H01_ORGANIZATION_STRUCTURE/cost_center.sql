{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as cost_center_id,
    externalcode as cost_center_code,
    startdate as cost_center_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as cost_center_end_date,
    name as cost_center_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as cost_center_name_fr,
    buv.business_unit_id as business_unit_id,
    cust_todepartment_externalcode as business_unit_code,
    ccgv.cost_center_group_id,
    cust_groupcostcenter as cost_center_group_code,
    status as cost_center_status
from {{ ref("stg_fo_cost_center_flatten") }} as sfccf
left join
    {{ ref("cost_center_group") }} as ccgv
    on sfccf.cust_groupcostcenter = ccgv.cost_center_group_code
    and sfccf.startdate
    between ccgv.cost_center_group_start_date and ccgv.cost_center_group_end_date
left join
    {{ ref("business_unit") }} as buv
    on sfccf.cust_todepartment_externalcode = buv.business_unit_code
    and sfccf.startdate
    between buv.business_unit_start_date and buv.business_unit_end_date
where sfccf.dbt_valid_to is null
