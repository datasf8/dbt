{{
    config(
        materialized="table",
        unique_key="1",
        transient=false,
        on_schema_change="sync_all_columns",        
    )
}}

select
    to_date(csrd_egm_start_date::string, 'yyyymmdd') as csrd_egm_start_date,
    to_date(csrd_egm_end_date::string, 'yyyymmdd') as csrd_egm_end_date,
    country_code,
    employee_group_code
from {{ ref("csrd_employee_group_mapping_seed") }}