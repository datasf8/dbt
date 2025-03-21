{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    externalcode as document_id,
    cust_version as document_version,
    cust_stardate as document_start_date,
    cust_enddate as document_end_date,
    json_extract_path_text(cust_name, 'en_US') as document_name_en,
    json_extract_path_text(cust_desc, 'en_US') as document_description_en,
    cust_status as document_status_code
from {{ ref("stg_cust_pltf_ww_ack_models_flatten") }}
where dbt_valid_to is null
