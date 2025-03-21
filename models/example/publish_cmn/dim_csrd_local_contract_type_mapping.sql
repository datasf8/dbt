{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * from {{ ref('csrd_local_contract_type_mapping') }}
