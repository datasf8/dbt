{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select hash(assignment_profile_id) as assignment_profile_id_sk, *
from
    {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_BTDP_DS_C1_H08_LEARNING_SCH") }}.dim_param_assignment_profile_referential
