{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    pos_rel as (
        select
            position_code,
            position_effectivestartdate,
            lead(position_effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by position_code order by position_effectivestartdate
                ) as position_relationship_end_date,
            matrixrelationshiptype,
            relatedposition
        from {{ ref("stg_position_matrix_relationship_flatten") }}
        where dbt_valid_to is null
    )
select
    position_code,
    position_effectivestartdate as position_relationship_start_date,
    position_relationship_end_date,
    matrixrelationshiptype as position_relationship_type_code,
    relatedposition as related_position
from pos_rel
