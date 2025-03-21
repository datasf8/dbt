{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    position_in_range as (

        select *
        from
            (
                select
                    position_in_range_code,
                    case
                        when position_in_range_label = 'Low'
                        then 'Entry'
                        else position_in_range_label
                    end as position_in_range_label_name,
                    dbt_valid_from

                from {{ ref("stg_position_in_range") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    position_in_range_code as min_position_in_range_code,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_position_in_range") }}
                group by position_in_range_code
            ) b
            on a.position_in_range_code = b.min_position_in_range_code
    ),
    surrogate_key as (
        select p.*, hash(position_in_range_code) as sk_position_in_range_code
        from position_in_range p
    )
select
    sk_position_in_range_code as dpir_pk_position_in_range_dpir,
    position_in_range_code as dpir_id_position_in_range_dpir,
    position_in_range_label_name as dpir_lb_position_in_range_dpir,
    creation_date as dpir_creation_date_dpir,
    dbt_valid_from as dpir_modification_date_dpir
from surrogate_key
union
select '-201', null, 'Masked', null, null
union
select '-1', null, null, null, null
