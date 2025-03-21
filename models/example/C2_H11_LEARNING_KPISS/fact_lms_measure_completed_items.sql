{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    lng_avty as (
        select
            user_id,
            completion_date,
            extract(year from completion_date) year,
            extract(month from completion_date) month,
            item_code
        from {{ ref("learning_activity_v1") }}
        qualify
            row_number() over (
                partition by user_id, item_code order by completion_date desc
            )
            = 1

    ),

    empl_prfl as (select * from {{ ref("employee_profile_directory_v1") }}),
    item as (
        select *
        from {{ ref("item_v1") }}
        where
            item_title = 'GOING SUSTAINABLE TOGETHER'
            and item_data_start_date <= current_date
        qualify
            row_number() over (
                partition by item_code order by item_data_start_date desc
            )
            = 1
    ),
    jn_item as (
        select it.item_code, user_id, completion_date, year, month
        from item it
        inner join lng_avty la on it.item_code = la.item_code
    ),
    jn_item_empl as (
        select ji.user_id, item_code, year, month, business_unit_code
        from jn_item ji
        left outer join empl_prfl ep on ji.user_id = ep.user_id
    )

select year, month, item_code, business_unit_code, count(user_id) as users_number
from jn_item_empl
group by 1, 2, 3, 4
