{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    cust_lpg as (
        select
            hash(externalcode, effectivestartdate) as local_pay_grade_id,
            externalcode as local_pay_grade_code,
            effectivestartdate as local_pay_grade_start_date,
            iff(
                mdfsystemeffectiveenddate in ('9999-12-31'),
                lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by effectivestartdate
                ),
                mdfsystemeffectiveenddate
            ) as local_pay_grade_end_date,
            externalname_defaultvalue as local_pay_grade_name_en,
            nvl(externalname_fr_fr, 'Not Translated FR') as local_pay_grade_name_fr,
            cust_globalgrade as global_grade_code,
            cust_country as country_code,
            mdfsystemstatus as local_pay_grade_status
        from {{ ref("stg_cust_local_paygrade_flatten") }}
        where dbt_valid_to is null
        union
        select
            global_grade_id as local_pay_grade_id,
            global_grade_code as local_pay_grade_code,
            global_grade_start_date as local_pay_grade_start_date,
            global_grade_end_date as local_pay_grade_end_date,
            global_grade_name as local_pay_grade_name_en,
            null as local_pay_grade_name_fr,
            global_grade_code,
            null as country_code,
            global_grade_status as local_pay_grade_status
        from {{ ref("global_grade") }}
    )

     select cust_lpg.*, global_grade_id, country_id
        from cust_lpg
        left outer join {{ ref("global_grade_v1") }} jobrole on jobrole.global_grade_code = cust_lpg.global_grade_code
		and cust_lpg.local_pay_grade_start_date <= jobrole.global_grade_end_date
         and cust_lpg.local_pay_grade_end_date >= jobrole.global_grade_start_date
        left outer join {{ ref("country_v1") }} ctry on ctry.country_code = cust_lpg.country_code
and cust_lpg.local_pay_grade_start_date <= ctry.country_end_date
         and cust_lpg.local_pay_grade_end_date >= ctry.country_start_date
				        qualify
            row_number() over (
                partition by local_pay_grade_id 
				order by ctry.country_start_date desc,jobrole.global_grade_start_date desc
            )
            = 1
