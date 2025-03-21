{{ config(materialized="table", transient=true) }}

with
    employment_details_cte as (
        select *
        from {{ ref("employment_details_v1") }}
        qualify
            row_number() over (
                partition by user_id order by employment_details_start_date desc
            )
            = 1
    ),
    disability_status_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    base.user_id,
                    termination_date,
                    --headcount_present_flag,
                    employee_detais.personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    base.key_position_type_id,
                    base.local_contract_type_id,
                    base.employee_status_id,
                    base.legal_gender_id,
                    disability_status.disability_status_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            dt_begin >= coalesce(
                                personal_info.personal_info_start_date,
                                '1900-01-01'
                            )
                        then dt_begin
                        else
                            coalesce(
                                personal_info.personal_info_start_date,
                                '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            dt_end <= coalesce(
                                personal_info.personal_info_end_date,
                                '9999-12-31'
                            )
                        then dt_end
                        else
                            coalesce(
                                personal_info.personal_info_end_date,
                                '9999-12-31'
                            )
                    end as dt_end

                from
                    {{ ref('_tmp_csrd_legal_gender') }} base

                left outer join
                    employment_details_cte employee_detais
                    on base.user_id = employee_detais.user_id

                -- and base.dt_begin <= employee_detais.employment_details_end_date
                -- and base.dt_end >= employee_detais.employment_details_start_date
                left outer join
                    {{ ref("personal_information_v1") }} personal_info
 
                    on employee_detais.personal_id = personal_info.personal_id
                    and base.dt_begin
                    <= personal_info.personal_info_end_date
                    and base.dt_end
                    >= personal_info.personal_info_start_date
                -- and personal_info.personal_info_start_date <=
                -- employee_detais.employment_details_end_date
                -- and personal_info.personal_info_end_date >=
                -- employee_detais.employment_details_start_date
                left outer join
                    {{ ref("disability_status_v1") }} disability_status
                    on personal_info.disability_status_id
                    = disability_status.disability_status_id
                    and personal_info.personal_info_start_date
                    <= disability_status.disability_status_end_date
                    and personal_info.personal_info_end_date
                    >= disability_status.disability_status_start_date

            )
    ),
    disability_status_intervals as (

        select

            user_id,
            termination_date,
            --headcount_present_flag,
            personal_id,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            key_position_type_id,
            local_contract_type_id,
            employee_status_id,
            legal_gender_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from
            (

                -- borne inférieure
                select

                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from disability_status_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    begda,
                    endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    --beg_inter.headcount_present_flag,
                    beg_inter.personal_id,
                    beg_inter.cost_center_id,
                    beg_inter.business_unit_id,
                    beg_inter.company_id,
                    beg_inter.country_id,
                    beg_inter.geographic_zone_id,
                    beg_inter.employee_group_id,
                    beg_inter.key_position_type_id,
                    beg_inter.local_contract_type_id,
                    beg_inter.employee_status_id,
                    beg_inter.legal_gender_id,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from disability_status_cte beg_inter
                inner join
                    disability_status_cte end_inter
                    on (
                        beg_inter.user_id = end_inter.user_id
                        and beg_inter.begda = end_inter.begda
                        and beg_inter.rk_1 = end_inter.rk
                    )
                union
                -- borne supérieure
                select
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from disability_status_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    )
select distinct

    user_id,
    termination_date,
    --headcount_present_flag,
    personal_id,
    cost_center_id,
    business_unit_id,
    company_id,
    country_id,
    geographic_zone_id,
    employee_group_id,
    key_position_type_id,
    local_contract_type_id,
    employee_status_id,
    legal_gender_id,
    disability_status_id,
    begda,
    endda,
    dt_begin,
    dt_end

from disability_status_cte

union
select
    user_id,
    termination_date,
    --headcount_present_flag,
    personal_id,
    cost_center_id,
    business_unit_id,
    company_id,
    country_id,
    geographic_zone_id,
    employee_group_id,
    key_position_type_id,
    local_contract_type_id,
    employee_status_id,
    legal_gender_id,
    null as disability_status_id,
    begda,
    endda,
    dt_begin,
    dt_end

from disability_status_intervals