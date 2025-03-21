{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}


with
    csrd_headcount as (
        select
            hdc.*,
            headcount_csrd.headcount_present_flag as is_csrd_headcount_flag,
            headcount_sta.headcount_present_flag as is_sta_headcount_flag,
            iff(
                map.employee_subgroup_code is null,
                iff(hdc.employee_group_code in ('EG0002', 'EG0003', 'EG0006'), 1, 0),
                iff(
                    map.country_code is not null
                    and map.local_contract_code = 'Fixed term contract',
                    1,
                    0
                )
            ) as fg_fixed_term_contract,
            iff(
                map.employee_subgroup_code is null,
                iff(hdc.employee_group_code in ('EG0001', 'EG0008'), 1, 0),
                iff(
                    map.country_code is not null
                    and map.local_contract_code = 'Permanent Contract',
                    1,
                    0
                )
            ) as fg_permanant_contract

        from {{ ref("csrd_headcount_details") }} hdc
        inner join
            {{ ref("headcount_v1") }} headcount_csrd
            on hdc.user_id = headcount_csrd.user_id
            and hdc.csrd_hd_start_date <= headcount_csrd.job_end_date
            and hdc.csrd_hd_end_date >= headcount_csrd.job_start_date
            and headcount_csrd.headcount_type_code = 'CSR'
        left outer join
            {{ ref("headcount_v1") }} headcount_sta
            on hdc.user_id = headcount_sta.user_id
            and hdc.csrd_hd_start_date <= headcount_sta.job_end_date
            and hdc.csrd_hd_end_date >= headcount_sta.job_start_date
            and headcount_sta.headcount_type_code = 'STA'

        left outer join
            (
                select distinct country_code
                from {{ ref("csrd_local_contract_type_mapping") }}
            ) country
            on hdc.country_code = country.country_code

        left outer join
            {{ ref("csrd_local_contract_type_mapping") }} map
            on hdc.country_code = map.country_code
            and hdc.employee_group_code = map.employee_group_code
            and hdc.employee_subgroup_code = map.employee_subgroup_code

    ),
    date_cte as (

        select date_month_code, date_month_start_date, date_month_end_date
        from {{ ref("date_month_referential") }}
        where date_month_code >= 202401
    ),
    csrd_headcount_details as (

        select ref_date.date_month_code, hdc.*
        from csrd_headcount hdc
        join
            date_cte ref_date
            on ref_date.date_month_end_date
            between hdc.csrd_hd_start_date and hdc.csrd_hd_end_date

    ),
    headcount as (
        select
            date_month_code as csrd_calculation_date,
            csrd_hd_id,
            iff(is_csrd_headcount_flag = 1, '1', null) as meth,  -- METH : Employees
            iff(
                is_csrd_headcount_flag = 1 and legal_gender_code = 'M', '1', null
            ) as methm,  -- Month end - Men
            iff(
                is_csrd_headcount_flag = 1 and legal_gender_code = 'F', '1', null
            ) as methwo,  -- Month end - Women
            iff(
                is_csrd_headcount_flag = 1 and legal_gender_code = 'O', '1', null
            ) as methot,  -- Month end - Women
            iff(
                is_csrd_headcount_flag = 1
                and (legal_gender_code in ('U', 'D') or legal_gender_code is null),
                '1',
                null
            ) as methnd,  -- Month end - Not Declared

            iff(
                is_csrd_headcount_flag = 1
                and fg_permanant_contract = 1
                and legal_gender_code = 'M',
                '1',
                null
            ) as mepcm,  -- Employees  - Permanent - Men
            iff(
                is_csrd_headcount_flag = 1
                and fg_permanant_contract = 1
                and legal_gender_code = 'F',
                '1',
                null
            ) as mepcwo,  -- Employees  - Permanent - Women
            iff(
                is_csrd_headcount_flag = 1
                and fg_permanant_contract = 1
                and legal_gender_code = 'O',
                '1',
                null
            ) as mepcot,  -- Employees  - Permanent - Other
            iff(
                is_csrd_headcount_flag = 1
                and fg_permanant_contract = 1
                and (legal_gender_code in ('U', 'D') or legal_gender_code is null),
                '1',
                null
            ) as mepcnd,  -- Employees  - Permanent - Not declared

            iff(
                is_csrd_headcount_flag = 1
                and fg_fixed_term_contract = 1
                and legal_gender_code = 'M',
                '1',
                null
            ) as meftcm,  -- Employees  - Fixed term - Men
            iff(
                is_csrd_headcount_flag = 1
                and fg_fixed_term_contract = 1
                and legal_gender_code = 'F',
                '1',
                null
            ) as meftcwo,  -- Employees  - Fixed term - Women
            iff(
                is_csrd_headcount_flag = 1
                and fg_fixed_term_contract = 1
                and legal_gender_code = 'O',
                '1',
                null
            ) as meftcot,  -- Employees  - Fixed term - Other
            iff(
                is_csrd_headcount_flag = 1
                and fg_fixed_term_contract = 1
                and (legal_gender_code in ('U', 'D') or legal_gender_code is null),
                '1',
                null
            ) as meftcnd,  -- Employees  - Fixed term - Not declared

            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 1
                and key_position_type_code = 'SKP',
                '1',
                null
            ) as meskp,  -- Strategic Key Position

            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 1
                and key_position_type_code = 'SKP'
                and legal_gender_code = 'M',
                '1',
                null
            ) as meskpm,  -- Month end - strategic key position - Men
            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 1
                and key_position_type_code = 'SKP'
                and legal_gender_code = 'F',
                '1',
                null
            ) as meskpwo,  -- Month end - strategic key position - Women
            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 1
                and key_position_type_code = 'SKP'
                and legal_gender_code = 'O',
                '1',
                null
            ) as meskpot,  -- Month end - strategic key position - Others
            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 1
                and key_position_type_code = 'SKP'
                and (legal_gender_code in ('U', 'D') or legal_gender_code is null),
                '1',
                null
            ) as meskpnd,  -- Month end - strategic key position - Not declared

            iff(
                is_csrd_headcount_flag = 1 and fg_permanant_contract = 1, '1', null
            ) as mepc,  -- Employees  - Permanent contract
            iff(
                is_csrd_headcount_flag = 1 and fg_fixed_term_contract = 1, '1', null
            ) as meftc,  -- Employees  - Fixed term contract

            iff(
                is_csrd_headcount_flag = 1
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                >= 0
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                < 16,
                '1',
                null
            ) as metneu16,  -- Employees  - < 16 years 
            iff(
                is_csrd_headcount_flag = 1
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                >= 16
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                < 18,
                '1',
                null
            ) as "METNEB16&18",  -- Employees  - 16-18 years
            iff(
                is_csrd_headcount_flag = 1
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                < 30,
                '1',
                null
            ) as "METNEU30",  -- Employees  - < 30 years
            iff(
                is_csrd_headcount_flag = 1
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                >= 30
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                < 50,
                '1',
                null
            ) as "METNEb30&49",  -- Employees  - 30-50 years
            iff(
                is_csrd_headcount_flag = 1
                and datediff(day, date_of_birth, last_day(csrd_hd_start_date)) / 365.0
                >= 50,
                '1',
                null
            ) as "METNEA50",  -- Employees  - > 50 years

            iff(
                is_csrd_headcount_flag = 1 and date_of_birth is null, '1', null
            ) as "MDB",  -- Missing Date of Birth

            iff(is_sta_headcount_flag = 1, '1', null) as sta,
            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 0
                and employee_status_code = 'P'
                and employee_group_code not in ('EG0003','EG0006'),
                '1',
                null
            ) as pl,  -- Paid Leave
            iff(
                is_csrd_headcount_flag = 1
                and is_sta_headcount_flag = 0
                and employee_status_code = 'U'
                and employee_group_code not in ('EG0003','EG0006'),
                '1',
                null
            ) as ul,  -- Unpaid Leave

            iff(
                employee_group_code = 'EG0003' and employee_status_code = 'A', '1', null
            ) as totint,  -- Total interns

            iff(
                is_csrd_headcount_flag = 1 and employee_group_code = 'EG0003', '1', null
            ) as csrdi,  -- Interns

            iff(
                is_csrd_headcount_flag = 1 and employee_group_code = 'EG0003', '1', null
            ) as iwcs,  -- Intern working contracts status (country law)

            iff(
                is_csrd_headcount_flag = 0
                and employee_group_code = 'EG0003'
                and employee_status_code = 'A',
                '1',
                null
            ) as incsrds,  -- Intern not in CSRD scope

            iff(
                employee_group_code = 'EG0006' and employee_status_code = 'A', '1', null
            ) as totapp,  -- apprentices
            iff(
                is_csrd_headcount_flag = 1 and employee_group_code = 'EG0006', '1', null
            ) as csrda,  -- Apprentices

            iff(
                is_csrd_headcount_flag = 1 and employee_group_code = 'EG0006', '1', null
            ) as awcs,  -- Apprentices working contracts status (country law)

            iff(
                is_csrd_headcount_flag = 0
                and employee_group_code = 'EG0006'
                and employee_status_code = 'A',
                '1',
                null
            ) as ancsrds,  -- Apprentice not in CSRD scope
            iff(
                is_csrd_headcount_flag = 1 and disability_status_code = 'Y', '1', null
            ) as edis,  -- Employees  - Disability
            iff(
                is_csrd_headcount_flag = 1 and is_sta_headcount_flag = 0 and employee_status_code not in ('U','P') and employee_group_code not in ( 'EG0003','EG0006')
                and employee_status_code = 'A', '1', null
            ) as "OTHERS"              
        from csrd_headcount_details
    ),
    kpiheadcountdata as (
        select *
        from
            headcount unpivot (
                value for csrd_measure_code in (

                    meth,
                    methm,
                    methwo,
                    methot,
                    methnd,
                    mepcm,
                    mepcwo,
                    mepcot,
                    mepcnd,
                    meftcm,
                    meftcwo,
                    meftcot,
                    meftcnd,
                    meskp,
                    meskpm,
                    meskpwo,
                    meskpot,
                    meskpnd,
                    mepc,
                    meftc,
                    metneu16,
                    "METNEB16&18",
                    "METNEU30",
                    "METNEb30&49",
                    metnea50,
                    mdb,
                    sta,
                    pl,
                    ul,
                    csrdi,
                    iwcs,
                    incsrds,
                    csrda,
                    awcs,
                    ancsrds,
                    totapp,
                    totint,
                    edis,
                    "OTHERS" 
                )
            )
        where value is not null  -- and dates_referential_id > 20241000 --group by all
    ),

    pervious_period as (
        select
            lag(hdc.is_csrd_headcount_flag) over (
                partition by user_id order by csrd_hd_start_date
            ) previous_is_csrd_headcount_flag,

            hdc.*
        from csrd_headcount hdc

    ),

    turnover as (
        select
            date_month_code as csrd_calculation_date,
            csrd_measure_id as csrd_measure_id,
            csrd_hd_id as csrd_hd_id,
            date_month_start_date,
            '1' as csrd_value
        from pervious_period hdc
        join
            date_cte ref_date
            on hdc.termination_date
            between ref_date.date_month_start_date and ref_date.date_month_end_date

        join {{ ref("csrd_measures_referential") }} on csrd_measure_code = 'METOM'
        where
            termination_date is not null
            and previous_is_csrd_headcount_flag = 1
            and fg_permanant_contract = 1
    /*     qualify
            row_number() over (
                partition by hdc.user_id order by hdc.csrd_hd_start_date desc
            )
            = 1
            */
    ),
    trunover_cumul as (
        select
            date_month_code as csrd_calculation_date,
            csrd_measures_referential.csrd_measure_id,
            csrd_hd_id as csrd_hd_id,
            '1' as csrd_value
        from turnover
        join
            date_cte
            on turnover.date_month_start_date <= date_cte.date_month_start_date
            and year(turnover.date_month_start_date)
            = year(date_cte.date_month_start_date)

        join
            {{ ref("csrd_measures_referential") }}
            on csrd_measures_referential.csrd_measure_code = 'METO'

    ),
    learning_indicators as (
        select
            date_month_code as csrd_calculation_date,
            csrd_hd_id as csrd_hd_id,
            date_month_start_date,
            sum(total_hours) as tlh,
            sum(total_hours) as tlhm,
            sum(iff(legal_gender_code = 'M', total_hours, null)) as lhm,
            sum(iff(legal_gender_code = 'F', total_hours, null)) as lhw,
            sum(iff(legal_gender_code = 'O', total_hours, null)) as lho,
            sum(
                iff(
                    (legal_gender_code in ('U', 'D') or legal_gender_code is null),
                    total_hours,
                    null
                )
            ) as lhnd
        from csrd_headcount hdc
        inner join
             {{ ref("learning_activity") }} lrn
            on hdc.user_id = lrn.user_id
            and completion_date
            between csrd_hd_start_date and csrd_hd_end_date
        join
            date_cte ref_date
            on completion_date
            between ref_date.date_month_start_date and ref_date.date_month_end_date 
        where is_csrd_headcount_flag = 1  
        group by 1,2,3 
 
    ) ,
    learning_cumulative_indicators as (
        select
            date_month_code as csrd_calculation_date,
           
            csrd_hd_id as csrd_hd_id,
            sum(tlh) as tlh,
            sum(lhm) as lhm,
            sum(lhw) as lhw,
            sum(lho) as lho,
            sum(lhnd) as lhnd
        from learning_indicators
        join
            date_cte
            on learning_indicators.date_month_start_date <= date_cte.date_month_start_date
            and year(learning_indicators.date_month_start_date)
            = year(date_cte.date_month_start_date) 
            group by 1,2 
  
    ) ,
 

    kpilearningdata as (
        select *
        from
            learning_cumulative_indicators
            unpivot (value for csrd_measure_code in (tlh, lhm, lhw, lho, lhnd))
        where value is not null  -- and dates_referential_id > 20241000 --group by all
    ),
    employee_by_measure_cte as (

        select
            a.csrd_hd_id,
            b.csrd_measure_id,
            a.csrd_calculation_date,
            a.value as csrd_value
        from kpiheadcountdata a
        left outer join
            {{ ref("csrd_measures_referential") }} b
            on a.csrd_measure_code = b.csrd_measure_code
        union all

        select
            a.csrd_hd_id,
            b.csrd_measure_id,
            a.csrd_calculation_date,
            a.value as csrd_value
        from kpilearningdata a
        left outer join
            {{ ref("csrd_measures_referential") }} b
            on a.csrd_measure_code = b.csrd_measure_code
        union all
        select csrd_hd_id, b.csrd_measure_id, csrd_calculation_date,tlhm as csrd_value
        from learning_indicators
                join
            {{ ref("csrd_measures_referential") }} b
            on b.csrd_measure_code = 'TLHM'
        where tlhm is not null 
        union all
        select csrd_hd_id, csrd_measure_id, csrd_calculation_date, csrd_value
        from turnover
        union all
        select csrd_hd_id, csrd_measure_id, csrd_calculation_date, csrd_value
        from trunover_cumul

    )

select
    hash(csrd_hd_id, csrd_measure_id, csrd_calculation_date) as csrd_ebm_id,
    csrd_calculation_date,
    csrd_measure_id,
    csrd_hd_id as csrd_hd_id,
    csrd_value
from employee_by_measure_cte
