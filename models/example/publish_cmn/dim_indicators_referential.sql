{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    group_data as (
        select *
        from
            (
                values
                    ('Grp0', 'General Information', '#7A87C6'),
                    ('Grp1', 'Gender Diversity', '#474DAE'),
                    ('Grp2', 'Turnover', '#995EF8'),
                    ('Grp3', 'Key Positions', '#40C2C0'),
                    ('Grp4', 'Expatriates', '#BE8F15')
            ) dummy_table(grp_id, indicators_group_name, indicators_group_colour)
    ),
    indicators_data as (
        select *
        from
            (
                values
                    ('HDC_HDC', 1, 'Grp0', 'Number', '#93A3D9'),
                    ('HDC_KP', 2, 'Grp0', 'Number', '#93A3D9'),
                    ('HDC_PMH', 3, 'Grp1', 'Percentage', '#7D89DC'),
                    ('HDC_PFLKP', 4, 'Grp1', 'Percentage', '#7D89DC'),
                    ('TRN_GT', 5, 'Grp2', 'Percentage', '#BF92FC'),
                    ('TRN_EDT', 6, 'Grp2', 'Percentage', '#BF92FC'),
                    ('TRN_KPT', 7, 'Grp2', 'Percentage', '#BF92FC'),
                    ('HDC_KPLKP', 8, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_MLKP', 9, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_MSGKP', 10, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_NLKP', 11, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_RLKP', 12, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_ILKP', 13, 'Grp3', 'Percentage', '#8ADCD9'),
                    ('HDC_IM', 14, 'Grp4', 'Number', '#D8AE5F'),
                    ('HDC_IM5Y', 15, 'Grp4', 'Number', '#D8AE5F')
            ) dummy_table(
                indicators_code,
                indicators_sort_order,
                grp_id,
                display_type,
                indicators_kpi_colour
            )
    ),
    denominator_info as (
        select *
        from
            (
                values
                    ('HDC_PMH', 'HIR_GLH'),
                    ('HDC_PFLKP', 'HDC_LKP'),
                    ('TRN_GT', 'HDC_AHDC'),
                    ('TRN_EDT', 'HDC_AHDC'),
                    ('TRN_KPT', 'HDC_KP'),
                    ('HDC_KPLKP', 'HDC_LKP'),
                    ('HDC_MLKP', 'HDC_LKP'),
                    ('HDC_MSGKP', 'HDC_SGKP'),
                    ('HDC_NLKP', 'HDC_LKP'),
                    ('HDC_RLKP', 'HDC_LKP'),
                    ('HDC_ILKP', 'HDC_LKP')
            ) dummy_table(indicators_code, denominator_code)
    )
select distinct
    hash(indicators_code) as indicators_referential_sk,
    id.indicators_code,
    nvl(eir.employee_indicators_name, pir.position_indicators_name) as indicators_name,
    id.* exclude(indicators_code, grp_id),
    gd.* exclude(grp_id),
    di.* exclude(indicators_code),
    nvl(
        eird.employee_indicators_name, pird.position_indicators_name
    ) as denominator_name,
from indicators_data id
join group_data gd using (grp_id)
left join
    {{ ref("employee_indicators_referential") }} eir
    on indicators_code = employee_indicators_code
left join
    {{ ref("position_indicators_referential") }} pir
    on indicators_code = position_indicators_code
left join denominator_info di using (indicators_code)
left join
    {{ ref("employee_indicators_referential") }} eird
    on denominator_code = eird.employee_indicators_code
left join
    {{ ref("position_indicators_referential") }} pird
    on denominator_code = pird.position_indicators_code
