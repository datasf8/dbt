{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        cluster_by=["employee_sk"],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call masking_policy_apply_sp('{{ database }}','{{ schema }}','DIM_EMPLOYEE_V1');"
    )
}}
with
    legal_gender_v1 as (
        select *
        from {{ ref("legal_gender_v1") }}
        where legal_gender_start_date <= current_date()
        qualify
            row_number() over (
                partition by legal_gender_code order by legal_gender_start_date desc
            )
            = 1
    ),
    all_players_status_v1 as (
        select *
        from {{ ref("all_players_status_v1") }}
        where all_players_status_start_date <= current_date()
        qualify
            row_number() over (
                partition by all_players_status_code
                order by all_players_status_start_date desc
            )
            = 1
    ),
    country_v1 as (
        select *
        from {{ ref("country_v1") }}
        where country_start_date <= current_date()
        qualify
            row_number() over (
                partition by country_code order by country_start_date desc
            )
            = 1
    ),
    ethnicity_v1 as (
        select *
        from {{ ref("ethnicity_v1") }}
        where ethnicity_start_date <= current_date()
        qualify
            row_number() over (
                partition by ethnicity_code order by ethnicity_start_date desc
            )
            = 1
    ),
    personal_information_local_usa_v1 as (
        select *
        from {{ ref("personal_information_local_usa_v1") }}
        where personal_info_usa_start_date <= current_date()
        qualify
            row_number() over (
                partition by personal_id order by personal_info_usa_start_date desc
            )
            = 1
    ),
    race_v1 as (
        select *
        from {{ ref("race_v1") }}
        where race_start_date <= current_date()
        qualify
            row_number() over (partition by race_code order by race_start_date desc) = 1
    ),
    hr as (
        select distinct user_id
        from {{ ref("job_information_v1") }} ji
        join
            {{ ref("position_relationship_v1") }} pl
            on ji.position_code = pl.related_position
    ),
    ed as (
        select
            employment_details_start_date as edvalidfrom,
            nvl(employment_details_end_date, '9999-12-31') as edvalidto,
            *
        from {{ ref("employment_details_v1") }}  -- where personal_id in ('70045492', '70353998', '70349682', '70485541')
    ),
    pi as (
        select
            personal_info_start_date as pivalidfrom,
            personal_info_end_date as pivalidto,
            *
        from {{ ref("personal_information_v1") }}  -- where personal_id in ('70045492', '70353998', '70349682', '70485541')
    ),
    jn as (
        select
            ed.personal_id,
            ed.user_id,
            case
                when edvalidfrom >= coalesce(pivalidfrom, '1900-01-01')
                then edvalidfrom
                else coalesce(pivalidfrom, '1900-01-01')
            end as validfrom,
            case
                when edvalidto <= coalesce(pivalidto, '9999-12-31')
                then edvalidto
                else coalesce(pivalidto, '9999-12-31')
            end as validto,
            edvalidfrom,
            edvalidto,
            pivalidfrom,
            pivalidto
        from ed
        left join
            pi
            on ed.personal_id = pi.personal_id
            and edvalidfrom <= pivalidto
            and edvalidto >= pivalidfrom
    ),  -- select * from ds0 order by 1,2,3;
    rk as (
        select
            *,
            rank() over (order by personal_id, user_id, validfrom) rk,
            rank() over (order by personal_id, user_id, validfrom) + 1 rk_1
        from jn
    ),
    intervals as (
        select
            personal_id,
            user_id,
            edvalidfrom validfrom,
            min(validfrom - 1) validto,
            edvalidfrom,
            edvalidto
        from jn
        group by 1, 2, 5, 6
        union
        select
            bi.personal_id,
            bi.user_id,
            iff(bi.validto != '9999-12-31', bi.validto + 1, bi.validto) validfrom,
            (ei.validfrom - 1) validto,
            bi.edvalidfrom,
            bi.edvalidto
        from rk bi
        join
            rk ei
            on bi.personal_id = ei.personal_id
            and bi.user_id = ei.user_id
            and bi.edvalidfrom = ei.edvalidfrom
            and bi.rk_1 = ei.rk
        union
        select
            personal_id,
            user_id,
            iff(max(validto) != '9999-12-31', max(validto) + 1, max(validto)) validfrom,
            edvalidto validto,
            edvalidfrom,
            edvalidto
        from jn
        group by 1, 2, 5, 6
    ),
    final as (
        select * exclude (pivalidto)
        from jn
        union
        select *, null
        from intervals
        where validfrom <= validto and validfrom != '9999-12-31'
    )
select
    hash(personal_id, user_id, validfrom) as employee_sk,
    personal_id,
    user_id,
    validfrom as employee_start_date,
    validto as employee_end_date,
    employment_details_start_date as employment_start_date,
    employment_details_end_date as employment_end_date,
    personal_info_start_date,
    personal_info_end_date,
    legal_first_name || ' ' || upper(legal_last_name) as employee_name,
    legal_gender_code,
    legal_gender_name_en,
    iff(
        legal_gender_name_en not in ('Male', 'Female'), 'Other', legal_gender_name_en
    ) as legal_gender_name_label,
    all_players_status_code,
    nvl(all_players_status_name_en, 'Player') as all_players_status_name_en,
    country_code,
    country_name_en || ' (' || country_code || ')' as country_name_en,
    eth.ethnicity_code,  -- HL: Hispainic or LatinX
    eth.ethnicity_name_en,
    iff(eth.ethnicity_code = 'HL', null, race.race_code) as race_1_code,
    iff(eth.ethnicity_code = 'HL', null, race.race_name_en) as race_1_name_en,
    iff(
        eth.ethnicity_code = 'HL',
        eth.ethnicity_name_en,
        iff(race.race_name_en = 'No Selection', 'Prefer Not to Say', race.race_name_en)
    ) ethnicity_name_label,
    group_seniority as group_seniority_date,
    company_seniority as company_seniority_date,
    (hr.user_id is not null) as hr_manager_flag
from final f
left join ed using (personal_id, user_id, edvalidfrom)
left join pi using (personal_id, pivalidfrom)
left join legal_gender_v1 lg using (legal_gender_code)
left join
    all_players_status_v1 aps on pi.all_player_status_id = aps.all_players_status_id
left join country_v1 con using (country_code)
left join personal_information_local_usa_v1 pilu using (personal_id)
left join ethnicity_v1 eth using (ethnicity_id)
left join race_v1 race on pilu.race_1_id = race.race_id
left join hr using (user_id)
union all
select
    -1,
    '',
    '',
    '1900-01-01',
    '9999-12-31',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    'Player',
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
order by 2, 3, 4
