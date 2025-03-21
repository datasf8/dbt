{{
    config(
        materialized="incremental",
        unique_key="VARIABLE_PAY_PROGRAM_NAME",
        transient=true,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
{% set results =cmp_tables_cnt_chk('VARIABLE_PAY_GLOBAL_BS2024') %}
select
    user_id,
    person_id_external,
    variable_pay_program_name,
    legal_first_name,
    legal_last_name,
    bonusable_salary,
    payout_percent,
    business_unit_payout,
    business_target,
    business_rating,
    team_result_payout_amount,
    people_target,
    people_rating,
    individual_result_payout_amount,
    final_bonus_payout,
    payout_as_percent_of_target,
    payout_as_percent_of_bonusable_salary,
    gender,
    total_target_amount,
    hire_date,
    null as all_players_status,
    null as comp_plan_owner
from {{ source("landing_tables_CMP", "VARIABLE_PAY_GLOBAL_REPORT") }}
union
select
    user_id,
    person_id_external,
    variable_pay_program_name,
    legal_first_name,
    legal_last_name,
    bonusable_salary,
    payout_percent,
    business_unit_payout,
    business_target,
    business_rating,
    team_result_payout_amount,
    people_target,
    people_rating,
    individual_result_payout_amount,
    final_bonus_payout,
    payout_percent_target,
    payout_percent_bonusable_salary,
    gender,
    total_target_amount,
    hire_date,
    all_players_status,
    null as comp_plan_owner
from {{ source("landing_tables_CMP", "VARIABLE_PAY_GLOBAL_BS2023") }}
union
select
    "User ID" AS user_id,
    "Person ID External" AS person_id_external,
    "Variable Pay Program Name" AS variable_pay_program_name,
    "Legal First Name" AS legal_first_name,
    "Legal Last Name" AS legal_last_name,
    "Bonusable Salary"::number(38,2) AS bonusable_salary,
    "Payout %"::number(38,2) AS payout_percent,
    "Business Unit Payout"::number(38,2) AS business_unit_payout,
    "Business Target"::number(38,2) AS business_target,
    "Business Rating" AS business_rating,
    "Team Result Payout Amount"::number(38,2) AS team_result_payout_amount,
    "People Target"::number(38,2) AS people_target,
    "People Rating" AS people_rating,
    "Total Payout (incl. GLS)"::number(38,2) AS individual_result_payout_amount,
    "Final Bonus Payout"::number(38,2) AS final_bonus_payout,
    "Payout as % of Target"::number(38,2) AS payout_percent_target,
    "Payout as % of Bonusable Salary"::number(38,2) AS payout_percent_bonusable_salary,
    "Gender" AS gender,
    "Total Target Amount"::number(38,2) AS total_target_amount,
    "Hire Date" AS hire_date,
    "All Player Status" AS all_players_status,
    "VarPay Manager Form Owner" AS comp_plan_owner
from {{ source("landing_tables_CMP", "VARIABLE_PAY_GLOBAL_BS2024") }}