select
    empl_pk_employee_empl as employee_key,
    empl_person_id_external_empl as person_id_external,
    empl_user_id_empl as user_id,
    empl_person_id_empl as person_id,
    empl_first_name_empl as employee_first_name,
    empl_last_name_empl as employee_last_name,
    upper(employee_last_name) || ' ' || employee_first_name as empl_full_name,
    empl_creation_date_empl as creation_date,
    empl_modification_date_empl as modification_date,
    empl_dt_begin_empl as begin_date,
    empl_dt_end_empl as end_date,
    empl_date_of_birth_empl as date_of_birth,
    empl_age_empl as age,
    empl_assignment_class_empl as assignment_class,
    empl_ethnicity_empl as ethnicity,
    empl_race1_empl as race1,
    empl_ethnicity_calculated_empl as ethnicity_calculated
from
    {{ ref("dim_employee_snapshot") }}
    
