{{ config(materialized="table", unique_key="DDEP_EMPLOYEE_ID_DDEP", transient=false) }}
select distinct
    collate(replace(value:"userId", '""', ''),'en-ci') as ddep_employee_id_ddep,
    collate(replace(value:"empId", '""', ''),'en-ci') as ddep_person_id_ddep,
    collate(replace(value:"username", '""', ''),'en-ci') as ddep_upn_ddep,
    collate(replace(value:"firstName", '""', ''),'en-ci') as ddep_first_name_ddep,
    collate(replace(value:"lastName", '""', ''),'en-ci') as ddep_last_name_ddep,
    collate(replace(value:"email", '""', ''),'en-ci') as ddep_email_ddep,
    to_date(DATEADD(MS,
        replace(split_part(value:"hireDate", '(', 2), ')/', ''), '1970-01-01')
    ) as ddep_hire_date_ddep,
    collate(replace(value:"gender", '""', ''),'en-ci') as ddep_gender_ddep,
    collate(replace(value:"status", '""', ''),'en-ci') as ddep_status_ddep,
    collate(replace(value:"custom01", '""', ''),'en-ci') as ddep_ec_status_ddep,
    collate(replace(split_part(value:"location", '(', -1), ')', ''),'en-ci') as ddep_location_code_ddep,
    left(
        value:"location",
        len(value:"location") - charindex('(', reverse(value:"location"))
    ) as ddep_location_ddep,
    to_date(DATEADD(MS,
        replace(split_part(value:"dateOfBirth", '(', 2), ')/', ''), '1970-01-01')
    ) as ddep_birth_date_ddep,
    to_date(DATEADD(MS,
        replace(split_part(value:"customdate2", '(', 2), ')/', ''), '1970-01-01')
    ) as ddep_group_seniority_ddep,
    value:"nationality" as ddep_nationality_ddep,
    collate(replace(value:"country", '""', ''),'en-ci') as ddep_country_code_ddep,
    collate(replace(value:"country", '""', ''),'en-ci') as ddep_country_ddep,
    collate(replace(split_part(value:"jobCode", '(', 2), ')', ''),'en-ci') as ddep_job_role_code_ddep,
    collate(split_part(value:"jobCode", '(', 1),'en-ci') as ddep_job_role_ddep,
    collate(replace(
        split_part(value:"custom07", '(', 2), ')', ''
    ),'en-ci') as ddep_specialization_code_ddep,
    collate(split_part(value:"custom07", '(', 1),'en-ci') as ddep_specialization_ddep,
    collate(replace(
        split_part(value:"custom06", '(', 2), ')', ''
    ),'en-ci') as ddep_professional_field_code_ddep,
    collate(split_part(value:"custom06", '(', 1),'en-ci') as ddep_professional_field_ddep,
    try_to_date(replace(value:"lastReviewDate", '""', '')) as ddep_job_seniority_ddep,
    collate(replace(split_part(value:"custom04", '(', 2), ')', ''),'en-ci') as ddep_company_code_ddep,
    collate(split_part(value:"custom04", '(', 1),'en-ci') as ddep_company_ddep,
    collate(replace(split_part(value:"custom03", '(', 2), ')', ''),'en-ci') as ddep_division_code_ddep,
    collate(split_part(value:"custom03", '(', 1),'en-ci') as ddep_division_ddep,
    collate(replace(split_part(value:"custom02", '(', 2), ')', ''),'en-ci') as ddep_bu_type_code_ddep,
    collate(split_part(value:"custom02", '(', 1),'en-ci') as ddep_bu_type_ddep,
    collate(replace(split_part(value:"department", '(', 2), ')', ''),'en-ci') as ddep_bu_code_ddep,
    collate(split_part(value:"department", '(', 1),'en-ci') as ddep_bu_ddep,
    collate(replace(value:"title", '""', ''),'en-ci') as ddep_position_ddep,
    collate(replace(value:"custom10", '""', ''),'en-ci') as ddep_employee_group_ddep
from {{ ref("stg_employee_profile") }}, lateral flatten(input => src:d:results)