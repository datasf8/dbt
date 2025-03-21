select
    job_role_code,
    job_role,
    specialization_code,
    specialization,
    professional_field_code,
    professional_field
from
    (
        select
            row_number() over (
                partition by job_role_code order by number_employees desc
            ) rownumber,
            job_role_code,
            job_role,
            specialization_code,
            specialization,
            professional_field_code,
            professional_field
        from
            (
                select
                    ddep_job_role_code_ddep as job_role_code,
                    ddep_job_role_ddep as job_role,
                    ddep_specialization_code_ddep as specialization_code,
                    ddep_specialization_ddep as specialization,
                    ddep_professional_field_code_ddep as professional_field_code,
                    ddep_professional_field_ddep as professional_field,
                    count(distinct ddep_employee_id_ddep) as number_employees
                from {{ ref("dim_employee_profile") }}

                where job_role_code != ''
                group by
                    ddep_job_role_code_ddep,
                    ddep_job_role_ddep,
                    ddep_specialization_code_ddep,
                    ddep_specialization_ddep,
                    ddep_professional_field_code_ddep,
                    ddep_professional_field_ddep
            ) ja_n1
    ) ja_n2
where rownumber = 1
