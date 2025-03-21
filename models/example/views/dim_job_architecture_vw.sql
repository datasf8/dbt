select
    joar_pk_joar as job_architecture_key,
    joar_dt_begin_joar as begin_date,
    joar_dt_end_joar as end_date,
    joar_job_role_code_joar as job_role_code,
    joar_job_role_joar as job_role,
    joar_specialization_code_joar as specialization_code,
    joar_specialization_joar as specialization,
    collate(
        joar_specialization_code_joar || ' (' || joar_specialization_joar || ')',
        'en-ci'
    ) as specialization_field,
    joar_professional_field_code_joar as professional_field_code,
    joar_professional_field_joar as professional_field,
    collate(
        joar_professional_field_code_joar
        || ' ('
        || joar_professional_field_joar
        || ')',
        'en-ci'
    ) as professional_field_concat,
    collate(
        iff(
            upper(specialization) = upper('Beauty Advisory'), 'BAs Only', 'Without BAs'
        ),
        'en-ci'
    ) as ba,
    collate(
        iff(
            upper(specialization) = upper('Internship'),
            'Interns Only',
            'Without Interns'
        ),
        'en-ci'
    ) as intern,
    collate(
        iff(
            upper(specialization) like upper('Apprenticeship'),
            'Apprentices Only',
            'Without Apprentices'
        ),
        'en-ci'
    ) as apprentice
from {{ ref("dim_job_architecture_snapshot") }}
