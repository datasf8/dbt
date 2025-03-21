select
    sk_ethnicity_id,
    ethnicity_id,
    ethnicity_code,
    ethnicity_start_date,
    ethnicity_end_date,
    country_code,
    ethnicity_name_en,
    ethnicity_name_fr,
    ethnicity_status
from {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_SDDS_PERSON_SCH") }}.ethnicity