select
    business_unit_code,
    business_unit,
    company_code,
    company,
    country_code,
    a.country,
    zones as zone
from
    (
        select distinct
            ddep_bu_code_ddep as business_unit_code,
            ddep_bu_ddep as business_unit,
            ddep_company_code_ddep as company_code,
            ddep_company_ddep as company,
            ddep_country_code_ddep as country_code,
            ddep_country_ddep as country,
            business_unit_code_rank
        from
            (
                select
                    ddep_bu_code_ddep,
                    ddep_bu_ddep,
                    ddep_company_code_ddep,
                    ddep_company_ddep,
                    ddep_country_code_ddep,
                    ddep_country_ddep,
                    dense_rank() over (
                        partition by ddep_bu_code_ddep order by ddep_bu_ddep desc
                    ) as business_unit_code_rank

                from {{ ref("dim_employee_profile") }}

            )
        where business_unit_code_rank = 1
    ) a
left outer join {{ ref("country_zones") }} b on a.country = b.country
where a.business_unit is not null
