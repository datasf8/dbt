{{
    config(
        materialized="table",
        transient=false,
    )
}}

SELECT 
*
FROM 
{{ ref("dim_employee_group") }}