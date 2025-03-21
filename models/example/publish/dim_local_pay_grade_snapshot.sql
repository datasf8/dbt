{{
    config(
        materialized="table",
        transient=false,
    )
}}

SELECT 
*
FROM 
{{ ref("dim_local_pay_grade") }}