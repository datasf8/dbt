{{
    config(
        materialized="table",
        transient=false,
    )
}}

SELECT 
*
FROM 
{{ ref("dim_param_variable_pay_campaign") }}