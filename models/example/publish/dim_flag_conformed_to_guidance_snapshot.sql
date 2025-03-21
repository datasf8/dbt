{{
    config(
        materialized="table",
        transient=false,
    )
}}

SELECT 
*
FROM 
{{ ref("dim_flag_conformed_to_guidance") }}