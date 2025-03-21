{{
    config(
        materialized="table",
        transient=false,
    )
}}

SELECT 
*
FROM 
{{ ref("dim_operational_area") }}