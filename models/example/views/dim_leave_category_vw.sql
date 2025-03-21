{{ config(schema="cmn_pub_sch") }}

select * from 
{{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_param_leave_category