select  subject_area,dashboards,themes,power_bi_fact_table,airflow_job_name,dbt_fact_table,
job_start_time,job_end_time,job_execution_status,last_run from  {{ ref('dashboards_refresh_last_successful_runs') }} b left join  {{ ref('execution_logs_lookup') }} a  on
a.airflow_job_name=b.job_name where subject_area is not null 
