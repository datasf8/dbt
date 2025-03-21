{{ config(materialized="table") }}
select a.job_name,a.job_start_time,a.job_end_time,a.job_execution_status,a.last_run from(
select m.*,rank() over (partition by job_name  order by job_end_time desc) as rnk_f  from    
(select *,rank() over(partition by job_name,job_execution_status,previous_status order by job_start_time desc) as rnk  from (    
select job_name,job_start_time,job_end_time,job_execution_status,last_run,lead(job_execution_status) over(order by job_start_time desc) as previous_status
from 
(select b.job_name,b.job_start_time,b.job_end_time,b.job_execution_status,a.last_run
from 
(select job_name,max(job_end_time) as last_run
from hrdp_stg_{{ env_var('DBT_REGION') }}_db.cmn_stg_sch.stg_job_execution_log b group by 1)a,
(hrdp_stg_{{ env_var('DBT_REGION') }}_db.cmn_stg_sch.stg_job_execution_log) b
where a.job_name=b.job_name 
-- and a.job_name='sdds_employee_central_api_job'
)) qualify rnk=1 order by job_name,job_start_time desc)m 
where m.job_execution_status='success'
qualify rnk_f=1
order by job_end_time desc)a