{{ config(materialized='table',unique_key='GRP_ID',transient=false) }}

select  SGU.GRP_ID,collate(SGU.EMPLOYEE_ID,'en-ci') as EMPLOYEE_ID
,collate(coalesce(SRGF.GRTGROUPNAME,SRGFT.TGTGROUPNAME),'en-ci') as GROUPNAME from
(
select distinct
split_part(split_part(file_name,'/',-1),'_',1) as GRP_ID,
replace(SRC:"userId",'""','') as EMPLOYEE_ID
from {{ ref('stg_group_user') }}
) SGU
  left outer join  (select distinct GRTGROUP,GRTGROUPNAME from {{ ref('stg_sf_roles_group_flatten') }}) SRGF
  on SGU.GRP_ID=SRGF.GRTGROUP
    left outer join  (select distinct TGTGROUP,TGTGROUPNAME from {{ ref('stg_sf_roles_group_flatten') }}) SRGFT
  on SGU.GRP_ID=SRGFT.TGTGROUP
