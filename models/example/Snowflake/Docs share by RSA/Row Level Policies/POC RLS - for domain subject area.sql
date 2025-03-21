--- DRIVING TABLES
select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_EMPLOYEE_MANAGER" order by 2,3;
select * from  "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_EMPLOYE_PROFILE";
select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER";
select * from  "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_PARAM_SF_ROLES";
select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP" order by 1;


grtgroup is who can see
tgtgroup is what can they see

------GAP IN THE DATA
-- MISSING CODES
insert into  "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP"
select 'EX30_WW', grtgroup,tgtgroup from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP"
where sfrole=538
union
select 'EX20_US', grtgroup,tgtgroup from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP"
where sfrole=697
union
select 'EX10_FR', grtgroup,tgtgroup from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP"
where sfrole=700;

--- ADDING THE GROUP_IDs for TARGET REFERENCE'


select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER" where grp_id in (2312,3203,3440) ;
insert into 
"HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER"
values (2312,'ANDREA.RAJOLAPESCARINI@LOREAL.COM'),
(3203,'SAGAR.MORAKHIA@LOREAL.COM'),
(3440,'WAEL.BENAMAR@LOREAL.COM');



--- CREATE VIEW TO DRIVE RLS
create or replace view "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW AS 
select distinct 'HEIR', dpsr.SUBJETDOMAIN as subjectdomain 
,rgu_grt.employee_id as ACCESS_EMPLOYEE_ID
,rgu_tgt.employee_id as EMPLOYEE_ID
from
"HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_PARAM_SF_ROLES" dpsr
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_SF_ROLES_GROUP" rsrg
on dpsr.sfrole=rsrg.sfrole
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER" rgu_grt
on rgu_grt.grp_id=rsrg.grtgroup
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER" rgu_tgt
on rgu_tgt.grp_id=rsrg.tgtgroup
UNION 
-- manager relationship
select distinct 'MANAGER', dpsr.SUBJETDOMAIN,rme.rems_manager_id_ddep as ACCESS_EMPLOYEE_ID, dep.ddep_employee_id_ddep as EMPLOYEE_ID
FROM "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_EMPLOYEE_MANAGER" rme
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_EMPLOYE_PROFILE" dep
on dep.ddep_pk_employee_key_ddep=rme.rems_employee_key_ddep
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_GROUP_USER" rgu
on rgu.employee_id=dep.ddep_employee_id_ddep
JOIN "HRDP_CORE_DV_DB"."PMG_CORE_SCH"."DIM_PARAM_SF_ROLES" dpsr
on dpsr.flg_emp_man=1
order by 1,2,3
;


select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW;

--- CREATE RLP
CREATE OR REPLACE ROW ACCESS POLICY "HRDP_CORE_DV_DB"."PMG_CORE_SCH".PPOC_MOCK_PMGM_RLS AS
(
    P_EMPLOYEE_ID VARCHAR
)
RETURNS BOOLEAN ->
EXISTS
(
    SELECT 1
    FROM "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW
    WHERE ACCESS_EMPLOYEE_ID         = CURRENT_USER()
    AND SUBJECTDOMAIN        = 'PMGM'
    AND EMPLOYEE_ID        = P_EMPLOYEE_ID
);

select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW;


--- CREATE MOCK DATA TABLE
create or replace table "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL
(SUBJECTDOMAIN varchar,
EMPLOYEEID varchar,
COURSE varchar,
STATUS char(1)
);



select * from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL;

-- ADDING MOCK DATA
insert into "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL
select distinct SUBJECTDOMAIN, EMPLOYEE_ID, 'SF ADVANCE',case when EMPLOYEE_ID < '00025152' then 'Y' else 'N' end   from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW
;
insert into "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL
select distinct SUBJECTDOMAIN, EMPLOYEE_ID, 'SF CORE',case when EMPLOYEE_ID < '00035152' then 'Y' else 'N' end   from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW
;



--- VERIFY THE OUTCOME OF THE QUERIES PER USER
select access_employee_id,course,sum(case when status='Y' then 1 else 0 end) as num_courses from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL
a
join "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_POC_VW b
   on a.SUBJECTDOMAIN        = b.SUBJECTDOMAIN
    AND a.EMPLOYEEID        = b.EMPLOYEEID
where access_employee_id in ('ANDREA.RAJOLAPESCARINI@LOREAL.COM','SAGAR.MORAKHIA@LOREAL.COM','WAEL.BENAMAR@LOREAL.COM')
    group by 1,2;


--- APPLY RLS
alter table "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL add row access policy "HRDP_CORE_DV_DB"."PMG_CORE_SCH".PPOC_MOCK_PMGM_RLS ON (EMPLOYEEID);


--- VERIFY OUTPUT FOR FIRST USER (1 and 1)
select course,sum(case when status='Y' then 1 else 0 end) as num_courses from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL group by 1;

--- VERIFY OUTPUT FOR SECOND USER (0 and 0)
execute using policy_context (CURRENT_USER => 'SAGAR.MORAKHIA@LOREAL.COM') AS select course,sum(case when status='Y' then 1 else 0 end) as num_courses from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL group by 1;

--- VERIFY OUTPUT FOR THIRD USER (no result)
execute using policy_context (CURRENT_USER => 'WAEL.BENAMAR@LOREAL.COM') AS select course,sum(case when status='Y' then 1 else 0 end) as num_courses from "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL group by 1;


---  DROP RLS from TABLE
alter table "HRDP_CORE_DV_DB"."PMG_CORE_SCH".RLS_MAIN_TBL drop row access policy "HRDP_CORE_DV_DB"."PMG_CORE_SCH".PPOC_MOCK_RLS;

---  DROP RLS
DROP ROW ACCESS POLICY "HRDP_CORE_DV_DB"."PMG_CORE_SCH".PPOC_MOCK_RLS;

