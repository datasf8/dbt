{% macro rls_delete_20230920() %}

-------------- Prerequisite to Set the Ground ---------
USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;

-------------- Actual Delete Statements ---------
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='CMP' and sfrole='EX10 - HR CONTROLLER';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='CMP' and sfrole='HR15 - HR PROFESSIONNAL (VIEW)';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='CMP' and sfrole='HR20 - GLOBAL HR EXECUTIVE TEAM';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='CMP' and sfrole='HR30 - HR INTERNATIONAL MOBILITY';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='CMP' and sfrole='HRMA - HR MANAGER on DIRECT REPORT (EC)';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='EC' and sfrole='EX10 - HR CONTROLLER';
delete from cmn_lnd_sch.param_sf_roles where subjectdomain='EC' and sfrole='HRMA - HR MANAGER on DIRECT REPORT (EC)';


{% endmacro %}