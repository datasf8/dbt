{% macro stry0421666_csrd_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}
 
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0421666';
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0421663';

        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('CSRD','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H16_CSRD_EU_{{ env_var('DBT_REGION') }}_PRIVATE','CSRD_HEADCOUNT_DETAILS','USER_ID,FLAG_PURGED',current_timestamp(),'STRY0421666');

   
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('CSRD','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H16_CSRD_EU_{{ env_var('DBT_REGION') }}_PRIVATE','CSRD_HEADCOUNT_DETAILS_HISTO','USER_ID,FLAG_PURGED',current_timestamp(),'STRY0421663');

 
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('CSRD','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_CSRD_HEADCOUNT_DETAILS','USER_ID,FLAG_PURGED',current_timestamp(),'STRY0421666');

 
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('CSRD','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_CSRD_HEADCOUNT_DETAILS_HISTO','USER_ID,FLAG_PURGED',current_timestamp(),'STRY0421663');
 
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}