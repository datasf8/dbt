{% macro drop_unused_tables() %}
 
    {% set qDropobjects %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
 

        drop table if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_AGGREGATE;
        drop table if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_EMPLOYEE_BY_MEASURE;
        drop table if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_HEADCOUNT_DETAILS;
        drop table if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_PUB_AGGREGATE_HISTO;
        

        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_AGGREGATE_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_EMPLOYEE_BY_MEASURE_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_CSRD_HEADCOUNT_DETAILS_VW;

        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CSRD_PUB_SCH.DIM_CSRD_EMPLOYEE_GROUP_MAPPING_V1_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CSRD_PUB_SCH.DIM_CSRD_MEASURES_REFERENTIAL_V1_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CSRD_PUB_SCH.FACT_CSRD_AGG_HISTO_V1_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CSRD_PUB_SCH.FACT_CSRD_EMPLOYEES_BY_MEASURE_HISTO_V1_VW;
        drop view if exists HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CSRD_PUB_SCH.FACT_CSRD_HEADCOUNT_DETAILS_HISTO_V1_VW;

    {% endset %}
    {% do run_query(qDropobjects) %}

 
{% endmacro %}