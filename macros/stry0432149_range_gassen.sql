{% macro stry0432149_range_gassen() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES where range_type='GASSEN';
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',0,1,1,0,'< 1 Year');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',1,2,1,0,'1 to 2 Years');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',2,4,1,0,'2 to 4 Years');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',4,5,1,0,'4 to 5 Years');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',5,9999999,1,0,'>= 5 Years');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}