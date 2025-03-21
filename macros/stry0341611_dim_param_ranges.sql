{% macro stry0341611_dim_param_ranges() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES where range_type='AGESEN';
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',0,20,1,0,'< 20');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',20,25,1,0,'20 - 24');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',25,35,1,0,'25 - 34');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',35,45,1,0,'35 - 44');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',45,55,1,0,'45 - 54');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',55,60,1,0,'55 - 59');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',60,65,1,0,'60 - 65');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('AGESEN',65,9999999,1,0,'> 65');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}