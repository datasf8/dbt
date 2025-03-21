{% macro stry0403594_range_gassen() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES where range_type='GASSEN';
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',0,12,1,0,'< 12 Months');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',12,24,1,0,'12 to 24 Months');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',24,48,1,0,'24 to 48 Months');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',48,59,1,0,'48 to 59 Months');
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_RANGES values('GASSEN',59,9999999,1,0,'>= 59 Months');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}