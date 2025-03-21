{% macro stry0351284_assignment_profile_ref() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL
            where ASSIGNMENT_PROFILE_ID='AP_ONBMUST_L’OREAL_DISCOVERY_TARGET';
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL values
            ('AP_BUSINESSMUST_GST_TARGET','Going Sustainable Together','MUST','BUSINESSMUST','Business Must')
            ,('AP_JBMUST_THEWAYWEWORKWITHOURSUPPLIERS_TARGET','The Way we Work With Our Suppliers','MUST','JBMUST','Job Must')
            ,('AP_ONBMUST_LOREAL_DISCOVERY_TARGET','L’Oreal Discovery','MUST','ONBMUST','Onboarding Must')
        ;

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}