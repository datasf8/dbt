{% macro lrn_landing_table_creation_SP31() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;

        CREATE OR REPLACE TABLE PA_CPNT_CPTY (
        CPNT_ID varchar (255) ,
        CPNT_TYP_ID varchar (255),
        REV_DTE timestamp,
        CPTY_ID  varchar (255) ,
        CPTY_LEVEL number (38,0),
        LST_UPD_USR varchar (255),
        LST_UPD_TSTMP timestamp );            
        
        CREATE OR REPLACE TABLE     PA_CPNT_CPTY_K AS SELECT "CPNT_ID","CPNT_TYP_ID","REV_DTE",CPTY_ID FROM PA_CPNT_CPTY;
        		
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
