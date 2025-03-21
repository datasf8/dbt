{% macro lrn_column_mismatch_lms_tables() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;
        ALTER TABLE PA_STUDENT add column ORIG_START_DATE varchar(1000);
        ALTER TABLE PA_STUDENT add column MULTI_EMP_TYPE varchar(1000);
        create or replace TABLE PA_CPNT_CPTY (
            CPNT_ID VARCHAR(255) COLLATE 'en-ci',
            CPNT_TYP_ID VARCHAR(255) COLLATE 'en-ci',
            REV_DTE TIMESTAMP_NTZ(9),
            CPTY_ID VARCHAR(255) COLLATE 'en-ci',
            CPTY_LEVEL NUMBER(38,0),
            LST_UPD_USR VARCHAR(255) COLLATE 'en-ci',
            LST_UPD_TSTMP varchar(1000) COLLATE 'en-ci',
            ORIGIN varchar(1000) COLLATE 'en-ci',
            UPDATED_BY varchar(1000) COLLATE 'en-ci'
        );


        create or replace TABLE PA_CPNT_EVTHST_COST_K (
	                            CPNT_EVTHST_COST_ID VARCHAR(1000));
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}