{% macro stry0454202_lms_tables() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;

        create or replace TABLE PA_CPNT_EVTHST_COST (
            CPNT_EVTHST_COST_ID VARCHAR(1000),
            STUD_ID             VARCHAR(90),
            CPNT_TYP_ID         VARCHAR(90),
            CPNT_ID             VARCHAR(90),
            REV_DTE             TIMESTAMP_NTZ(9),
            CMPL_STAT_ID        VARCHAR(90),
            COMPL_DTE           TIMESTAMP_NTZ(9),
            FIN_VAR_ID          VARCHAR(100),
            AMOUNT              NUMBER(38,5),
            CURRENCY_CODE       VARCHAR(50),
            COMMENTS            VARCHAR(168000),
            LST_UPD_USR         VARCHAR(500),
            LST_UPD_TSTMP      TIMESTAMP_NTZ(9)
            );


        create or replace TABLE PA_CPNT_EVTHST_COST_K (
	                            CPNT_EVTHST_COST_ID VARCHAR(1000));
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}