{% macro stry0461552_lms_tables() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;


        CREATE OR REPLACE TABLE PA_CPTY (
                CPTY_ID                                	VARCHAR(90),
                CPTY_TYPE                              	VARCHAR(90),
                DMN_ID                                 	VARCHAR(90),
                NOTACTIVE                              	VARCHAR(1),
                CPTY_DESC                              	VARCHAR(2000),
                COMMENTS                               	VARCHAR(2000),
                CPTY_CATEGORY_ID                       	VARCHAR(90),
                RATING_SCALE_ID                        	VARCHAR(90),
                CPTY_SOURCE_ID                         	VARCHAR(90),
                VERSION_NUMBER                         	NUMBER(38,5),
                CPTY_EXPLANATION                       	VARCHAR(2000),
                ESIG_ENABLED                           	VARCHAR(1),
                EXTERNAL                               	VARCHAR(1),
                OFT_SEARCH_INDX_NOTIFIER               	VARCHAR(90),
                LST_UPD_USR                            	VARCHAR(90),
                LST_UPD_TSTMP                          	TIMESTAMP,
                CAPABILITY_GROUP_SYS_GUID              	VARCHAR(90),
                EXTERNAL_CODE                          	VARCHAR(128),
                IS_CORE                                	VARCHAR(1),
                IS_CRITICAL                            	VARCHAR(1),
                IS_TRENDING                            	VARCHAR(1),
                JPB_INTERNAL_ID                        	VARCHAR(90),
                SOURCE_VERSION_TS                       TIMESTAMP
          );


        CREATE OR REPLACE TABLE PA_CPTY_K (
            CPTY_ID                                	VARCHAR(90)
            );
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}