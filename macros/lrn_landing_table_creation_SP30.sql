{% macro lrn_landing_table_creation_SP30() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;

        CREATE OR REPLACE TABLE PA_AP_ATTR_VAL (
        AP_ATTR_ID number (38,0) ,
        VALUE varchar (255),
        LST_UPD_USR varchar (255),
        LST_UPD_TSTMP  timestamp );
                
        CREATE OR REPLACE TABLE PA_AP_ATTR_TYPE (
        AP_TYPE_ID varchar (255),
        ATTR_ID varchar (255),
        LST_UPD_USR varchar (255),
        LST_UPD_TSTMP timestamp );
        
        CREATE OR REPLACE TABLE PA_AP_ATTRIBUTE (
        ATTR_ID varchar (255),
        MANDATORY_ATTR varchar (255),
        SELECTOR_NAME varchar (255),
        DFLT_OPER_ID varchar (255),
        ATTR_DESC varchar (255),
        LABEL_ID varchar (255),
        ATTR_DATA_TYPE varchar (255),
        ATTR_DATA_FORMAT varchar (255),
        TABLE_NAME varchar (255),
        COL_NAME varchar (255),
        CUST_COL_NUM varchar (255),
        REL_TAB_NAME varchar (255),
        REL_PRNT_COL_NAME varchar (255),
        REL_CHLD_COL_NAME varchar (255),
        IS_CUST_COLUMN varchar (255),
        LST_UPD_USR varchar (255),
        LST_UPD_TSTMP timestamp );
        
        CREATE OR REPLACE TABLE PA_AP_ASSOC_ATTR (
        AP_ATTR_ID  number (38,0),
        AP_ID varchar (255),
        OPER_ID varchar (255),
        ATTR_ID varchar (255),
        ATTR_GROUP  number (38,0),
        LST_UPD_USR varchar (255),
        LST_UPD_TSTMP timestamp);
                
        
        CREATE OR REPLACE TABLE     PA_AP_ATTR_VAL_K AS SELECT "AP_ATTR_ID","VALUE" FROM PA_AP_ATTR_VAL;
        CREATE OR REPLACE TABLE     PA_AP_ATTR_TYPE_K AS SELECT "AP_TYPE_ID","ATTR_ID" FROM PA_AP_ATTR_TYPE;
		CREATE OR REPLACE TABLE     PA_AP_ATTRIBUTE_K AS SELECT "ATTR_ID" FROM PA_AP_ATTRIBUTE;
		CREATE OR REPLACE TABLE     PA_AP_ASSOC_ATTR_K AS SELECT "AP_ATTR_ID" FROM PA_AP_ASSOC_ATTR;
		
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
