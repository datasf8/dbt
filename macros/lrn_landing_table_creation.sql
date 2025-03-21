{% macro lrn_sdds_landing_tbl_c() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        --------------------Create Schema---------------------
        USE SCHEMA LRN_LND_SCH;

        CREATE OR REPLACE TABLE PA_CPNT_FORMULA (
        CPNT_TYP_ID VARCHAR(100),
        CPNT_ID  VARCHAR(100),
        REV_DTE TIMESTAMP,
        FIN_VAR_ID  VARCHAR(100),
        CURRENCY_CODE  VARCHAR(50),
        FORMULA VARCHAR(255),
        CUSTOM_COST_HANDLER_CLASS VARCHAR(1000) ,
        IS_DEFAULT VARCHAR(10),
        LST_UPD_USR VARCHAR(100),
        LST_UPD_TSTMP TIMESTAMP);
        
        CREATE OR REPLACE TABLE PA_SCHED_FORMULA (
        SCHD_ID VARCHAR(1000),
        FIN_VAR_ID VARCHAR(1000),
        CURRENCY_CODE VARCHAR(50),
        FORMULA VARCHAR(255),
        CUSTOM_COST_HANDLER_CLASS VARCHAR(1000),
        COST_REALIZED VARCHAR(100),
        IS_DEFAULT VARCHAR(10),
        LST_UPD_USR VARCHAR(100),
        LST_UPD_TSTMP TIMESTAMP);
        
        CREATE OR REPLACE TABLE PA_SCHED_USER (
        COL_NUM NUMBER(28,0),
        SCHD_ID VARCHAR(1000),
        USER_VALUE VARCHAR(1000),
        OFT_SEARCH_INDX_NOTIFIER VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP  TIMESTAMP);
        
        CREATE OR REPLACE TABLE PA_CURRENCY (
        CURRENCY_CODE VARCHAR(50),
        DESCRIPTION VARCHAR(16777216),
        ACTIVE VARCHAR(1),
        IS_DEFAULT VARCHAR(1),
        SYMBOL VARCHAR(50),
        LABEL_ID VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP);
        
        
        CREATE OR REPLACE TABLE PA_SCHED (
        SCHD_ID VARCHAR(1000),
        DESCRIPTION VARCHAR(16777216),
        GRP_INSTANCE_ID VARCHAR(1000),
        DMN_ID VARCHAR(1000),
        SCHD_CPNT VARCHAR(1000),
        CPNT_TYP_ID VARCHAR(1000),
        ACT_CPNT_ID VARCHAR(1000),
        REV_DTE TIMESTAMP,
        MIN_ENRL NUMBER(38,0),
        MAX_ENRL NUMBER(38,0),
        CONTACT VARCHAR(1000),
        EMAIL_ADDR VARCHAR(1000),
        PHON_NUM VARCHAR(1000),
        FAX_NUM VARCHAR(1000),
        ENRL_CUT_DTE DATETIME,
        SELF_ENRL VARCHAR(1000),
        RMNDR_SENT VARCHAR(1000),
        SCHD_DESC VARCHAR(1000),
        COMMENTS VARCHAR(16777216),
        NOTACTIVE VARCHAR(1000),
        FACILITY_ID VARCHAR(1000),
        CANCELLED VARCHAR(1000),
        CANCEL_DTE DATETIME,
        AUTO_FILL_ENRL VARCHAR(1000),
        TOTAL_LEARNING_COST NUMBER(38,0),
        CLOSED_DATE DATETIME,
        CHGBCK_METHOD VARCHAR(1000),
        VLE_AUTO_RECORD_LRNGEVNTS VARCHAR(1000),
        VLE_MIN_SUCCESS_ATTENDANCE_PCT VARCHAR(1000),
        VLE_CMPL_STAT_ID VARCHAR(1000),
        VLE_FAILURE_CMPL_STAT_ID VARCHAR(1000),
        TAP_DEF_ID VARCHAR(1000),
        APPROVAL_REQD VARCHAR(1000),
        TIMEZONE_ID VARCHAR(1000),
        DISPLAY_IN_SCHD_TZ VARCHAR(1000),
        USER_CAN_WAITLIST VARCHAR(1000),
        COST_CURRENCY_CODE VARCHAR(1000),
        EMAIL_STUDENT VARCHAR(1000),
        EMAIL_INSTRUCTOR VARCHAR(1000),
        EMAIL_SUPERVISOR VARCHAR(1000),
        EMAIL_CONTACTS VARCHAR(1000),
        OFT_SEARCH_INDX_NOTIFIER VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000) ,
        LST_UPD_TSTMP TIMESTAMP,
        REG_EMAIL_STUDENT VARCHAR(1000),
        REG_EMAIL_INSTRUCTOR VARCHAR(1000),
        REG_EMAIL_SUPERVISOR VARCHAR(1000),
        REG_EMAIL_CONTACTS VARCHAR(1000),
        REG_EMAIL_SLOT_CONFIRMATIONS VARCHAR(1000),
        REMOVE_ITEM_FROM_LRN_PLAN VARCHAR(1000),
        SUPER_ENRL VARCHAR(1000),
        ORIGIN VARCHAR(1000),
        WITHDRAW_APPROVAL_REQD VARCHAR(1000),
        WITHDRAW_TAP_DEF_ID VARCHAR(1000),
        WITHDRAW_CUTOFF_DTE VARCHAR(1000),
        CERT_TEMPLATE_SYS_GUID VARCHAR(1000),
        RULE_SYS_GUID VARCHAR(1000),
        LOA_EXTERNAL_CODE VARCHAR(1000),
        REGISTRATION_INSTRUCTIONS VARCHAR(1000),
        ENABLE_CANCELLATION_REASON VARCHAR(1000),
        ALLOCATION_CODE_SYS_GUID VARCHAR(1000),
        IS_SCHED_VIRTUAL VARCHAR(1000),
        SHORT_DESCRIPTION VARCHAR(16777216),
        WITHDRAW_SUP_ASSIGNED_SCHED VARCHAR(1000)
        );
        
        CREATE OR REPLACE TABLE PA_ENROLL_SEAT (
        ENRL_SEAT_ID VARCHAR(1000),
        STUD_ID VARCHAR(1000),
        ENRL_SLOT_ID VARCHAR(1000),
        SCHD_ID VARCHAR(1000),
        ENRL_STAT_ID VARCHAR(1000),
        ENRL_DTE DATETIME,
        COMMENTS VARCHAR(16777216),
        SHIPPING_ADDR VARCHAR(1000),
        SHIPPING_CITY VARCHAR(1000),
        SHIPPING_STATE VARCHAR(1000),
        SHIPPING_POSTAL VARCHAR(1000),
        SHIPPING_CNTRY VARCHAR(1000),
        SHIPPING_PHON_NUM VARCHAR(1000),
        SHIPPING_FAX_NUM VARCHAR(1000),
        SHIPPING_EMAIL_ADDR VARCHAR(1000),
        ADD_USER VARCHAR(1000),
        CANCEL_DTE DATETIME,
        PMT_ORDER_TICKET_NO VARCHAR(1000),
        TICKET_SEQUENCE VARCHAR(1000),
        ORDER_ITEM_ID VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP,
        ORIGIN VARCHAR(1000),
        WITHDRAW_PENDING VARCHAR(1000),
        CANCELLATION_REASON_SYSGUID VARCHAR(1000),
        WAITLIST_NUMBER VARCHAR(1000) );
        
        CREATE OR REPLACE TABLE PS_SCHD_RESOURCES (
        SCHD_ID VARCHAR(1000),
        START_DTE DATETIME,
        START_TME DATETIME,
        END_DTE DATETIME,
        END_TME DATETIME,
        CLASS_HRS NUMBER(38,0),
        INST_ID VARCHAR(1000),
        LOCN_ID VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP );
        
        CREATE OR REPLACE TABLE PA_CATALOG_ITEM_SCHED_PRICE (
        CATALOG_ID VARCHAR(1000),
        SKU VARCHAR(1000),
        CURRENCY_CODE VARCHAR(1000),
        SCHD_ID VARCHAR(1000),
        PRICE NUMBER(38,6),
        DISCOUNT_PRICE NUMBER(38,6),
        IS_DEFAULT VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP );
        
        CREATE OR REPLACE TABLE PA_CATALOG_ITEM_SCHED (
        CATALOG_ID VARCHAR(1000),
        SKU VARCHAR(1000),
        SCHD_ID VARCHAR(1000),
        PRICE NUMBER(38,6),
        DISCOUNT_PRICE NUMBER(38,6),
        START_DATE DATETIME,
        FACILITY_ID VARCHAR(1000),
        CANCELLED VARCHAR(1000),
        CLOSED_DATE DATETIME,
        ENRL_CUT_DTE DATETIME,
        NOTACTIVE VARCHAR(1000),
        CPNT_TYP_ID VARCHAR(1000),
        CPNT_ID VARCHAR(1000),
        REV_DTE DATETIME,
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP );
        
        CREATE OR REPLACE TABLE PA_CATALOG (
        CATALOG_ID VARCHAR(1000),
        DMN_ID VARCHAR(1000),
        RULE_ID VARCHAR(1000),
        CATALOG_DESC VARCHAR(1000),
        ACTIVE VARCHAR(1000),
        DISCOUNT_APPLIED VARCHAR(1000),
        CONTACT_EMAIL_ADDR VARCHAR(1000),
        OFT_SEARCH_INDX_NOTIFIER VARCHAR(1000),
        LST_UPD_USR VARCHAR(1000),
        LST_UPD_TSTMP TIMESTAMP,
        USR_GENERATED_CNT VARCHAR(1000) );
        
        CREATE OR REPLACE TABLE PA_STUD_PROGRAM
        (
        STUD_PROGRAM_SYS_GUID                      VARCHAR(90)    ,
        PROGRAM_SYS_GUID                   VARCHAR(90)    ,
        STUD_ID                            VARCHAR(90)    ,
        CPNT_TYP_ID                                   VARCHAR(90)    ,
        CPNT_ID                            VARCHAR(90)    ,
        REV_DTE                           TIMESTAMP       ,
        SEQ_NUM                         NUMBER             ,
        PROGRAM_TYPE                             VARCHAR(300) ,
        PROGRAM_START_DATE              TIMESTAMP       ,
        PROGRAM_END_DATE                 TIMESTAMP       ,
        COMPL_DTE                                     TIMESTAMP       ,
        DURATION                        NUMBER             ,
        DURATION_TYPE                            VARCHAR(90)    ,
        TIMEZONE_ID                                  VARCHAR(300) ,
        LIVE_EDIT_TIME                              TIMESTAMP       ,
        LST_UPD_USR                                  VARCHAR(90)    ,
        LST_UPD_TSTMP                            TIMESTAMP
        );
        
        
        CREATE OR REPLACE TABLE PA_PROGRAM (
        PROGRAM_SYS_GUID                   VARCHAR(90)    ,
        PROGRAM_ID                                  VARCHAR(90)    ,
        PROGRAM_GROUP_SYS_GUID                   VARCHAR(90)    ,
        PROGRAM_TITLE                            VARCHAR(300) ,
        PROGRAM_DESC                            VARCHAR(300) ,
        CPNT_TYP_ID                                   VARCHAR(90)    ,
        CPNT_ID                            VARCHAR(90)    ,
        REV_DTE                           TIMESTAMP       ,
        CMPL_STAT_ID                                VARCHAR(90)    ,
        DMN_ID                            VARCHAR(90)    ,
        PROGRAM_TYPE                             VARCHAR(90)    ,
        ACTIVE                               VARCHAR(1)       ,
        START_DTE                       TIMESTAMP       ,
        END_DTE                           TIMESTAMP       ,
        DURATION                        NUMBER             ,
        DURATION_TYPE                            VARCHAR(90)    ,
        ADD_USER_TYPE                            VARCHAR(3)       ,
        ADD_USER_NAME                         VARCHAR(500) ,
        CAN_ACCESS_SEC_COMPL_PREVSEC       VARCHAR(1)       ,
        CAN_ACCESS_SEC_SCHEDULED_TIME     VARCHAR(1)       ,
        TIMEZONE_ID                                  VARCHAR(300) ,
        COVER_PAGE_ID                             VARCHAR(90)    ,
        LIVE_EDIT_TIME                              TIMESTAMP       ,
        LIVE_EDIT_BACKGROUND_JOB_ID            VARCHAR(90)    ,
        LST_UPD_USR                                  VARCHAR(90)    ,
        LST_UPD_TSTMP                            TIMESTAMP       ,
        OFT_SEARCH_INDX_NOTIFIER                    VARCHAR(90)
        
        );
        
        
        
        CREATE OR REPLACE TABLE          PA_CPNT_FORMULA_K AS           SELECT                 "CPNT_TYP_ID","CPNT_ID","REV_DTE","FIN_VAR_ID","CURRENCY_CODE"             FROM   PA_CPNT_FORMULA;
        CREATE OR REPLACE TABLE          PA_SCHED_FORMULA_K              AS           SELECT                 "SCHD_ID","FIN_VAR_ID","CURRENCY_CODE"    FROM   PA_SCHED_FORMULA;
        CREATE OR REPLACE TABLE          PA_SCHED_USER_K        AS           SELECT "COL_NUM","SCHD_ID" FROM                PA_SCHED_USER;
        CREATE OR REPLACE TABLE          PA_CURRENCY_K             AS           SELECT "CURRENCY_CODE"        FROM                PA_CURRENCY;
        CREATE OR REPLACE TABLE          PA_SCHED_K     AS           SELECT "SCHD_ID"          FROM   PA_SCHED;
        CREATE OR REPLACE TABLE          PS_SCHD_RESOURCES_K              AS           SELECT "SCHD_ID"          FROM                PS_SCHD_RESOURCES;
        CREATE OR REPLACE TABLE          PA_CATALOG_ITEM_SCHED_PRICE_K     AS           SELECT                 "CATALOG_ID","SKU","CURRENCY_CODE","SCHD_ID"     FROM   PA_CATALOG_ITEM_SCHED_PRICE;
        CREATE OR REPLACE TABLE          PA_CATALOG_ITEM_SCHED_K   AS           SELECT "CATALOG_ID","SKU","SCHD_ID"                FROM   PA_CATALOG_ITEM_SCHED;
        CREATE OR REPLACE TABLE          PA_CATALOG_K AS           SELECT "CATALOG_ID" FROM   PA_CATALOG;
        CREATE OR REPLACE TABLE          PA_ENROLL_SEAT_K      AS           SELECT "ENRL_SEAT_ID"              FROM                PA_ENROLL_SEAT;
        CREATE OR REPLACE TABLE     PA_PROGRAM_K AS SELECT "PROGRAM_SYS_GUID" FROM PA_PROGRAM;
        CREATE OR REPLACE TABLE     PA_STUD_PROGRAM_K AS SELECT "STUD_PROGRAM_SYS_GUID" FROM PA_STUD_PROGRAM;

    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
