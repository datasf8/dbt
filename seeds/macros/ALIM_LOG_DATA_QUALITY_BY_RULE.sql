
{% macro ALIM_LOG_DATA_QUALITY_BY_RULE() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_LOG_DATA_QUALITY_BY_RULE("RUN_ID" NUMBER(38,0), "DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE

    var_table_name_param VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''DIM_PARAM_DATAQUALITY_QUERIES'') ; 
    var_table_name_log   VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''LOG_DATA_QUALITY_BY_RULE'') ; 
    var_table_name_bu    VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''DIM_ORGANIZATION'') ; 
    var_table_name_pf    VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''DIM_PROFESSIONAL_FIELD'') ; 


    var_result_set_query_dq RESULTSET DEFAULT (
    SELECT  
      RULE_ID
    , RLS AS SUBJECT_DOMAIN 
    , CONCAT(''SELECT BU_CODE_TECH, PF_CODE_TECH, 
 
    SUM (IFF (DQ_FLAG_TECH = 1, 1,0)) AS TOTAL_DQ_ROWS_COUNT
    , COUNT (1) AS TOTAL_ROWS_COUNT  FROM
    ( '',char(13)
    ,FORMULA_CODE_SQL,char(13),
    '' ) GROUP BY  BU_CODE_TECH, PF_CODE_TECH'') AS CODE_SQL_RULE
 
    FROM   
     IDENTIFIER(:var_table_name_param)

     ORDER BY 1  

    
    );
    var_cursor_run_id CURSOR FOR var_result_set_query_dq;

 
  var_rule_id INT; 
  var_subject_domain VARCHAR; 
  var_sql_rule VARCHAR; 
  --var_sql_failure_rule VARCHAR; 
  
  var_count_sql_rule INT; 
  var_count_sql_failure_rule INT; 
  --var_pourcentage NUMBER (5,2);   

  var_log_status String;    
  var_log_message String;   
 
  var_result_set_count_sql_rule RESULTSET;
  --var_result_set_sql_failure_rule RESULTSET;   
 

    
BEGIN
 
  OPEN var_cursor_run_id;
  LOOP 
    FETCH var_cursor_run_id INTO var_rule_id,var_subject_domain, var_sql_rule;
     IF(var_sql_rule <> '''') THEN

  var_result_set_count_sql_rule := (execute immediate var_sql_rule) ;
 
  
INSERT INTO IDENTIFIER(:var_table_name_log)
       (RUN_ID
       ,RULE_ID
       ,SUBJECT_DOMAIN
       ,BUSINESS_UNIT_CODE
       ,PROFESSIONAL_FIELD_CODE
       ,BUSINESS_UNIT_PK
       ,PROFESSIONAL_FIELD_PK
       ,ERROR_COUNT
       ,TOTAL_COUNT) --, POURCENTAGE
       SELECT 
       :RUN_ID 
       ,:var_rule_id
       ,:var_subject_domain
       , BU_CODE_TECH
       ,PF_CODE_TECH 
       ,IFF (ORG.BUSINESS_UNIT_PK IS NULL, -99,ORG.BUSINESS_UNIT_PK ) AS BUSINESS_UNIT_PK
       ,IFF (PF.PROFESSIONAL_FIELD_PK IS NULL, -99,PF.PROFESSIONAL_FIELD_PK ) AS PROFESSIONAL_FIELD_PK
       ,TOTAL_DQ_ROWS_COUNT
       ,TOTAL_ROWS_COUNT 
       FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
       LEFT OUTER JOIN IDENTIFIER(:var_table_name_pf) PF ON PF_CODE_TECH = PF.PROFESSIONAL_FIELD_CODE
       LEFT OUTER JOIN IDENTIFIER(:var_table_name_bu) ORG ON BU_CODE_TECH = ORG.BUSINESS_UNIT_CODE;      
 
     var_log_status := ''Success'' ; 
     var_log_message := ''Completed'' ;
 
 ELSE
   BREAK;
 END IF;
 END LOOP;
 CLOSE var_cursor_run_id;
  RETURN ''OK'';
     EXCEPTION
        WHEN statement_error THEN 
    RETURN OBJECT_CONSTRUCT(''Error type'', ''STATEMENT_ERROR'',
                            ''SQLCODE'', sqlcode,
                            ''SQLERRM'', sqlerrm,
                            ''SQLSTATE'', sqlstate);
        WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(''Error type'', ''Other error'',
                            ''SQLCODE'', sqlcode,
                            ''SQLERRM'', sqlerrm,
                            ''SQLSTATE'', sqlstate);  

END';
        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 