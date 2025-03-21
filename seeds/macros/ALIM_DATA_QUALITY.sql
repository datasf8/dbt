
{% macro ALIM_DATA_QUALITY() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;

CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_DATA_QUALITY("DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE 

    var_table_name_fact_dq_result VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''FACT_DATAQUALITY_RESULTS'') ; 
    var_table_name_fact_dq_result_tmp   VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''TMP_DQ_RESULT'') ; 
    var_table_name_fact_dq_result_histo    VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''FACT_DATAQUALITY_RESULTS_HISTO'') ; 
    var_table_name_log_dq_runs    VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'', ''LOG_DATAQUALITY_RUNS'') ; 
    
var_run_id integer DEFAULT -1;
var_check_log_dq_by_rule string;
var_check_dq_results string;
BEGIN
 
 CALL HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_DIM_ORGANIZATION( :DB, :SCH);
 CALL HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_DIM_PROFESSIONAL_FIELD( :DB, :SCH);

CALL HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_LOG_DATAQUALITY_RUNS( :DB, :SCH)  INTO :var_run_id;
 

  IF (:var_run_id = -1) THEN
    return ''A DQ process is already in progress'';
  END IF; 
 
  IF (:var_run_id > 0 ) THEN 



CALL HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_LOG_DATA_QUALITY_BY_RULE( 
      RUN_ID => :var_run_id
     ,DB => :DB
     ,SCH => :SCH
     ) INTO :var_check_log_dq_by_rule;

    
    IF (:var_check_log_dq_by_rule = ''OK'') THEN 
     CALL HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_FACT_DATAQUALITY_RESULTS( 
     RUN_ID => :var_run_id
     ,DB => :DB
     ,SCH => :SCH
     ) INTO :var_check_dq_results; 
     IF (:var_check_dq_results = ''OK'') THEN 
 
     INSERT INTO IDENTIFIER(:var_table_name_fact_dq_result_histo)
     SELECT * FROM   IDENTIFIER(:var_table_name_fact_dq_result_tmp);

     TRUNCATE TABLE  IDENTIFIER(:var_table_name_fact_dq_result) ;
     INSERT INTO IDENTIFIER(:var_table_name_fact_dq_result) 
     SELECT * FROM IDENTIFIER(:var_table_name_fact_dq_result_tmp); 
 
     
    --HIST
        UPDATE IDENTIFIER(:var_table_name_log_dq_runs)
        SET 
        END_DATE = current_timestamp
        ,STATUS = ''Success'' 
        ,MESSAGE = ''OK''
        WHERE RUN_ID = :var_run_id; 
     ELSE 
        UPDATE IDENTIFIER(:var_table_name_log_dq_runs)  
        SET 
        END_DATE = current_timestamp
        ,STATUS = ''Failed'' 
        ,MESSAGE = :var_check_dq_results
        WHERE RUN_ID = :var_run_id; 
     END IF;
    ELSE 
        UPDATE IDENTIFIER(:var_table_name_log_dq_runs)  
        SET 
        END_DATE = current_timestamp
        ,STATUS = ''Failed'' 
        ,MESSAGE = :var_check_log_dq_by_rule
        WHERE RUN_ID = :var_run_id; 
    END IF; 

    
  END IF; 
 
END';

        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 