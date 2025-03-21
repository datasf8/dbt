{% macro update_qck_tables() %}
    {% set query %}

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results 
alter column user_id VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results 
alter column COLUMN_NAME VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results 
alter column COLUMN_VALUE VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results_histo 
alter column user_id VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results_histo 
alter column COLUMN_NAME VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_qck_{{ env_var('DBT_REGION') }}_db.cmn_core_sch.fact_dataquality_results_histo 
alter column COLUMN_VALUE VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_pub_{{ env_var('DBT_REGION') }}_db.QCK_PUB_SCH.FACT_DATAQUALITY_RESULTS_HISTO 
alter column user_id VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_pub_{{ env_var('DBT_REGION') }}_db.QCK_PUB_SCH.FACT_DATAQUALITY_RESULTS_HISTO 
alter column COLUMN_NAME VARCHAR(1000) COLLATE 'en-ci';

alter table hrdp_pub_{{ env_var('DBT_REGION') }}_db.QCK_PUB_SCH.FACT_DATAQUALITY_RESULTS_HISTO 
alter column COLUMN_VALUE VARCHAR(1000) COLLATE 'en-ci';


    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
