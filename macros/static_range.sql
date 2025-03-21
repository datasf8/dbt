{% macro static_ran() %}
    {% set query %}

     USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB; 
        --------------------Create Schema---------------------
        USE SCHEMA BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE; 




     CREATE OR REPLACE TABLE dim_param_ranges
 (
 range_type			String	,
range_start			Decimal	,
range_end			Decimal	,
include_start_flag			Int	,
include_end_flag			Int	,
range_name			String	
 );
 
 insert	into dim_param_ranges	values	
('GRPSEN',0,6,1,0,'< 6 Months'),
('GRPSEN',6,12,1,0,'6 to 12 Months'),
('GRPSEN',12,36,1,0,'1 to 3 Years'),
('GRPSEN',36,96,1,0,'3 to 8 Years'),
('GRPSEN',96,180,1,0,'8 to 15 Years'),
('GRPSEN',180,999999,1,0,'> 15 Years'),
('JOBSEN',0,6,1,0,'< 6 Months'),
('JOBSEN',6,12,1,0,'6 to 12 Months'),
('JOBSEN',12,36,1,0,'1 to 3 Years'),
('JOBSEN',36,96,1,0,'3 to 8 Years'),
('JOBSEN',96,180,1,0,'8 to 15 Years'),
('JOBSEN',180,999999,1,0,'> 15 Years'),
('AGESEN',0,20,1,0,'< 20'),
('AGESEN',20,25,1,0,'20 - 25'),
('AGESEN',25,35,1,0,'25 - 35'),
('AGESEN',35,45,1,0,'35 - 45'),
('AGESEN',45,55,1,0,'45 - 55'),
('AGESEN',55,60,1,0,'55 - 60'),
('AGESEN',60,65,1,0,'60 - 65'),
('AGESEN',65,9999999,1,0,'> 60');

        {% endset %}

    {% do run_query(query) %}

{% endmacro %}    