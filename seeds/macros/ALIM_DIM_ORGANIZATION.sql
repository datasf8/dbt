{% macro ALIM_DIM_ORGANISATION() %}
    {% set query %}
use database hrdp_qck_{{ env_var('DBT_REGION') }}_db;

use schema cmn_core_sch;
CREATE OR REPLACE PROCEDURE HRDP_QCK_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.ALIM_DIM_ORGANIZATION("DB" VARCHAR(16777216), "SCH" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE SQL
COMMENT='user-defined procedure'
EXECUTE AS CALLER
AS 'DECLARE 
    var_table_name VARCHAR DEFAULT  CONCAT (:DB,''.'',:SCH, ''.'',''DIM_ORGANIZATION'') ;   
BEGIN
 
CREATE OR REPLACE TABLE IDENTIFIER(:var_table_name) AS 
with BU as (
with
    business_unit_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.business_unit_v1
        where business_unit_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_code order by business_unit_start_date desc
            )
            = 1
    ),
    business_unit_type_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.business_unit_type_v1
        where business_unit_type_start_date <= current_date()
        qualify
            row_number() over (
                partition by business_unit_type_code
                order by business_unit_type_start_date desc
            )
            = 1
    ),
    company_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.company_v1
        where company_start_date <= current_date()
        qualify
            row_number() over (
                partition by company_code order by company_start_date desc
            )
            = 1
    ),
    country_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.country_v1
        where country_start_date <= current_date()
        qualify
            row_number() over (
                partition by country_code order by country_start_date desc
            )
            = 1
    ),
    geographic_zone_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.geographic_zone_v1
        where geographic_zone_start_date <= current_date()
        qualify
            row_number() over (
                partition by geographic_zone_code
                order by geographic_zone_start_date desc
            )
            = 1
    ),
    area_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.area_v1
        where area_start_date <= current_date()
        qualify
            row_number() over (partition by area_code order by area_start_date desc) = 1
    ),
    hr_division_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.hr_division_v1
        where hr_division_start_date <= current_date()
        qualify
            row_number() over (
                partition by hr_division_code order by hr_division_start_date desc
            )
            = 1
    ),
    hub_v1 as (
        select *
        from HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H01_ORGANIZATION_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.hub_v1
        where hub_start_date <= current_date()
        qualify
            row_number() over (partition by hub_code order by hub_start_date desc) = 1
    ), bu_cte as (
select distinct 
    business_unit_code ,
    business_unit_name_en || '' ('' || business_unit_code || '')'' as business_unit_name,
    company_code,
    company_name_en || '' ('' || company_code || '')'' as company_name,
    country_code,
    country_name_en as country_name,
    geographic_zone_code,
    geographic_zone_name_en as geographic_zone_name,
    hr_division_code,
    hr_division_name_en || '' ('' || hr_division_code || '')'' as hr_division_name
from 
business_unit_v1
left join business_unit_type_v1 using (business_unit_type_code)
left join company_v1 using (company_code)
left join country_v1 using (country_code)
left join geographic_zone_v1 using (geographic_zone_code)
left join area_v1 using (area_code)
left join hr_division_v1 using (hr_division_code)
left join hub_v1 on business_unit_v1.hub_id = hub_v1.hub_code)
select * from bu_cte
WHERE BUSINESS_UNIT_CODE IS NOT NULL 
AND (case when (COUNTRY_NAME = ''Canada'' and GEOGRAPHIC_ZONE_CODE=''NA'') 
            OR (COUNTRY_NAME = ''Switzerland'' and GEOGRAPHIC_ZONE_CODE=''AL'') THEN 0 else 1 end)=1
UNION 
SELECT ''ALL'',''ALL'',''ALL'',''ALL'',''ALL'',''ALL'',''ALL'',''ALL'',''ALL'',''ALL'' 

)
select ROW_NUMBER() OVER (ORDER BY BUSINESS_UNIT_CODE ) AS BUSINESS_UNIT_PK, * from BU
union select -99, ''not defined'',''not defined'',''not defined'',''not defined'',''not defined'',''not defined'',''not defined'',''not defined'',''not defined'',''not defined'' ;
return 1 ;
END';
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
