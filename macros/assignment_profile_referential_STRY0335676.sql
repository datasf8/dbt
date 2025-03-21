{% macro assign_prfl_STRY0335676() %}
    {% set query %}

     USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        USE DATABASE HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB; 
        --------------------Create Schema---------------------
        USE SCHEMA BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE;

insert into DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL
  values 

('AP_JBMUST_BRAND_IDENTITY_TARGET'	,	'BRAND IDENTITY'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_BRIEFING_THAT_WORKS_TARGET'	,	'BRIEFING THAT WORKS'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_IMC_THAT_WORKS_TARGET'	,	'IMC THAT WORKS'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_PRODUCT_THAT_WORKS_TARGET'	,	'PRODUCT THAT WORKS'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_GENERATING_BREAKTHROUGH_CONCEPTS_TARGET'	,	'GENERATING BREAKTHROUGH CONCEPTS'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_LSA _O+O_CUSTOMER_ENGAGEMENT_1_TARGET'	,	'LSA - O+O Customer engagement - 1'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_LSA _TRUSTED_BUSINESS_PARTNER_1_TARGET'	,	'LSA - Trusted business partner - 1'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_LSA _TRUSTED_BUSINESS_PARTNER_2_TARGET'	,	'LSA - Trusted business partner - 2'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_LSA_STRATEGIC_EDUCATION_LEADER_TARGET'	,	'LSA – STRATEGIC EDUCATION LEADER'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_PERFECT_SELLING_KIT_TARGET'	,	'Perfect Selling Kit'	,	'MUST',	'JBMUST',	'Job Must'),
						
('AP_BUSINESSMUST_DATA4ALL_STEP_2_TARGET'	,	'Data4All – Step 2'	,	'MUST',	'BUSINESSMUST',	'Business Must'),
						
('AP_JBMUST_O+O_COMMERCE_TARGET'	,	'O+O Commerce'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_STRATEGIC_JOINT_BUSINESS_PLAN_TARGET'	,	'Strategic Joint Business Plan'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_RGM FOR COMMERCIAL MANAGERS_CPD_TARGET'	,	'RGM for Commercial Managers'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_FROM_SELLING_TO_NEGOTIATION_TARGET'	,	'From Selling to Negotiation'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_KAM&EKAM_LUXE_ADVANCED_TARGET'	,	'KAM & EKAM – Luxe - Advanced'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_HIGYENE_CULTURE_TARGET'	,	'Hygiene culture'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_ONBMUST_SOURCING_DISCOVERY_TARGET'	,	'Sourcing Discovery'	,	'MUST',	'ONBMUST',	'Onboarding Must'),
('AP_ONBMUST_SOURCINGSTRATEGYIMPLEMENTATION_TARGET'	,	'Sourcing Strategy & Implementation'	,	'MUST',	'ONBMUST',	'Onboarding Must'),
('AP_JBMUST_BUSINESS_AND_SUSTAINABILITY_MANAGEMENT_TARGET'	,	'Business & Sustainability Management'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_TA_ESSENTIALS_TARGET'	,	'TA Essentials'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_HRBP_ESSENTIALS_TARGET'	,	'HRBP Essentials'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_REWARDS_ESSENTIALS_TARGET'	,	'REWARDS Essentials'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_BUSINESSMUST_DIGITAL_SCENOGRAPHY_TARGET'	,	'Digital & Scenography'	,	'MUST',	'BUSINESSMUST',	'Business Must'),
('AP_JBMUST_RETAIL_DESIGN_EXCELLENCE_TARGET'	,	'Retail Design Excellence'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_SERVICE_EXCELLENCE_TARGET'	,	'Service Excellence'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_SRA_EMPATHY_LUXE_ADVANCED_TARGET'	,	'SRA Empathy Luxe Essential'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_SRA_RSM_TTT_LUXE_ADVANCED_TARGET'	,	'SRA RSM TTT Luxe Advanced'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_JBMUST_MARKETING_ESSENTIALS_WEEK_TARGET'	,	'Marketing Essentials week'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_ONBMUST_FINANCE_LEGAL_DISCOVERY_TARGET'	,	'F&L Discovery'	,	'MUST',	'ONBMUST',	'Onboarding Must'),
('AP_JBMUST_CONTROLLING_ESSENTIALS_TARGET'	,	'Controlling Essentials'	,	'MUST',	'JBMUST',	'Job Must'),
('AP_BUSINESSMUST_DIVERSITY_WORKSHOP_ESSENTIALS_TARGET'	,	'Diversity workshop essentials'	,	'MUST',	'BUSINESSMUST',	'Business Must'),
('AP_BUSINESSMUST_DIVERSITY_WORKSHOP_ADVANCED_TARGET'	,	'Diversity workshop advanced'	,	'MUST',	'BUSINESSMUST',	'Business Must'),
('AP_JBMUST_RIsC_Serious_Game_TARGET'	,	'RIsC Serious Game'	,	'MUST',	'JBMUST',	'Job Must');

update DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL
set ASSIGNMENT_PROFILE_NAME='Data4All – Step 1'
where ASSIGNMENT_PROFILE_ID='AP_BUSINESSMUST_DATA4ALL_TARGET';

       {% endset %}

    {% do run_query(query) %}

{% endmacro %}    