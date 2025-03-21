{% macro stry0342443_assignment_profile_ref() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        truncate table HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL;
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL values
            ('AP_BUSINESSMUST_DATA4ALL_STEP_2_TARGET','Data4All – Step 2','MUST','BUSINESSMUST','Business Must')
            ,('AP_BUSINESSMUST_DATA4ALL_TARGET','Data4All – Step 1','MUST','BUSINESSMUST','Business Must')
            ,('AP_BUSINESSMUST_DIGITAL_SCENOGRAPHY_TARGET','Digital & Scenography','MUST','BUSINESSMUST','Business Must')
            ,('AP_BUSINESSMUST_DIVERSITY_WORKSHOP_ADVANCED_TARGET','Diversity workshop advanced','MUST','BUSINESSMUST','Business Must')
            ,('AP_BUSINESSMUST_DIVERSITY_WS_ESSENTIALS_TARGET','Diversity workshop essentials','MUST','BUSINESSMUST','Business Must')
            ,('AP_JBMUST_BRAND_IDENTITY_TARGET','Brand Identity','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_BRIEFING_THAT_WORKS_TARGET','Briefing That Works','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_BUSINESSSUSTAINABILITY_MGMT_TARGET','Business & Sustainability Management','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_COMMERCE_ESSENTIALS_TARGET','Commerce Essentials','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_CONTROLLING_ADVANCED_TARGET','Controlling Advanced','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_CONTROLLING_ESSENTIALS_TARGET','Controlling Essentials','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_FROM_SELLING_TO_NEGOTIATION_TARGET','From Selling to Negotiation','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_GENERATING_BREAKTHROUGH_CONCEPTS_TARGET','Generating Breakthrough Concepts','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_HRBP_ESSENTIALS_TARGET','HRBP Essentials','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_HYGIENE_CULTURE_TARGET','Hygiene culture','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_IMC_THAT_WORKS_TARGET','IMC That Works','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_KAM&EKAM_LUXE_ADVANCED_TARGET','KAM & EKAM – Luxe - Advanced','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_LSA _OO_CUSTOMER_ENGAGEMENT_1_TARGET','LSA - O+O Customer engagement - 1','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_LSA_STRATEGIC_EDUCATION_LEADER_TARGET','LSA – Strategic Education Leader','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_LSA_TRUSTED_BUSINESS_PARTNER_1_TARGET','LSA - Trusted business partner - 1','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_LSA_TRUSTED_BUSINESS_PARTNER_2_TARGET','LSA - Trusted business partner - 2','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_MARKETING_ESSENTIALS_WEEK_TARGET','Marketing Essentials week','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_OO_COMMERCE_TARGET','O+O Commerce','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_PERFECT_SELLING_KIT_TARGET','Perfect Selling Kit','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_PRODUCT_THAT_WORKS_TARGET','Product That Works','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_RETAIL_DESIGN_EXCELLENCE_TARGET','Retail Design Excellence','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_REWARDS_ESSENTIALS_TARGET','REWARDS Essentials','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_RGM_FOR_COMMERCIAL_MANAGERS_CPD_TARGET','RGM for Commercial Managers','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_RISC_EXPERT_TRAINING_TARGET','RIsC Expert Training','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_RISC_SERIOUS_GAME_TARGET','RIsC Serious Game','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_SCENOGRAPHY_TARGET','Scenography','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_SERVICE_EXCELLENCE_TARGET','Service Excellence','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_SRA_EMPATHY_LUXE_ADVANCED_TARGET','SRA Empathy Luxe Essential','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_SRA_RSM_TTT_LUXE_ADVANCED_TARGET','SRA RSM TTT Luxe Advanced','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_STRATEGIC_JOINT_BUSINESS_PLAN_TARGET','Strategic Joint Business Plan','MUST','JBMUST','Job Must')
            ,('AP_JBMUST_TA_ESSENTIALS_TARGET','TA Essentials','MUST','JBMUST','Job Must')
            ,('AP_LEADMUST_TTM_TARGET','TTM','MUST','LEADMUST','Leadership Must')
            ,('AP_MANDATORY_COMPETITIONLAW_TARGET','Your antitrust passport - competitive law','MANDT','MANDT','MANDT')
            ,('AP_MANDATORY_CORRUPTION_TARGET','The way we prevent corruption','MANDT','MANDT','MANDT')
            ,('AP_MANDATORY_DATAPRIVACY_TARGET','Data privacy for all','MANDT','MANDT','MANDT')
            ,('AP_MANDATORY_ETHICS&HUMANRIGHTS_TARGET','Ethics & human rights','MANDT','MANDT','MANDT')
            ,('AP_MANDATORY_ISECURE_TARGET','I-Secure','MANDT','MANDT','MANDT')
            ,('AP_MANDATORY_JOINTHENEXTSHIELD_TARGET','Join the next shield','MANDT','MANDT','MANDT')
            ,('AP_ONBMUST_FINANCE_LEGAL_DISCOVERY_TARGET','F&L Discovery','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_FORMULATION_ECO_DESIGN_TARGET','Formulation Eco Design','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_HOWTOBUILD_DESIRABLE_ESSENTIALS_TARGET','How To Build Desirable And Memorable Experiences','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_L’OREAL_DISCOVERY_TARGET','L’Oreal Discovery','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_OUTSMART_YOUR_INTUITION_TARGET','Outsmart Your Intuition','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_SOURCING_DISCOVERY_TARGET','Sourcing Discovery','MUST','ONBMUST','Onboarding Must')
            ,('AP_ONBMUST_SOURCINGSTRATEGYIMPLEMENTATION_TARGET','Sourcing Strategy & Implementation','MUST','ONBMUST','Onboarding Must')
        ;

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}