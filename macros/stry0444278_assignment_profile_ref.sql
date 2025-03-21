{% macro stry0444278_assignment_profile_ref() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        truncate table HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL;
        insert into HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DIM_PARAM_ASSIGNMENT_PROFILE_REFERENTIAL values
            (	'AP_BUSINESSMUST_COSMETICREGULATION_TARGET',	'COSMETIC REGULATION FOR R&I',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_DATA4ALL_TARGET',	'Data4All - Step 1',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_DIGITAL_SCENOGRAPHY_TARGET',	'Digital & Scenography',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_DIVERSITY_WS_ESSENTIALS_TARGET',	'Diversity workshop essentials',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_ENVIROMENTALSAFETY_TARGET',	'ENVIRONMENTAL SAFETY ASSESSMENT',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_FORMULATION_ECO_DESIGN_TARGET',	'Formulation eco design',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_GALASKILLSWS_TARGET',	'GalaSkills Workshop',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_GENAIFORALL_TARGET',	'GenAI For All',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_GST_TARGET',	'Going Sustainable Together',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_HCPOMNICHANEL_EXECUTE_TARGET',	'HCP omnichannel engagement - execute 1 monitor',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_HCPOMNICHANEL_PLANANDDESIGN_TARGET',	'HCP omnichannel engagement - PLAN & DESIGN',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_HCPOMNICHANEL_VISION_TARGET',	'HCP omnichannel engagement - the vision',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_HOWTOBUILD_RI_TARGET',	'How to build desirable and memorable experiences',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_HUMANSAFETYASSESSMENT_TARGET',	'Human safety assessment',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_MICROAGGRESSIONS_TARGET',	'Microaggressions',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_OUTSMART_YOUR_INTUITION_TARGET',	'Outsmart your intuition!',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_PPD_DIVERSION_TARGET',	'PPD Market Protection: The Ways We Fight Diversion',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_RI_PROJECTMANAGEMENT_TARGET',	'R&I PROJECT MANAGEMENT ESSENTIALS',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_SCIENTIFIC_ETHICS_INTEGRITY_TARGET',	'Scientific ethics & integrity',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_BUSINESSMUST_UNCONSCIOUSBIAS_TARGET',	'Unconsious Bias',	'MUST',	'BUSINESSMUST',	'Business Must'	),
            (	'AP_JBMUST_BRAND_IDENTITY_TARGET',	'Brand Identity',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_BRIEFING_THAT_WORKS_TARGET',	'Briefing That Works',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_BUSINESSSUSTAINABILITY_MGMT_TARGET',	'Business & Sustainability Management',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_COACHING_TOOLS_FOR_HR_TARGET',	'Coaching Tools for HR',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_COMMERCE_ESSENTIALS_TARGET',	'Commerce Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_CONTROLLING_ADVANCED_TARGET',	'Controlling Advanced',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_CONTROLLING_ESSENTIALS_TARGET',	'Controlling Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_LOCAL_MANDATORY_DIGITAL_APP_TARGET',	'Digital app',	'MANDT',	'LCMANDT',	'Local Mandatory'	),
            (	'AP_JBMUST_DOCTOR_CENTRICAPPROACH_COACHING_TARGET',	'Doctor centric approach coaching essential',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_DOCTOR_CENTRICAPPROACH_ESSENTIAL_TARGET',	'Doctor centric approach essential',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_DOCTOR_CENTRICAPPROACHCOACHINGTTT_TARGET',	'Doctor centric approach coaching essentials TTT',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_DOCTORCENTRICAPPROACHTTTESSENTIAL_TARGET',	'Doctor centric approach essentials - TTT',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_EHS_EXCELLENCE_ESSENTIALS_TARGET',	'EHS excellence essential',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_EXPNEGO_TARGET',	'Experienced negociator',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_FROM_SELLING_TO_NEGOTIATION_TARGET',	'From Selling to Negotiation',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_GENERATING_BREAKTHROUGH_CONCEPTS_TARGET',	'Generating Breakthrough Concepts',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_HCP_PRACTICE_EXPERTISE_TARGET',	'HCP practice & expertise',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_HCPOMNICHANNELENGAGEMENTTTT_TARGET',	'HCP Omnichannel Engagement TTT',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_HCPPRACTICEANDEXPERTISE_TARGET',	'HCP Practice & Expertise TTT',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_HRBP_ESSENTIALS_TARGET',	'HRBP Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_HYGIENE_CULTURE_TARGET',	'Hygiene culture',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_IMC_THAT_WORKS_TARGET',	'IMC That Works',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_LOCAL_MANDATORY_IP_RIGHTS_TARGET',	'IP Rights',	'MANDT',	'LCMANDT',	'Local Mandatory'	),
            (	'AP_JBMUST_KAM&EKAM_LUXE_ADVANCED_TARGET',	'KAM & EKAM â€“ Luxe - Advanced',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LEADERSHIP_AND_SAFETY_CULTURE_TARGET',	'Leadership & Safety Culture',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LEARNINGCONTENT_ESSENTIALS_TARGET',	'Learning Content Design Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LEARNINGCONTENTAWARENESS_TARGET',	'Learning Content Design Awareness',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA _OO_CUSTOMER_ENGAGEMENT_1_TARGET',	'LSA - O+O Customer engagement - 1',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_MANAGER_COACH_TARGET',	'LSA - manager coach (2)',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_OO_CUSTOMER_ENGAGEMENT_1_TARGET',	'LSA - O+O Customer engagement - 1',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_OO_CUSTOMER_ENGAGEMENT_2_TARGET',	'LSA - O+O Customer engagement - 2',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_STRATEGIC_EDUCATION_LEADER_TARGET',	'LSA â€“ Strategic Education Leader',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_TRAININGFORIMPACT_TARGET',	'LSA - TRAINING FOR IMPACT',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_TRUSTED_BUSINESS_PARTNER_1_TARGET',	'LSA - Trusted business partner - 1',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LSA_TRUSTED_BUSINESS_PARTNER_2_TARGET',	'LSA - Trusted business partner - 2',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_CANDC_TARGET',	'License to Operate: C&C',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_CONSUMER_CARE_TARGET',	'License to Operate: Consumer Care',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_DCRN_TARGET',	'License to Operate: DCRM',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_MEDIA_TARGET',	'License to Operate: Media',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_OO_DATA_TARGET',	'License to Operate: O+O Data',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_LTO_SERVICE_TARGET',	'License to Operate: Service',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_MARKETING_ESSENTIALS_WEEK_TARGET',	'Marketing Essentials week',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_MEDICAL_VISIT_METHODOLOGY_TARGET',	'Medical visit methodology',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_MINERAL_KPI7_TARGET',	'Mineral abudant KPI7',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_OO_COMMERCE_TARGET',	'O+O Commerce',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_OPERATIONS_LABS_ESSENTIALS_TARGET',	'EHS operations & labs essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_PERFECT_SELLING_KIT_TARGET',	'Perfect Selling Kit',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_PRIVACYBYDESIGN_RI_TARGET',	'Privacy by design - clinical studies',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_PRODQUALMINDSETSKILLS_TARGET',	'Production Quality: Mindset & Skills',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_PRODUCT_THAT_WORKS_TARGET',	'Product That Works',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RETAIL_DESIGN_EXCELLENCE_TARGET',	'Retail Design Excellence',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RETAILFUNDAMENTALS_TARGET',	'Retail Fundamentals',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_REWARDS_ESSENTIALS_TARGET',	'REWARDS Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RGM_FOR_COMMERCIAL_MANAGERS_CPD_TARGET',	'RGM for Commercial Managers',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RGM_FUNDAMENTALS_TARGET',	'RGM Fundamentals',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RISC_EXPERT_TRAINING_TARGET',	'RIsC Expert Training',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RISC_SERIOUS_GAME_TARGET',	'RIsC Serious Game',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_RSM_TARGET',	'SRA - RSM (Retail Store Management) Core Learning - Luxe - Advanced',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SCENOGRAPHY_TARGET',	'Scenography',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SELLING_TECHNIQUES_ESSENTIAL_TARGET',	'Selling techniques essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SERVICE_EXCELLENCE_TARGET',	'Service Excellence',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SPOT_SIMULATION_TOOL_TUTORIAL_TARGET',	'SPOT & SIMULATION TOOL TUTORIAL',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SQCF_ESSENTIALS_TARGET',	'SQCF essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_SRA_EMPATHY_LUXE_ADVANCED_TARGET',	'SRA Empathy Luxe Essential',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_STRATEGIC_JOINT_BUSINESS_PLAN_TARGET',	'Strategic Joint Business Plan',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_TA_ESSENTIALS_TARGET',	'TA Essentials',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_JBMUST_THEWAYWEWORKWITHOURSUPPLIERS_TARGET',	'The Way we Work With Our Suppliers',	'MUST',	'JBMUST',	'Job Must'	),
            (	'AP_LEADMUST_TTM_TARGET',	'TTM',	'MUST',	'LEADMUST',	'Leadership Must'	),
            (	'AP_LOCAL_MANDATORY_INTERNALCONTROL_TARGET',	'Internal Control',	'MANDT',	'LCMANDT',	'Local Mandatory'	),
            (	'AP_MANDATORY_COMPETITIONLAW_TARGET',	'Your antitrust passport - competitive law',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_MANDATORY_CORRUPTION_TARGET',	'The way we prevent corruption',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_MANDATORY_DATAPRIVACY_TARGET',	'Data privacy for all',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_MANDATORY_ETHICS&HUMANRIGHTS_TARGET',	'Ethics & human rights',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_MANDATORY_ISECURE_TARGET',	'I-Secure',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_MANDATORY_JOINTHENEXTSHIELD_TARGET',	'Join the next shield',	'MANDT',	'GBMANDT',	'Global Mandatory'	),
            (	'AP_ONBMUST_CANDC_LICENSETOOPERATE_TARGET',	'C&C License to Operate',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_FINANCE_LEGAL_DISCOVERY_TARGET',	'F&L Discovery',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_FORMULATION_ECO_DESIGN_TARGET',	'Formulation Eco Design',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_HOWTOBUILD_DESIRABLE_ESSENTIALS_TARGET',	'How To Build Desirable And Memorable Experiences',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_LOREAL_DISCOVERY_TARGET',	'Lâ€™Oreal Discovery',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_LSA_SOCIALPRO1_TARGET',	'LSA social pro 1',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_LSA_SOCIALPRO2_TARGET',	'LSA social pro 2',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_LSA_SOCIALPRO3_TARGET',	'LSA social pro 3',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_OUTSMART_YOUR_INTUITION_TARGET',	'Outsmart Your Intuition',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_PRODUCTDEV_LICENSETOOPERATE_TARGET',	'Product Development License to Operate',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_SOURCING_DISCOVERY_TARGET',	'Sourcing Discovery',	'MUST',	'ONBMUST',	'Onboarding Must'	),
            (	'AP_ONBMUST_SOURCINGSTRATEGYIMPLEMENTATION_TARGET',	'Sourcing Strategy & Implementation',	'MUST',	'ONBMUST',	'Onboarding Must'	),
			(   'AP_ONBMUST_GO2030OPERATIONS_TARGET' , 'GO 2030: Growing with Operations' , 'MUST' , 'ONBMUST' , 'Onboarding Must' ),
			(   'AP_JBMUST_DIGITALPRODUCT_MANAGERDISCOVERY_TARGET' , 'DIGITAL PRODUCT MANAGER DISCOVERY - ADVANCED' , 'MUST' , 'JBMUST' ,'Job Must'),
			(   'AP_JBMUST_DIGITALPRODUCT_OWNERDELIVERY_TARGET' , 'DIGITAL PRODUCT OWNER DELIVERY - ADVANCED' , 'MUST' , 'JBMUST' ,'Job Must'),
			(   'AP_LOCAL_MANDATORY_INDIRECTDIGITAL_TARGET' ,'Indirect E-commerce' , 'MANDT' , 'LCMANDT' , 'Local Mandatory'),
			(   'AP_ONBMUST_DIGITALDISCOVERY_TARGET' , 'Digital Discovery' , 'MUST',	'ONBMUST',	'Onboarding Must'	),
			(   'AP_JBMUST_BUSINESSPLANESSNTIALS_TARGET' , 'Business planning essentials ' , 'MUST',	'JBMUST',	'Job Must'	),
			(   'AP_JBMUST_METIERS_TARGET' , 'Métiers' , 'MUST',	'JBMUST',	'Job Must'	),
			(   'AP_LEADMUST_THF_TARGET' , 'THF' , 'MUST' , 'LEADMUST' , 'Leadership Must'),
			(   'AP_BUSINESSMUST_DATA4ALL_STEP_2_TARGET' , 'Data4All - Step 2' , 'MUST' , 'BUSINESSMUST' , 'Business Must' ),
			(   'AP_ONBMUST_LUXECULTURE_FUNDAMENTALS_TARGET' ,'Luxe Culture Fundamentals' ,'MUST',	'ONBMUST',	'Onboarding Must' ),
			(   'AP_BUSINESSMUST_RGMLUXEESSENTIALS_TARGET' , 'RGM - luxe essentials' , 'MUST' , 'BUSINESSMUST' , 'Business Must' )

        ;

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
