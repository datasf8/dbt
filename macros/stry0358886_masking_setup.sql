{% macro stry0358886_masking_setup() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}
        use database HRDP_LND_{{ env_var('DBT_REGION') }}_DB;
        -- Param Role Tags: This table will have Roles has access to specific Tags
        alter table CMN_LND_SCH.PARAM_ROLES_TAGS add column if not exists updated_at timestamp;
        alter table CMN_LND_SCH.PARAM_ROLES_TAGS add column if not exists updated_by varchar;

        delete from CMN_LND_SCH.PARAM_ROLES_TAGS where updated_by='STRY0358886';
        insert into CMN_LND_SCH.PARAM_ROLES_TAGS values('KEY_PLAYER','EX10 - HR CONTROLLER',current_timestamp(),'STRY0358886');

        -- Param Tag Columns: This table will have Column details with specific Tag and category
        alter table CMN_LND_SCH.PARAM_TAGS_COLUMNS add column if not exists updated_at timestamp;
        alter table CMN_LND_SCH.PARAM_TAGS_COLUMNS add column if not exists updated_by varchar;

        delete from CMN_LND_SCH.PARAM_TAGS_COLUMNS where updated_by='STRY0358886';
        insert into CMN_LND_SCH.PARAM_TAGS_COLUMNS values('COL','KEY_PLAYER','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','DIM_EMPLOYEE_V1','ALL_PLAYERS_STATUS_CODE','USER_ID','Masked',current_timestamp(),'STRY0358886');
        insert into CMN_LND_SCH.PARAM_TAGS_COLUMNS values('COL','KEY_PLAYER','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','DIM_EMPLOYEE_V1','ALL_PLAYERS_STATUS_NAME_EN','USER_ID','Masked',current_timestamp(),'STRY0358886');

        -- Below procedure and format of Procedure needs to included in regular load process post hook.
        --call cmn_core_sch.masking_policy_apply_all_sp('%','%');
        --call cmn_core_sch.masking_policy_apply_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','DIM_EMPLOYEE_V1');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
