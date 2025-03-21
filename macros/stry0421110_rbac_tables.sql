{% macro stry0421110_rbac_tables() %}

    {% set qPrerequisite %}
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        create database if not exists HRDP_ADM_{{ env_var('DBT_REGION') }}_DB;
        USE DATABASE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB;
        create schema if not exists DB_SCHEMA_SCH with managed access;
        USE SCHEMA DB_SCHEMA_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}
        create or replace table role_dar_mapping 
            (role_name varchar, database_name varchar, dar_name varchar, sp_name varchar
            , updated_by varchar default current_user, updated_at timestamp default current_timestamp);

        create or replace table schema_table 
            (database_name varchar, schema_name varchar, owner_role varchar
            , updated_by varchar default current_user, updated_at timestamp default current_timestamp);

        create or replace table dgrh_role_mapping
            (long_name varchar, short_name varchar);
        insert into dgrh_role_mapping
            values
            ('COMPENSATION','CMP'),
            ('LEARNING','LRN'),
            ('PMGM','PMG'),
            ('HEADCOUNT','CMN'),
            ('ACKNOLEDGEMENT','ACK'),
            ('SDDS_SELF_SERVICE','SDDS');

        create or replace table custom_role_mapping
            (role_name varchar, database_name varchar, schema_name varchar, privileges_level varchar(3)
            , updated_by varchar default current_user, updated_at timestamp default current_timestamp);
        insert overwrite into custom_role_mapping
            (role_name,database_name,schema_name,privileges_level)
            values 
            ('HRDP_SUPER_USER_ROLE','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','ALL','RO'),
            ('HRDP_DATA_QUALITY_ROLE','HRDP_DATAQUALITY_{{ env_var('DBT_REGION') }}_DB','ALL','RW');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
