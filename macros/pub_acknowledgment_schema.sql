{% macro pub_acknowledgment_schema() %}
    {% set query %}
        -------------- Execute below statement using DOMAIN_ADMIN role ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        ----------------Create Databases------------------------------
        CREATE DATABASE IF NOT EXISTS HRDP_PUB_{{ env_var('DBT_REGION') }}_DB; 
        USE DATABASE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB;
        ----------------Create all Scehemas in the database-------------------------
        CREATE SCHEMA IF NOT EXISTS ACK_PUB_SCH WITH MANAGED ACCESS;
        ----------------Create Technical Roles for each schema------------------------------
        CREATE ROLE IF NOT EXISTS _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        CREATE ROLE IF NOT EXISTS _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        --------------------Grant Usage on database------------------
        GRANT USAGE ON DATABASE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON DATABASE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        --------------------Grant Usage on Schema---------------------
        GRANT USAGE ON SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT ALL PRIVILEGES ON SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        ----------------Grant Individual Tech Roles to Database Tech Roles--------------------------
        GRANT ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE TO ROLE _TECH_PUB_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE TO ROLE _TECH_PUB_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        -----------Granting Object privileges to roles---------
        -----------Grant Privileges to Technical Read only roles---------
        -----------Grants for current Object---------
        GRANT SELECT ON ALL TABLES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT SELECT ON ALL VIEWS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE, READ ON ALL STAGES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON ALL FILE FORMATS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT SELECT ON ALL STREAMS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        -----------Grants for Future Object---------
        GRANT SELECT ON FUTURE TABLES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT SELECT ON FUTURE VIEWS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE, READ ON FUTURE STAGES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT SELECT ON FUTURE STREAMS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_RO_ROLE;
        -----------Grant Privileges to Technical Ownership roles---------
        GRANT OWNERSHIP ON ALL TABLES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL STAGES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON ALL TASKS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        -----------Grants For Future Object---------
        GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE STAGES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE FILE FORMATS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE STREAMS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE SEQUENCES IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        GRANT OWNERSHIP ON FUTURE TASKS IN SCHEMA ACK_PUB_SCH TO ROLE _TECH_PUB_ACK_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        --------------------------------------------------------------------------------------------------
    {% endset %}
    
    {% do run_query(query) %}

{% endmacro %}