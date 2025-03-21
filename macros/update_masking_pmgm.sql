{% macro update_masking_pmgm() %}
  {% set query %}
    USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
    USE DATABASE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
    USE SCHEMA CMN_CORE_SCH;
    ALTER MASKING POLICY CMN_CORE_SCH.PMGM_RATING_COL_MASKING  SET BODY    ->  case  when exists (
                                    select  1 from rel_sf_group_rls_user a
					                join  REL_GROUP_MASKING_USER_PMG b on a.GRP_ID = b.reeu_group_id_reem
                                    where         employee_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        )
                                    and REEU_EMPLOYEE_ID_DDEP=val2)
                         then '*****'
                    else val
                end;
  {% endset %}
  {% do run_query(query) %}
{% endmacro %}