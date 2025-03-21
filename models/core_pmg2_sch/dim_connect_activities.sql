{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    connect_activities as (
        select distinct
            activity_id as dcac_activity_id_dcac,
            activity_name as dcac_activity_name_dcac,
            activity_status_name as dcac_activity_status_dcac,
            activity_created_date as dcac_activity_creation_date_dcac,
            activity_modified_date as dcac_activity_last_modification_date_dcac
        from {{ ref("stg_perf_goal_report") }}

    ),
    surrogate_key as (
        select
            p.*,{{ dbt_utils.surrogate_key("DCAC_ACTIVITY_ID_DCAC") }} as sk_activity_id
        from connect_activities p

    )
select
    hash(dcac_activity_id_dcac) as dpcac_activity_sk_dpcac,
    sk_activity_id as dcac_activity_key_dcac,
    dcac_activity_id_dcac,
    dcac_activity_name_dcac,
    dcac_activity_status_dcac,
    dcac_activity_creation_date_dcac,
    dcac_activity_last_modification_date_dcac
from surrogate_key
