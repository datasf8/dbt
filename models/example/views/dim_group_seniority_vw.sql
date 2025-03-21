select

    dgrs_pk_dgrs as group_seniority_key,
    dgrs_range_code_dgrs as range_code,
    dgrs_range_dgrs as range,
    dgrs_in_nc_dgrs as in_nc

from {{ ref("dim_group_seniority_snapshot") }}
