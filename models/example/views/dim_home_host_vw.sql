select

    hoho_pk_hoho as home_host_key,
    hoho_id_hoho as id,
    hoho_code_hoho as code,
    hoho_label_hoho as home_host,
    hoho_status_hoho as status
from {{ ref("dim_home_host_snapshot") }}
