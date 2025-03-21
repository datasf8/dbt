select
    dyer_pk_dyer as key,
    dyer_code_dyer::number(18, 0) as code,
    initcap(dyer_label_dyer) as label

from {{ ref("dim_ratings_snapshot") }}
