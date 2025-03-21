select
    gend_pk_gend as gender_key,
    gend_code_gend as gender_code,
    gend_label_gend as gender_label,
    gend_gender_category_gend as gender_category,
    gend_gender_sort_gend as gender_sort

from {{ ref("dim_gender_snapshot") }}
