select
    ddll_linked_learning_sk_ddll as linked_learning_key,
    ddll_linked_learning_key_ddll as linked_learning_key_v1,
    ddll_linked_id_ddll as learning_activity_id
from {{ ref("dim_linked_learning") }}
