select report_name, employee_id, current_user() employee_upn, field_name, visibility
from {{ ref("rel_cls_employee_dashboard") }}
where employee_upn in ('All', current_user())
