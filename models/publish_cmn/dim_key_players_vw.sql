select distinct
    all_player_status,
    collate(
        case
            when all_player_status = 'Essential Player'
            then 'All Players only'
            when all_player_status = 'Future Leader'
            then 'All Players only'
            when all_player_status = 'Rising Player'
            then 'All Players only'
            else 'Without All Players'
        end,
        'en-ci'
    ) as is_player
from {{ ref("dim_employee_profile_vw") }}
where all_player_status is not null
