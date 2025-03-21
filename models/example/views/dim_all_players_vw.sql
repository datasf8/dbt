select
    play_pk_play as player_key,
    play_id_play as id,
    play_code_play::int as code,
    play_label_play as label,
    play_status_play as status,
    case
        when label in ('Essential Player', 'Rising Player', 'Future Leader')
        then label
        else 'N/A'
    end as all_player_status,
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
from {{ ref("dim_all_players_snapshot") }}
