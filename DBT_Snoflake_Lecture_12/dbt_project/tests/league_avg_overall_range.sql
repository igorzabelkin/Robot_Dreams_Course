-- FAIL, если где-то за пределами диапазона
select *
from {{ ref('int_league_insights_male') }}
where not (avg_player_overall between 40 and 100)

union all
select *
from {{ ref('int_league_insights_female') }}
where not (avg_player_overall between 40 and 100)
