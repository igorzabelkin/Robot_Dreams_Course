-- FAIL, если есть дубли player_id между male и female
with all_players as (
  select player_id from {{ ref('int_players_male') }}
  union all
  select player_id from {{ ref('int_players_female') }}
),
dups as (
  select player_id, count(*) as cnt
  from all_players
  group by player_id
  having count(*) > 1
)
select * from dups
