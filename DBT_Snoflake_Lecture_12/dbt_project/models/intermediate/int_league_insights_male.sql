{{ config(alias='int_league_insights_male') }}

with players as (select * from {{ ref('int_players_male') }}),
teams   as (select * from {{ ref('int_team_profiles_male') }}),

player_agg as (
  select
    league_id,
    league_name,
    avg(overall)                                    as avg_player_overall,
    count(*)                                        as player_count,
    max(iff(age <= 23 and potential > 85, 1, 0)) = 1 as has_young_talents
  from players
  group by league_id, league_name
),
team_agg as (
  select
    league_id,
    league_name,
    avg(avg_overall)                                as avg_team_overall,
    sum(total_value_eur)                            as league_total_value_eur,
    count(*)                                        as team_count
  from teams
  group by league_id, league_name
)
select
  coalesce(p.league_id, t.league_id)      as league_id,
  coalesce(p.league_name, t.league_name)  as league_name,
  p.avg_player_overall,
  t.avg_team_overall,
  p.player_count,
  t.team_count,
  t.league_total_value_eur,
  p.has_young_talents
from player_agg p
full outer join team_agg t
  on p.league_id = t.league_id and p.league_name = t.league_name
