{{ config(alias='int_team_profiles_female') }}

with p as (select * from {{ ref('int_players_female') }}),
agg as (
  select
    club_id,
    club_name,
    league_id,
    league_name,
    count(*)                    as player_count,
    avg(overall)                as avg_overall,
    sum(coalesce(value_eur,0))  as total_value_eur
  from p
  group by club_id, club_name, league_id, league_name
)
select * from agg
