{{ config(alias='int_players_male') }}

with src as (
  select * from {{ ref('stg_male_players') }}
),
norm as (
  select
    -- ключи и основные поля
    player_id,
    short_name,
    long_name,
    nationality,
    age,
    date_of_birth,
    height_cm,
    weight_kg,

    -- стабильные идентификаторы по именам (без зависимости от наличия id-колонок)
    to_varchar(hash(lower(coalesce(club_name,'')),   lower(coalesce(league_name,'')))) as club_id,
    club_name,
    to_varchar(hash(lower(coalesce(league_name,''))))                                    as league_id,
    league_name,

    -- основы атрибутов
    player_positions,
    preferred_foot,

    overall,
    potential,
    pace, shooting, passing, dribbling, defending, physic,

    -- деньги (оставляем только value_eur как минимально нужное для агрегатов)
    value_eur,

    'male' as gender
  from src
),
dedup as (
  select *
  from norm
  qualify row_number() over (
    partition by player_id
    order by coalesce(overall,-1) desc, coalesce(date_of_birth, to_date('1900-01-01')) desc
  ) = 1
)
select * from dedup
