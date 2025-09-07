{{ config(
    alias='stg_male_coaches',
    schema='STG',
    materialized='view'
) }}

with src as (

    select *
    from {{ source('RAW', 'MALE_COACHES') }}

),

renamed as (

    select
        -- идентификатор
        {{ to_int('COACH_ID') }}          as coach_id,

        -- ссылки
        {{ clean_string('COACH_URL') }}   as coach_url,
        {{ clean_string('FACE_URL') }}    as face_url,

        -- имена
        {{ clean_string('SHORT_NAME') }}  as short_name,
        {{ clean_string('LONG_NAME') }}   as long_name,

        -- демография
        {{ to_date('DOB') }}              as date_of_birth,
        {{ to_int('NATIONALITY_ID') }}    as nationality_id,
        {{ clean_string('NATIONALITY_NAME') }} as nationality_name,

        -- техметка загрузки
        current_timestamp()               as _loaded_at

    from src
)

select * from renamed
