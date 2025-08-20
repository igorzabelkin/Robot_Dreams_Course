{{ config(alias='stg_first_model', schema='RAW_STG', materialized='view') }}

with src as (
    select * from {{ source('RAW', 'FIRST_MODEL') }}
),
renamed as (
    select
        CAST(C_CUSTKEY   AS NUMBER(38,0)) as cust_key,      -- целое
        NULLIF(TRIM(C_NAME),      '')     as name,
        NULLIF(TRIM(C_ADDRESS),   '')     as address,
        CAST(C_NATIONKEY AS NUMBER(38,0)) as nation_key,    -- целое
        NULLIF(TRIM(C_PHONE),     '')     as phone,

        -- баланс уже NUMBER(12,2) → можно оставить как есть или привести масштабом
        CAST(C_ACCTBAL   AS NUMBER(38,2)) as acct_bal,

        NULLIF(TRIM(C_MKTSEGMENT), '')    as mkt_segment,
        NULLIF(TRIM(C_COMMENT),   '')     as comment,
        current_timestamp()               as _loaded_at
    from src
)
select * from renamed
