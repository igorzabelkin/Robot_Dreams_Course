

{{ config(materialized='table') }}

select *
from {{ source('tpch_sf100', 'customer')}}