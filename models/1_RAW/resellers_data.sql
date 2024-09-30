{{ config(
    materialized = "table",
    schema='1_RAW'
) }}
-- Use the `ref` function to select from other models

select
    *
from {{ source('cloud_storage', 'resellers_data_EXT') }}
