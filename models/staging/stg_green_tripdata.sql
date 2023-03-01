{{ config(materialized='view') }}

select * from {{ source('staging', 'green_trips') }} limit 100
