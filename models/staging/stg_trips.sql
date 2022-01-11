with source as (
    select * from {{ ref('trips') }}   
)

select * from source