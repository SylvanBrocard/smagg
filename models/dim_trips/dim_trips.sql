{{ config(materialized='table') }}
with source_trips as (

    select trip_id, route_id from {{ ref('trips') }}

),

source_places as (
    select * from {{ ref('places') }}
)

select source_trips.trip_id, source_trips.route_id, source_places.nb_places
from source_trips
inner join source_places on source_trips.route_id = source_places.route_id