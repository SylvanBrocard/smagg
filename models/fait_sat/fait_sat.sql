with stop_times as (
	select trip_id, stop_id, arrival_time
	from {{ ref('stop_times') }}
),

with stops as (
	select stop_id, stop_name, stop_lat, stop_lon
	from {{ ref('dim_stops_geo') }}
)

with trip as (
	select trip_id, route_id
	from {{ ref('dim_trip') }}
)