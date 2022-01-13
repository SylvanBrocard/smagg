{{ config(materialized='table') }}

with stop_times as (
	select trip_id, stop_id, arrival_time
	from {{ref ('stop_times')}}
),

dim_stopgeo as (
	select stop_id::integer, stop_name, stop_lat, stop_lon
	from {{ref ('dim_stopgeo')}}
),

dim_trips as (
	select trip_id, route_id, direction_id
	from {{ref ('dim_trips')}}
),

dim_time as (
	select arrival_time, timelaps 
	from {{ref ('dim_time')}}
),

stop_times_rich as (
	select stop_times.trip_id, stop_times.stop_id, stop_name, stop_lat, stop_lon, stop_times.arrival_time, route_id, timelaps, direction_id
	from stop_times
		join dim_trips
			on stop_times.trip_id = dim_trips.trip_id
		join dim_time
			on stop_times.arrival_time = dim_time.arrival_time
		join dim_stopgeo
			on stop_times.stop_id = dim_stopgeo.stop_id 
	where route_id in ('A', 'B', 'C', 'D', 'E')
)

select * from stop_times_rich