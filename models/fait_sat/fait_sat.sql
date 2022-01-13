{{ config(materialized='table') }}

with stop_times_rich as (
	select * from {{ ref('stop_times_rich') }}
),

source_updown as (
	select
		cluster_name, 
		passengers_count,
		up_down,
		timelaps,
		upper(split_part(Line, '-', 2)) route_id,
		case line_direction
			when 'S1' then 0
			when 'S2' then 1
		end direction_id
	from {{ ref('updown_per_cluster_and_semline') }}
	where up_down = 'up'
		and split_part(Line, '-', 1) = 'tram'
		and line_direction <> 'ALL'
),

final_table as (
	select stop_id, stop_times_rich.trip_id, source_updown.passengers_count, stop_times_rich.arrival_time
	from stop_times_rich
	join source_updown
	on stop_times_rich.timelaps = source_updown.timelaps
		and stop_times_rich.route_id = source_updown.route_id
		and stop_times_rich.direction_id = source_updown.direction_id
)

select *
from final_table