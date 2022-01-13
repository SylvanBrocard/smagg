{{ config(materialized='table') }}

with stop_times_rich as (
	select * from {{ ref('stop_times_rich') }}
),

source_updown as (
	select
		up_cluster_name, 
		passengers_count,
		timelaps,
		upper(split_part(Line, '-', 2)) route_id,
		case line_direction
			when 'S1' then 0
			when 'S2' then 1
		end direction_id,
		goal
	from {{ ref('updown_per_cluster_inout') }}
	where split_part(Line, '-', 1) = 'tram'
		and line_direction <> 'ALL'
),

final_table as (
	select stop_id, stop_times_rich.trip_id, sum(source_updown.passengers_count), stop_times_rich.arrival_time
	from stop_times_rich
	join source_updown
		on stop_times_rich.timelaps = source_updown.timelaps
			and stop_times_rich.route_id = source_updown.route_id
			and stop_times_rich.direction_id = source_updown.direction_id
			and (stop_times_rich.new_cluster_name = source_updown.up_cluster_name
				or stop_times_rich.old_cluster_name = source_updown.up_cluster_name)
	where source_updown.goal = 'ALL'
	group by stop_id, stop_times_rich.trip_id, source_updown.up_cluster_name, stop_times_rich.arrival_time
)

select *
from final_table