{{ config(materialized='table') }}
with source_times as (

    select distinct arrival_time
    from {{ ref('stop_times') }}

),

split_times as (
	select 
		arrival_time,
		mod(split_part(arrival_time, ':', 1)::integer, 24)::text time_hours,
		split_part(arrival_time, ':', 2) time_minutes
	from source_times
),

concat_times as (
	select
		arrival_time,
		to_timestamp(concat_ws(':',time_hours, time_minutes), 'HH24:MI') time_stamped
	from split_times
),

source_timelaps as (
    select distinct timelaps
    from {{ ref('updown_per_cluster_and_mode') }}
    where timelaps <> 'ALL'
),

split_timelaps as (
    select
    	timelaps,
    	split_part(split_part(timelaps, '-', 1), 'H', 1)::integer time_start_hours,
    	split_part(split_part(timelaps, '-', 1), 'H', 2)::integer time_start_minutes,
    	split_part(split_part(timelaps, '-', 2), 'H', 1)::integer time_end_hours,
    	split_part(split_part(timelaps, '-', 2), 'H', 2)::integer time_end_minutes
    from source_timelaps
),

concat_timelaps as (
	select
		timelaps,
		to_timestamp(concat_ws(':', time_start_hours, time_start_minutes), 'HH24:MI') time_start,
		to_timestamp(concat_ws(':', time_end_hours, time_end_minutes), 'HH24:MI') time_end
	from split_timelaps
)

select arrival_time, timelaps
from concat_times
cross join concat_timelaps
where
	time_stamped > time_start
	and (time_stamped < time_end or time_start > time_end)