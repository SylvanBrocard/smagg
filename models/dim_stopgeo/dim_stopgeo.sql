/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with old_new_names as (
    select *
    from {{ ref('old_new_names') }}
),

stop_names as (
    select
        st.stop_name,
        split_part(st.stop_name, ', ', 2) new_cluster_name
    from {{ ref('stops') }} st
)

select DISTINCT
    split_part(cl.id,':', 2) cluster_id,
    st.stop_id,
    st.stop_name,
    upper(stop_names.new_cluster_name) new_cluster_name,
    old_new_names.old_cluster_name,
    st.stop_lat,
    st.stop_lon,
    st.parent_station,
    cl.city
	FROM {{ ref('stops') }}  st
	join {{ ref('smmag_line_clusters') }} cl
		on split_part(cl.id,':',2) = st.parent_station 
    join stop_names
        on stop_names.stop_name = st.stop_name
    left join old_new_names
        on stop_names.new_cluster_name = old_new_names.new_cluster_name