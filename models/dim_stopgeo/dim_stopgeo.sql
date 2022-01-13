/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT DISTINCT split_part(cl.id,':', 2) as cluster_id,st.stop_id, st.stop_name, st.stop_lat
, st.stop_lon,st.parent_station,cl.city
	FROM {{ ref('stops') }} st, {{ ref('smmag_line_clusters') }} cl 
	where  split_part(cl.id,':', 2)=st.parent_station