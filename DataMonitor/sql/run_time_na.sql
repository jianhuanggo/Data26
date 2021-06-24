select job_instance_id, job_name, priority_value, ET- ST duration from (
	SELECT job_instance_id, job_name, priority_value, to_timestamp(start_time, 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone at time zone 'Etc/UTC' as ST, 
	to_timestamp(end_time, 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone at time zone 'Etc/UTC' as ET, 
	rank() OVER (PARTITION BY metaschema.scd_job_instance.job_id ORDER BY cast(metaschema.scd_job_instance.job_instance_id as integer) DESC) AS rnk 
	FROM metaschema.scd_job_instance where status = 'DONE') AS anon_2 where anon_2.rnk = 1 order by 2
