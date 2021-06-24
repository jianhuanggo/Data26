select 'Failed NA jobs: ' || job_name "job name", try_num, job_instance_id, start_time from metaschema.scd_job_instance where status = 'FAIL'
union all
select 'Failed SA jobs: ' || job_name "job name", try_num, job_instance_id, start_time from metaschemasa.scd_job_instance where status = 'FAIL'
union all
select 'Failed EU jobs: ' || job_name "job name", try_num, job_instance_id, start_time from metaschemaeu.scd_job_instance where status = 'FAIL'