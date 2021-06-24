select 'Time NOW: ' || cast(now() as varchar)
union all
select 'Longest job for NA region: ' || min(start_time) from metaschema.scd_job_instance where status in ('NEW', 'RUN')
union all
select 'Longest job for SA region: ' || min(start_time) from metaschemasa.scd_job_instance where status in ('NEW', 'RUN')
union all
select 'Longest job for EU region: ' || min(start_time) from metaschemaeu.scd_job_instance where status in ('NEW', 'RUN')