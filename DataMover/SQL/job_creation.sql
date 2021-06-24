
insert into metaschema.scd_job (job_id, job_name, schedule_id, priority_value, server_node, hold_flag, time_created, time_updated, job_type, job_command, job_argument)
values ('1', 'data_replication_user_eligibility_states', '100', '20', '127.0.0.1', '0', NOW(), NOW(), 'Package', 'data_mover', 'source_system:rds;source_object:user_eligibility_states;target_system:redshift;target_object:test_user_eligibility_states;highwatermark:;timestamp_col:created_at;key_col:id;etl_mode:full')

insert into metaschema.scd_job (job_id, job_name, schedule_id, priority_value, server_node, hold_flag, time_created, time_updated, job_type, job_command, job_argument)
values ('2', 'data_replication_batteries', '100', '10', '127.0.0.1', '0', NOW(), NOW(), 'Package', 'data_mover', 'source_system:rds;source_object:batteries;target_system:redshift;target_object:test_batteries;highwatermark:;timestamp_col:created_at,updated_at;key_col:id;etl_mode:full')


