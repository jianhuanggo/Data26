from Data.Utils import db
from Daemon.Model import Job, Job_Instance, Job_Task_Lock
from sqlalchemy.exc import DataError, IntegrityError, DatabaseError
from sqlalchemy import func


class Testing_data:
    def __init__(self):
        pass

    @db.connect('meta')
    def Data_pop_job(self, number_of_job, db_instance=None):
        #db_instance.session.query(Job)
        try:
            result = db_instance.session.query(func.max(Job.job_id)).all()
            if result[0][0]:
                starting_job_id = int(result[0][0]) + 1
            else:
                starting_job_id = 0
            print(result[0][0])
            for num in range(number_of_job):
                db_instance.session.add(Job(str(num + starting_job_id), 'test_' + str(num + starting_job_id), '0',
                                            '10', '127.0.0.1', '0', 'Package', 'data_mover',
                                            'source_system:rds;source_object:batteries;target_system:redshift;'
                                            'target_object:test_batteries'))
                print(f"Inserting Job record # {num + starting_job_id}...")
                db_instance.commit()
        except (DataError, IntegrityError, DatabaseError) as e:
            raise (f"Error during population of Job table {e}!")

        return starting_job_id

    @db.connect('meta')
    def Data_pop_job_instance(self, number_of_inst, starting_job_id, db_instance=None):
        try:
            for num in range(number_of_inst):
                db_instance.session.add(Job_Instance(num, 'test_' + str(num + starting_job_id), '0', '10', '127.0.0.1',
                                                     '0', '0', 'Package', 'data_mover',
                                                     'source_system:rds;source_object:batteries;target_system:redshift;'
                                                     'target_object:test_batteries'))

                print(f"Inserting Job Instance record JobId {num + starting_job_id}")
                db_instance.commit()
        except (DataError, DatabaseError, IntegrityError) as e:
            raise (f"Error during data populdation of Job Instance table {e}!")

    @db.connect('meta')
    def Data_pop_delete_lock(self, db_instance=None):
        try:
            db_instance.session.query(Job_Task_Lock).delete()
            #Job_Task_Lock.query.delete()
            #db_instance.execute("delete from metaschema.Job_Task_Lock")
            db_instance.commit()
        except (DataError, DatabaseError, IntegrityError) as e:
            raise (f"Error during deleting the Job Task Lock table {e}!")

    @db.connect('meta')
    def Data_clean_up(self, db_instance=None):
        db_instance.session.query(Job_Task_Lock).delete()
        db_instance.session.query(Job_Instance).delete()
        db_instance.session.query(Job).delete()
        #db_instance.session.query(Job_Instance).update({'status': 'NEW'})
        db_instance.commit()



def run(num_of_run):
    run_instance = Testing_data()
    run_instance.Data_clean_up()
    starting_id = run_instance.Data_pop_job(num_of_run)
    run_instance.Data_pop_job_instance(num_of_run, starting_id)


if __name__ == '__main__':
    run(1)

        #self, job_id, job_name, schedule_id, priority_value, server_node, hold_flag):
"""
    query = db_instance.session.query(Job_Instance).join(Job, Job_Instance.job_id == Job.job_id).filter(
        and_(Job_Instance.hold_flag == '0',
             Job_Instance.ignore_flag == '0',
             Job.hold_flag == '0'), Job_Instance.status == 'NEW')

"""
