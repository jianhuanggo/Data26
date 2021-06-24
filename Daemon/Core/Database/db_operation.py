from Data.Utils import db, pgparse
from types import SimpleNamespace
from Daemon.Model import Job_Task_Lock, Job_Instance
from sqlalchemy import and_, or_, not_, Integer, String, func


@db.connect('meta')
def delete_job_instance_lock(job_instance_id: str, db_instance=None) -> bool:
    return db_instance.session.query(Job_Task_Lock).filter \
        (Job_Task_Lock.job_instance_id == str(job_instance_id)).delete(synchronize_session='fetch')


@db.connect('meta')
def delete_instance_by_id(*, job_instance_id: str, db_instance=None):
    try:
        query_delete = delete_job_instance_lock(job_instance_id)
        db_instance.session.commit()
    except Exception as err:
        raise err
    else:
        return query_delete


@db.connect('meta')
def update_new_status_on_incr(*, job_instance_id: str, db_instance=None):
    try:
        query_update = db_instance.session.query(Job_Instance).filter(and_(Job_Instance.status == 'FAIL',
                                                                           Job_Instance.try_num < 3,
                                                                           Job_Instance.job_instance_id ==
                                                                           str(job_instance_id))).\
                                                                           update({'status': 'NEW'})
        db_instance.session.commit()
    except Exception as err:
        raise err
    else:
        return query_update


@db.connect('meta')
def update_new_status_on_full(*, job_instance_id: str, db_instance=None):
    default_datafile_loc = SimpleNamespace(datafile_loc="")
    try:
        loc = pgparse.ns_to_json(default_datafile_loc)
        restart_full_query = db_instance.session.query(Job_Instance).filter(and_(Job_Instance.status == 'FAIL',
                                                       not_(Job_Instance.job_argument.contains('table_size:large')),
                                                       Job_Instance.try_num == 3,
                                                       Job_Instance.job_instance_id == str(job_instance_id))).update(
                                                       {'status': 'NEW', 'system_parameter': str(loc),
                                                       Job_Instance.job_argument: func.replace(Job_Instance.job_argument,
                                                                                               'etl_mode:incr',
                                                                                               'etl_mode:full')},
                                                                                                synchronize_session=False)
        db_instance.session.commit()
    except Exception as err:
        raise err
    else:
        return restart_full_query

