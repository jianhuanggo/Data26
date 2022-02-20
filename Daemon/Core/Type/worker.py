import os
import time
import datetime
import subprocess
from sqlalchemy import and_, or_, not_, Integer, String
from sqlalchemy.exc import CompileError, DatabaseError, IntegrityError, DataError
from sqlalchemy.sql.expression import cast
from Data.Utils import pgparse
from Daemon.Core.Type import pgdaemontypebase
from Daemon.Core import daemon_decorator as db
from Daemon.Model import Job
from Daemon.Model import Job_Instance
from Daemon.Model import Job_Task_Lock, Job_Task_Check
from sqlalchemy import func
#from Daemon.Task import task


class worker(pgdaemontypebase.PGDaemonBase):
    def __init__(self, args):
        super().__init__(args)
        #self.args.logger.info(args.conf.parameters['PYTHON_PATH'])

    def test_job_type_system(self, current_task, *args, **kwargs):
        loadConf = []
        self.args.logger.info("starting debugging!!!!!!!")
        #self.args.logger.info(daemon_name)
        self.args.logger.info(args)
        self.args.logger.info(kwargs)
        logger = args[1]
        loadConf.append(current_task.job_command)
        try:
            for argument in current_task.job_argument.strip().split():
                loadConf.append(argument)
        except Exception as err:
            self.args.logger.critical(err)

        self.args.logger.info(loadConf)
        self.args.logger.info("ending debugging!!!!!!!")

    @db.connect('meta')
    def task_selection(self, db_instance=None):
        # print(self.args.conf.parameters)
        self.args.logger.info("Start Task Selection Process...")

        db_instance.session.rollback()
        query = db_instance.session.query(Job_Instance).join(Job, Job_Instance.job_id == Job.job_id).filter(
            and_(Job_Instance.hold_flag == '0',
                 Job_Instance.ignore_flag == '0', Job.hold_flag == '0'),
            Job_Instance.status == 'NEW').order_by(cast(Job_Instance.priority_value, Integer),
                                                   cast(Job_Instance.job_instance_id, Integer))
        row = None
        # print(query)
        for _row in query.all():
            self.args.logger.info(f"{str(_row.job_id)}, {_row.job_name}")
            #print(_row)

            try:
                db_instance.session.add(Job_Task_Lock(_row.job_instance_id,
                                                      _row.job_id,
                                                      _row.job_name,
                                                      self.args.daemon_name))
                if db_instance.commit():
                    row = _row
                    break

            except:
                pass

        if row:
            self.args.logger.info(f"Job {row.job_name} with instance id {row.job_instance_id} is selected")
            # print(f"{row.job_instance_id} and {row.job_id} and {row.job_name}")
            # 'job_comment': self.args.pid
            try:
                # print(row.try_num)
                # new_try_num = int(row.try_num) + 1
                # db_instance.session.query(Job_Instance).filter(Job_Instance.job_instance_id ==
                # str(row.job_instance_id)).update({'status': 'RUN',
                # 'job_comment': self.args.daemon_name,
                # 'try_num': str(new_try_num)})

                row.status = 'RUN'
                self.task_status_update(row)
                db_instance.commit()

            except (CompileError, IntegrityError, DatabaseError) as err:
                row.status = 'FAIL'
                self.task_status_update(row)

                # db_instance.session.query(Job_Instance).filter(Job_Instance.job_instance_id == str(row.job_instance_id)).update(
                # {'status': 'FAIL'})
                self.args.logger.info(f"Could not set job instance id {row.job_instance_id} status to 'RUN'")
                raise (f"something wrong with the update statement {err}!")

            # stmt = Job_Instance.update().values(status='RUNNING').where(Job_Instance.job_instance_id == row.job_instance_id)
            # print(stmt.statement.compile(db_instance.engine))

        # db_instance.close()

        return row

        # task = SimpleNamespace(job_instance_id='row.job_instance_id', job_id='row.job_')

    @db.connect('meta')
    def task_checking(self, task, db_instance=None):
        self.args.logger.info("Start Task Checking Process...")
        # print(task)
        if not task:
            db_instance.session.close()
            return

        try:
            # db_instance.session.add(Job_Task_Check(task.job_instance_id, task.job_id, task.job_name, task.job_comment))
            # db_instance.session.query(Job_Instance).filter(Job_Instance.job_instance_id == str(task.job_instance_id)).update(
            # {'status': 'RUN'})
            # db_instance.session.commit()
            self.args.logger.info(f"Successfully writing log record to table scd_job_task_check with instance id "
                                  f"{task.job_instance_id}")

        except (DatabaseError, DataError, IntegrityError) as err:
            self.args.logger.critical(f"Could not write log record to table scd_job_task_check with instance id "
                                      f"{task.job_instance_id}")
            raise (f"There is something wrong with inserting into Task Checking table {err}")

        # finally:
        #    db_instance.close()

    @db.connect('meta')
    def task_status_update(self, job_task, db_instance=None):
        self.args.logger.info(f"{job_task.status}")
        if not hasattr(job_task, "affected_rowcount") or job_task.affected_rowcount is None:
            job_task.affected_rowcount = 0
        now = str(datetime.datetime.now())
        try:
            if job_task.status == 'RUN':
                # print(f"try_num is {job_task.try_num}")
                new_try_num = int(job_task.try_num) + 1

                db_instance.session.query(Job_Instance).filter(
                    Job_Instance.job_instance_id == str(job_task.job_instance_id)). \
                    update({'status': str(job_task.status), 'time_updated': now,
                            'end_time': now, 'try_num': str(new_try_num), 'job_comment': self.args.daemon_name})
            else:
                if job_task.status == 'DONE':
                    # new_system_parameter = f"datafile_loc:{str(job_task.datafile_loc)}"
                    db_instance.session.query(Job_Instance).filter(
                        Job_Instance.job_instance_id == str(job_task.job_instance_id)). \
                        update({'status': str(job_task.status), 'time_updated': now,
                                'end_time': now, 'affected_row_count': str(job_task.affected_rowcount),
                                # 'system_parameter': new_system_parameter})
                                'system_parameter': job_task.system_parameter})

                    db_instance.session.query(Job).filter(
                        # Job.job_id == str(job_task.job_id)).update({'system_parameter': new_system_parameter})
                        Job.job_id == str(job_task.job_id)).update({'system_parameter': job_task.system_parameter})

                # failure scenario
                else:
                    db_instance.session.query(Job_Instance).filter(
                        Job_Instance.job_instance_id == str(job_task.job_instance_id)). \
                        update({'status': str(job_task.status), 'time_updated': now})
            db_instance.session.commit()
            self.args.logger.info(f"Successfully reflect the job status for {job_task.job_instance_id} to metadata")

        except Exception as err:
            self.args.logger.critical(f"Could not update status table {err}")
            raise (f"There is something wrong with update status in Job Task Instance table {err}")

        # finally:
        #    db_instance.close()

    @db.connect('meta')
    def run(self, db_instance=None):
        current_task = self.task_selection()

        if current_task:
            self.args.logger.info(f"This is the task: task command={current_task.job_command}, "
                                  f"task argument={current_task.job_argument}, "
                                  f"system parameter={current_task.system_parameter} ")

            # Setting default value for task attributes
            current_task.affected_rowcount = 0
            # current_task.delta_file_loc = '0'

            try:

                self.task_checking(current_task)
                # self.args.logger.info(f"{current_task.job_type}, {current_task.job_command}, {current_task.job_argument}")
                #job_status = self.test_job_type_system(current_task,  self.args.daemon_name, self.args.logger)
                self.args.logger.info(current_task.job_type.lower())
                job_status = job_type.get(current_task.job_type.lower(), "other")(current_task,
                                                                                  self.args.daemon_name,
                                                                                  self.args.logger,
                                                                                  self.args.conf.parameters)
                if job_status:
                    # if task.selector(current_task.job_command)(current_task.job_argument, self.args.logger):
                    time.sleep(1)
                    current_task.status = 'DONE'
                    self.task_status_update(current_task)
                    self.args.logger.info(f"Task {current_task.job_instance_id} is completed.")
                    # print("I'm here 1111")
                else:
                    current_task.status = 'FAIL'
                    self.task_status_update(current_task)
                    self.args.logger.info(f"End to end validation has failed.....")

            except Exception as err:
                current_task.status = 'FAIL'
                self.task_status_update(current_task)
                self.args.logger.critical(f"Something wrong with running command {current_task.job_command}")
                raise ("Something wrong with running command")

            # finally:
            #    db_instance.close()
            # print("I'm here 2")
            return
        else:
            self.args.logger.info(f"This task is completed: Nothing to process")


# def job_type_system(current_task, *args, **kwargs):
#     loadConf = []
#     logger = args[1]
#     logger.info(args[0])
#
#     #logger.info(args)
#     if "PYTHONPATH" not in os.environ and "PYTHONPATH" in args[2]:
#         os.environ['PYTHONPATH'] = args[2]['PYTHONPATH']
#
#     loadConf.append(current_task.job_command)
#     try:
#         for argument in current_task.job_argument.strip().split():
#             loadConf.append(argument)
#     except Exception as err:
#         logger.critical(err)
#
#     logger.info(loadConf)
#
#
#     #loadConf = ['python', 'pgdaemon.py', "-i", str(daemon_id), "-t", "60", "-y", daemon_type, "start"]
#     # print(loadConf)
#     #self.args.logger.debug(loadConf)
#
#     p2 = subprocess.Popen(loadConf)
#     p2.wait()
#     if p2.returncode != 0:
#         return False
#     time.sleep(2)
#     logger.info("ending debugging!!!!!!!")
#     return True

def job_type_system(current_task, *args, **kwargs):
    loadConf = []
    logger = args[1]
    logger.info(args[0])

    #logger.info(args)
    if "PYTHONPATH" not in os.environ and "PYTHONPATH" in args[2]:
        os.environ['PYTHONPATH'] = args[2]['PYTHONPATH']

    loadConf.append(current_task.job_command)
    try:
        for argument in current_task.job_argument.strip().split():
            loadConf.append(argument)
    except Exception as err:
        logger.critical(err)

    logger.info(loadConf)


    #loadConf = ['python', 'pgdaemon.py', "-i", str(daemon_id), "-t", "60", "-y", daemon_type, "start"]
    # print(loadConf)
    #self.args.logger.debug(loadConf)

    logger.info(current_task.system_parameter)
    if current_task.system_parameter:
        p2 = subprocess.Popen(loadConf, cwd=current_task.system_parameter)
    else:
        p2 = subprocess.Popen(loadConf)
    p2.wait()
    if p2.returncode != 0:
        return False
    time.sleep(2)
    logger.info("ending debugging!!!!!!!")
    return True

def job_type_native(current_task, *args, **kwargs):
    if False:
        job_status, back_sys_param = task.selector(current_task.job_command)(self.args.daemon_name,
                                                                         current_task.job_argument,
                                                                         current_task.system_parameter,
                                                                         self.args.logger)
        self.args.logger.info(f"Job status: {job_status}, system parameter: {back_sys_param}")

        if current_task.job_command == 'data_mover':
            current_task.affected_rowcount = int(back_sys_param.affected_rowcount)
            current_task.system_parameter = pgparse.ns_to_json(back_sys_param)
        # current_task.datafile_loc = back_sys_param.datafile_loc

    # print(current_task.affected_rowcount, current_task.datafile_loc)

    # current_task.affected_rowcount = int(affected_rowcount)
    return True


def job_type_unsupported(current_task, *args, **kwargs):
    return True


job_type = {
    'system': job_type_system,
    'native': job_type_native,
    'other': job_type_unsupported
}


def workerm(args):
    worker(args).run()


@db.connect('meta')
def try_run(db_instance=None):
    stmt = db_instance.session.query(Job_Instance).filter(Job_Instance.job_instance_id == '10').update(
        {'job_name': 'ok', 'status': 'DONE'})
    print(stmt)


@db.connect('meta')
def try_run1(db_instance=None):
    query = db_instance.session.query(Job_Instance).join(Job, Job_Instance.job_id == Job.job_id).filter(
        and_(Job_Instance.hold_flag == '0', Job_Instance.ignore_flag == '0', Job.hold_flag == '0'),
        Job_Instance.status == 'NEW').order_by(cast(Job_Instance.priority_value, Integer),
                                               cast(Job_Instance.job_instance_id, Integer))

    print(query)


if __name__ == '__main__':
    pass