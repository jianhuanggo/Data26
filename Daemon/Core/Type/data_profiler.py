import time
import datetime
from sqlalchemy import and_, or_, not_, Integer, String
from sqlalchemy.exc import CompileError, DatabaseError, IntegrityError, DataError
from sqlalchemy.sql.expression import cast

from Data.Utils import db, pgparse
from Daemon.Model import Job
from Daemon.Model import Job_Instance
from Daemon.Model import Job_Task_Lock, Job_Task_Check
from Daemon.Core.Type import pgdaemontypebase

#from Daemon.Task import task


class DataProfiler(pgdaemontypebase.PGDaemonBase):
    def __init__(self, args):
        super().__init__(args)

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
                self.args.logger.info(f"{current_task.job_command}")
                job_status, back_sys_param = task.selector(current_task.job_command)(self.args.daemon_name,
                                                                                     current_task.job_argument,
                                                                                     current_task.system_parameter,
                                                                                     self.args.logger)
                self.args.logger.info(f"Job status: {job_status}, system parameter: {back_sys_param}")

                if current_task.job_command == 'data_profiler':
                    current_task.system_parameter = pgparse.ns_to_json(back_sys_param)
                    # current_task.datafile_loc = back_sys_param.datafile_loc

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
            # print(_row)

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

            try:
                row.status = 'RUN'
                self.task_status_update(row)
                db_instance.commit()

            except (CompileError, IntegrityError, DatabaseError) as err:
                row.status = 'FAIL'
                self.task_status_update(row)

                self.args.logger.info(f"Could not set job instance id {row.job_instance_id} status to 'RUN'")
                raise (f"something wrong with the update statement {err}!")

        # db_instance.close()

        return row

def dataprofilem(args):
    DataProfiler(args).run()



