from types import SimpleNamespace
from sqlalchemy import and_, or_, not_, Integer, String, func, cast
from Daemon.Model import Threshold, Job, Job_Instance, Job_Task_Lock, System_Parameter
from Daemon.Core import daemon_decorator as db

from Daemon.Core.Type import pgdaemontypebase
from Daemon.Core.Database import db_operation
from Data.Config import pgconfig


class Watcher(pgdaemontypebase.PGDaemonBase):

    def __init__(self, args):
        super().__init__(args)

    @db.connect('meta')
    def is_task_lock_rec_exist(self, job_instance_id: str, db_instance=None) -> bool:
        return db_instance.session.query(db_instance.session.query(Job_Task_Lock).
                                         filter(Job_Task_Lock.job_instance_id == str(job_instance_id)).exists()). \
                                         scalar()

    @db.connect('meta')
    def get_failed_job(self, db_instance=None):
        for _row in db_instance.session.query(Job_Instance).filter(and_(Job_Instance.status == 'FAIL',
                                                                   Job_Instance.try_num <= 3)).all():
            yield _row

    @db.connect('meta')
    def run(self, db_instance=None):
        self.args.logger.info("Running Daemon Watcher... ")
        try:
            for _row in self.get_failed_job():
                for i in range(pgconfig.NUM_OF_DELETE_TRY):
                    if db_operation.delete_instance_by_id(job_instance_id=_row.job_instance_id):
                        if not self.is_task_lock_rec_exist(_row.job_instance_id):
                            self.args.logger.info(f"Removed {_row.job_name} @ id# {_row.job_instance_id}")
                            break

                if int(_row.try_num) < pgconfig.NUM_OF_TRY_UNTIL_FULL:
                    if db_operation.update_new_status_on_incr(job_instance_id=_row.job_instance_id):
                        self.args.logger.info(f"Set status for {_row.job_name} @ id# {_row.job_instance_id} to 'New' ")
                        self.args.logger.info("Watcher found appropriate jobs and restarted them")

                elif int(_row.try_num) == pgconfig.NUM_OF_TRY_UNTIL_FULL:
                    if db_operation.update_new_status_on_full(job_instance_id=_row.job_instance_id):
                        self.args.logger.info(f"Set status for {_row.job_name} @ id# {_row.job_instance_id} to 'New' ")
                        self.args.logger.info("Watcher found appropriate jobs and restarted them")

        except Exception as err:
            self.args.logger.critical(f"Could not restart job due to {err}")

    """
    @db.connect('meta')
    def check_db_health(self, db_instance=None):
        health = db.ping_all()
        for index, item in enumerate(health):
            try:
                db_instance.session.query(System_Parameter).filter(System_Parameter.system_id == str(index)).\
                                                               update({'system_health_status': health[index]})
            except Exception as err:
                self.args.logger.critical(f"Could not update system health table {err}")
                
    """

    @db.connect('meta')
    def check_db_health(self, db_instance=None):
        sys_health_func_list = []
        try:
            for _row in db_instance.session.query(System_Parameter).order_by(cast(System_Parameter.system_id, Integer)).all():
                sys_health_func_list.append(db.get_db_parameter(_row.system_name)(db.ping))

            sys_health = db.ping_all(sys_health_func_list)
            for index, item in enumerate(sys_health):
                db_instance.session.query(System_Parameter).filter(System_Parameter.system_id == str(index)).\
                                                               update({'system_health_status': str(item)})
                self.args.logger.info(f"updated system {System_Parameter.system_name} "
                                      f"to {System_Parameter.system_health_status}")
        except Exception as err:
            self.args.logger.critical(f"Could not update system health table {err}")


def watcherm(args):
    wt = Watcher(args)
    wt.run()
    wt.check_db_health()


if __name__ == '__main__':
    #try1()
    args = SimpleNamespace()
    watcher = Watcher(args)
    watcher.run()
