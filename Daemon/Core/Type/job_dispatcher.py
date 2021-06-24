from sqlalchemy import func
from sqlalchemy import and_, cast, Integer
from sqlalchemy.exc import DataError, DatabaseError, IntegrityError
from sqlalchemy import subquery
from Daemon.Core import daemon_decorator as db
from Daemon.Model import Job
from Daemon.Model import Job_Instance
from Daemon.Core.Type import pgdaemontypebase


class JobDispatcher(pgdaemontypebase.PGDaemonBase):

    @db.connect('meta')
    def __init__(self, args, db_instance=None):
        super().__init__(args)
        self.db_instance=db_instance

    def run(self):

        self.args.logger.info(f"Instantiate an instance of job dispatcher")

        #sub_query = db_instance.session.query(Job_Instance.job_id, Job_Instance.status, func.max(Job_Instance.job_instance_id)).group_by(Job_Instance.job_id).subquery()
        #recurr_job_query = db_instance.session.query(Job).outerjoin(sub_query, Job.job_id == sub_query.c.job_id).filter(
        #    and_(sub_query.c.status == 'DONE', Job.hold_flag == '0', Job.schedule_id == '0'))
        #print(recurr_job_query)

        subquery1 = self.db_instance.session.query(Job_Instance, func.rank().over(order_by=cast(Job_Instance.job_instance_id, Integer).desc(),
                                              partition_by=Job_Instance.job_id).label('rnk')).subquery()
        subquery2 = self.db_instance.session.query(subquery1).filter(subquery1.c.rnk == 1).subquery()
        recurr_job_query = self.db_instance.session.query(Job).outerjoin(subquery2, Job.job_id == subquery2.c.job_id).filter(
            and_(subquery2.c.status == 'DONE', Job.hold_flag == '0', Job.schedule_id == '0'))

        #print(recurr_job_query)

        self.args.logger.info(f"Running the logic for recurring jobs")
        for _row in recurr_job_query.all():
            self.args.logger.info(f"Adding {_row.job_name} with instance id# {_row.job_id}...")
            try:
                self.db_instance.session.add(Job_Instance(_row.job_id, _row.job_name, _row.schedule_id, _row.priority_value,
                                                     _row.server_node, _row.hold_flag, '0', _row.job_type,
                                                     _row.job_command, _row.job_argument, _row.system_parameter))

                self.db_instance.session.commit()

            except (DataError, DatabaseError, IntegrityError) as e:
                self.args.logger.critical(f"Something wrong with adding job instances for recurring jobs {e}")
                raise ("Something wrong with inserting to Job Instance table for recurring jobs")

        one_time_job_query = self.db_instance.session.query(Job, Job_Instance).outerjoin(Job_Instance, Job.job_id == Job_Instance.job_id).filter(and_(Job.hold_flag == '0',
                                                       Job_Instance.job_instance_id == None))

        #print(one_time_job_query)

        self.args.logger.info(f"Running the logic for one time jobs")
        for _row in one_time_job_query.all():
            try:
                self.args.logger.info(_row[0].job_id)
                self.args.logger.info(_row[0].job_name)

                self.args.logger.info(f"Adding {_row[0].job_name} with instance id# {_row[0].job_id}...")
                self.db_instance.session.add(Job_Instance(_row[0].job_id, _row[0].job_name, _row[0].schedule_id, _row[0].priority_value,
                                                     _row[0].server_node, _row[0].hold_flag, '0', _row[0].job_type,
                                                     _row[0].job_command, _row[0].job_argument, _row[0].system_parameter))
                self.db_instance.session.commit()

            except (DataError, DatabaseError, IntegrityError) as e:
                raise ("Something wrong with inserting to Job Instance table for one time job")

        self.args.logger.info(f"Job dispatcher is completed")


class trySQL:

    def trysql(self, db_instance=None):
        #query = db_instance.session.query(Job, Job_Instance).outerjoin(Job.job_id == Job_Instance.job_id).filter( and_(Job.hold_flag == '0',
                                                       #Job.schedule_id != '0', Job_Instance.job_instance_id is None))

        #query = db_instance.session.query(Job, Job_Instance).outerjoin(Job_Instance, Job.job_id == Job_Instance.job_id).filter(and_(Job.hold_flag == '0', Job_Instance.job_instance_id == None))
        #sub_query = db_instance.session.query(Job_Instance.job_id, Job_Instance.status, func.max(Job_Instance.job_instance_id)).group_by(Job_Instance.job_id).subquery()



        subquery1 = db_instance.session.query(Job_Instance, func.rank().over(order_by=Job_Instance.job_instance_id.desc(), partition_by=Job_Instance.job_id).label('rnk')).subquery()
        subquery2 = db_instance.session.query(subquery1).filter(subquery1.c.rnk == 1).subquery()
        recurr_job_query = db_instance.session.query(Job).outerjoin(subquery2, Job.job_id == subquery2.c.job_id).filter(and_(subquery2.c.status == 'DONE', Job.hold_flag == '0', Job.schedule_id == '0'))
        print(recurr_job_query)

        #for row in query.all():
        #    print (row)

        #row = query.all()
        #print(row[0])
        #print(row[0][0].job_id)
        #print(row[Job].job_id)

        #print([row.get('columnName') for row in query.all()])


def job_dispatcherm(args):
    JobDispatcher(args).run()


if __name__ == '__main__':
    trySQL().trysql()


"""
SELECT metaschema.scd_job.job_id AS metaschema_scd_job_job_id, metaschema.scd_job.job_name AS metaschema_scd_job_job_name, metaschema.scd_job.schedule_id AS metaschema_scd_job_schedule_id, metaschema.scd_job.priority_value AS metaschema_scd_job_priority_value, metaschema.scd_job.server_node AS metaschema_scd_job_server_node, metaschema.scd_job.hold_flag AS metaschema_scd_job_hold_flag, metaschema.scd_job.time_created AS metaschema_scd_job_time_created, metaschema.scd_job.time_updated AS metaschema_scd_job_time_updated 
FROM metaschema.scd_job LEFT OUTER JOIN (SELECT metaschema.scd_job_instance.job_id AS job_id, metaschema.scd_job_instance.status AS status, max(metaschema.scd_job_instance.job_instance_id) AS max_1 
FROM metaschema.scd_job_instance GROUP BY metaschema.scd_job_instance.job_id) AS anon_1 ON metaschema.scd_job.job_id = anon_1.job_id 
WHERE anon_1.status = %(status_1)s AND metaschema.scd_job.hold_flag = %(hold_flag_1)s AND metaschema.scd_job.schedule_id = %(schedule_id_1)s


SELECT metaschema.scd_job.job_id AS metaschema_scd_job_job_id, metaschema.scd_job.job_name AS metaschema_scd_job_job_name, metaschema.scd_job.schedule_id AS metaschema_scd_job_schedule_id, metaschema.scd_job.priority_value AS metaschema_scd_job_priority_value, metaschema.scd_job.server_node AS metaschema_scd_job_server_node, metaschema.scd_job.hold_flag AS metaschema_scd_job_hold_flag, metaschema.scd_job.time_created AS metaschema_scd_job_time_created, metaschema.scd_job.time_updated AS metaschema_scd_job_time_updated 
FROM metaschema.scd_job LEFT OUTER JOIN (SELECT anon_2.job_instance_id AS job_instance_id, anon_2.job_id AS job_id, anon_2.job_name AS job_name, anon_2.schedule_id AS schedule_id, anon_2.priority_value AS priority_value, anon_2.server_node AS server_node, anon_2.hold_flag AS hold_flag, anon_2.ignore_flag AS ignore_flag, anon_2.start_time AS start_time, anon_2.end_time AS end_time, anon_2.status AS status, anon_2.job_comment AS job_comment, anon_2.time_created AS time_created, anon_2.time_updated AS time_updated, anon_2.rnk AS rnk 
FROM (SELECT metaschema.scd_job_instance.job_instance_id AS job_instance_id, metaschema.scd_job_instance.job_id AS job_id, metaschema.scd_job_instance.job_name AS job_name, metaschema.scd_job_instance.schedule_id AS schedule_id, metaschema.scd_job_instance.priority_value AS priority_value, metaschema.scd_job_instance.server_node AS server_node, metaschema.scd_job_instance.hold_flag AS hold_flag, metaschema.scd_job_instance.ignore_flag AS ignore_flag, metaschema.scd_job_instance.start_time AS start_time, 
metaschema.scd_job_instance.end_time AS end_time, metaschema.scd_job_instance.status AS status, metaschema.scd_job_instance.job_comment AS job_comment, 
metaschema.scd_job_instance.time_created AS time_created, metaschema.scd_job_instance.time_updated AS time_updated, rank() OVER (PARTITION BY metaschema.scd_job_instance.job_id ORDER BY metaschema.scd_job_instance.job_instance_id DESC) AS rnk 
FROM metaschema.scd_job_instance) AS anon_2 
WHERE anon_2.rnk =  1) AS anon_1 ON metaschema.scd_job.job_id = anon_1.job_id 
WHERE anon_1.status = 'DONE')s AND metaschema.scd_job.hold_flag = '0')s AND metaschema.scd_job.schedule_id = '0')s
"""
