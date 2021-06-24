from types import SimpleNamespace
from Daemon.Core.Type import pgdaemontypebase
from Daemon.Core import daemon_decorator as db


class Sweeper(pgdaemontypebase.PGDaemonBase):

    def __init__(self, args):
        super().__init__(args)

        self.metaschema = self.args.conf.parameters['SCOOT_META_SCHEMA']
        self.step = "Beginning"

    def sql_vault(self, *, sql_tag):
        SQL_1 = f"insert into {self.metaschema}.scd_metadata_arch_tmp select * " \
                f"from {self.metaschema}.scd_job_instance where status = 'DONE'"

        SQL_2 = f"delete from {self.metaschema}.scd_job_instance t1 where t1.job_instance_id in " \
                f"(select job_instance_id from {self.metaschema}.scd_metadata_arch_tmp)"

        SQL_3 = f"delete from {self.metaschema}.scd_job_task_lock t1 where t1.job_instance_id in " \
                f"(select job_instance_id from {self.metaschema}.scd_metadata_arch_tmp)"

        SQL_4 = f"insert into {self.metaschema}.scd_metadata_arch select * " \
                f"from {self.metaschema}.scd_metadata_arch_tmp"

        SQL_5 = f"delete from {self.metaschema}.scd_metadata_arch_tmp "

        SQL_lookup = {'SQL_1': SQL_1,
                      'SQL_2': SQL_2,
                      'SQL_3': SQL_3,
                      'SQL_4': SQL_4,
                      'SQL_5': SQL_5
                     }

        return SQL_lookup[sql_tag]

    @db.connect('meta')
    def execute_sql(self, *, sql_tag, db_instance=None):
        try:
            print(self.sql_vault(sql_tag=sql_tag))
            db_instance.session.execute(self.sql_vault(sql_tag=sql_tag))

        except Exception as err:
            db_instance.session.rollback()
            raise err
        else:
            self.args.logger.info(f"Successfully executed sql tag {sql_tag}")

    @db.connect('meta')
    def run(self, db_instance=None):
        self.args.logger.info("Running Daemon Sweeper... ")
        self.args.logger.info(f"schema name is {self.args.conf.parameters['SCOOT_META_SCHEMA']}")

        try:

            self.step = "running SQL_1"
            self.execute_sql(sql_tag='SQL_1')
            self.step = "running SQL_2"
            self.execute_sql(sql_tag='SQL_2')
            self.step = "running SQL_3"
            self.execute_sql(sql_tag='SQL_3')
            self.step = "running SQL_4"
            self.execute_sql(sql_tag='SQL_4')
            self.step = "running SQL_5"
            self.execute_sql(sql_tag='SQL_5')
            db_instance.session.commit()
            self.step = "clean up"

        except Exception as err:
            db_instance.session.rollback()
            self.args.logger.critical(f"Could not complete {self.step} due to {err}")
        else:
            self.args.logger.info(f"Successfully completed all steps")


def sweeperm(args):
    Sweeper(args).run()


if __name__ == '__main__':
    args = SimpleNamespace()
    sweeper = Sweeper(args)
    sweeper.run()

