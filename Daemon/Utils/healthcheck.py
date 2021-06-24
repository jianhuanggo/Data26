from Daemon.Conf import dnconf as pgconf
from Data.Utils import pgprocess as pr
from types import SimpleNamespace
import logging
import sys
from Data.Logging import pglogging as log
from Daemon.Utils import base


class HealthCheck(base.JobManagement):

    def __init__(self, args=None):
        super().__init__(args)
        _conf = pgconf.DNConf()
        if args:
            self.args = args
            self.args._parameters = _conf
            self.args.name = 'Data_profiler'

        else:
            raise ("Missing table and configuration info, please pass them into the object.")

        try:
            if not self.args.logger:
                self.args.logger = log.Logging(self.args._parameters, logging_level=loggingLevel,
                                               subject='{0} logger'.format(self.args.name)).getLogger(self.args.name)

        except Exception as err:
            logging.critical('unable to instantiate Daemon logger' + str(err))
            sys.exit(300)

        self.application_name = "data_profiler"
        self.args.logger.info("Environment variables loadded correctly")


    def check(self, *, num_try: int) -> bool:
        try:
            print(self.args.parameters['SCOOT_CLIENT_DBSHELL'])
            result = pr.run_query(db_client_dbshell=self.args.parameters['SCOOT_CLIENT_DBSHELL'],
                                  db_url=self._redshift_url,
                                  query=self.args.arguments.query_condition)

            self._query_result = result[0]

            return self._query_result

        except Exception as err:
            pass
            #self.args.logger.Critical(f"Something is wrong {err}")


    def action(self):
        pass




if __name__ == '__main__':

    new_args=SimpleNamespace
    new_args.arguments=SimpleNamespace(query='select check_val from health_check',
                                       num_try=3)
    hc = HealthCheck(new_args)
    hc.setup_redshift()
    hc.check(num_try=new_args.arguments.num_try)



