import sys
import logging
from Data.Config import pgconfig
from types import SimpleNamespace
from Data.Utils import pgprocess as pr
from Data.Mixin import connectstring
from Data.Logging import pglogging as log

loggingLevel = logging.INFO
logging.basicConfig(level=loggingLevel)


class DataProfiler(connectstring.ConStrMixin):

    def __init__(self, args=None):

        self.conf = pgconfig.Config()
        if args:
            self.args = args
            self.args.parameters = self.conf.parameters
            self.args.name = 'Data_profiler'

        else:
            raise ("Missing table and configuration info, please pass them into the object.")

        try:
            if not self.args.logger:
                self.args.logger = log.Logging(self.conf, logging_level=loggingLevel,
                                               subject='{0} logger'.format(self.args.name)).getLogger(self.args.name)

        except Exception as err:
            logging.critical('unable to instantiate Daemon logger' + str(err))
            sys.exit(300)

        self._url = None
        self._db_client_home = None
        self._extract_filename = None
        self._query_result = None
        self.application_name = "data_profiler"
        self.args.logger.info("Environment variables loaded correctly")

    def perform(self):
        try:
            result = pr.run_query(db_client_dbshell=self.args.parameters['SCOOT_CLIENT_DBSHELL'],
                                  db_url=self.get_postgresql_url(),
                                  query=self.args.arguments.query_condition)

            self._query_result = result[0]

        except Exception as err:
            self.args.logger.critical(f"Something is wrong {err}")

        return self._query_result

    @property
    def query_result(self):
        return self._query_result

    @property
    def matching_operator(self):
        return self.args.arguments.matching_operator

    @property
    def query_condition(self):
        return self.args.arguments.query_condition

    @property
    def threshold_value(self):
        return self.args.arguments.threshold_value

    @property
    def get_arguments(self):
        return self.args.arguments

    @staticmethod
    def matching_condition(*, condition1: int, condition2: int, operator: str) -> bool:

        action = None
        dp_operator = {
                        'greater': '>',
                        'less': '<',
                        'equal': '='
                       }

        if operator.lower() not in dp_operator:
            raise ("This is not a valid operator")

        if condition1 > condition2:
            action = 'greater'
        elif condition1 == condition2:
            action = 'less'
        else:
            action = 'equal'

        if operator.lower() == action:
            return True
        else:
            return False


if __name__ == '__main__':

    #config = config.Config()
    new_args = SimpleNamespace()
    new_args.arguments = SimpleNamespace(query_condition='select count(1) from batteries',
                                         matching_operator='greater',
                                         threshold_value='10000',)
    new_args.logger = None
    dp = DataProfiler(new_args)
    dp.perform()

    print(dp.matching_condition(condition1=int(dp.query_result),
                                condition2=int(dp.threshold_value),
                                operator=dp.matching_operator))
