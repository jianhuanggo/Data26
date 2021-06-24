import os
import re
from twilio.rest import Client
from Communication import pgcombase
from Data.Utils import pgyaml as pgyl
from Data.Utils import pgdirectory


# Find these values at https://twilio.com/user/account
# To set up environmental variables, see http://twil.io/secure


class PGTwilio(pgcombase.PGCommBase):

    def __init__(self, project_name: str = "twilio", logging_enable: bool = False):
        super(PGTwilio, self).__init__(project_name=project_name,
                                       object_short_name="PG_TW",
                                       config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                       logging_enable=logging_enable)

        print(self._config.parameters)

        self._client = Client(self._config.parameters['TWILIO_ACCOUNT_SID'], self._config.parameters['TWILIO_AUTH_TOKEN'])

    def phone_num_format(self, phone_num: str):
        only_num = str(re.sub("[^0-9]", "", phone_num))
        return only_num if len(only_num) < 10 else f"1{only_num}"

    def send_msg(self, receive_phone_num, msg_text):
        try:
            message = self._client.api.account.messages.create(
                to=self.phone_num_format(receive_phone_num),
                #from_=self.phone_num_format(receive_phone_num),
                from_="+14048828254",
                body=msg_text)

            print(message.sid)
        except Exception as err:
            if not self._logger:
                raise Exception(f"{err}\nSomething is wrong! Not able to send SMS")
            else:
                self._logger(f"{err}\nSomething is wrong! Not able to send SMS")

    def call_msg(self, receive_phone_num):
        try:
            call = self._client.calls.create(
                twiml='<Response><Say voice = "alice">Ahoy, World!</Say></Response>',
                        #< Say voice = "alice" > Thanks for trying our documentation.Enjoy! < / Say >
                        #< Play > https://demo.twilio.com / docs / classic.mp3 < / Play >
                        #to='+14155551212',
                to=self.phone_num_format(receive_phone_num),
                from_='+14048828254')
            print(call.sid)
        except Exception as err:
            if not self._logger:
                raise Exception(f"{err}\nSomething is wrong! Not able to call {self.phone_num_format(receive_phone_num)}")
            else:
                self._logger(f"{err}\nSomething is wrong! Not able to call {self.phone_num_format(receive_phone_num)}")


if __name__ == '__main__':
    x = PGTwilio("mytest")
    #x.send_msg("770-309-5053", "hello from Jian")
    x.send_msg("630-276-6656", "hello from Jian")


"""

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']

client = Client(account_sid, auth_token)

client.api.account.messages.create(
    to="+12316851234",
    from_="+15555555555",
    body="Hello there!")



import Config.pgconfig as pgconfig
import Logging.pglogging as pglogging
import sys

LOGGING_LEVEL = pglogging.logging.INFO
pglogging.logging.basicConfig(level=LOGGING_LEVEL)


class StockData:
    def __init__(self):
        self._project_name = self.__class__.__name__.lower()
        self._config = pgconfig.Config(project_name=self._project_name)
        try:
            self._logger = pglogging.Logging(self._config, logging_level=LOGGING_LEVEL,
                                             subject=f" {self._project_name} logger").getLogger(self._project_name)

        except Exception as err:
            self._logger.logging.critical(f"unable to instantiate Daemon logger {err}")
            sys.exit(99)

        self._logger.debug('Instantiate config and metadata objects')
        self._logger.info(f"The log for '{self._project_name}' is stored at {getattr(self._config, 'logger_file_filedir')}")

"""