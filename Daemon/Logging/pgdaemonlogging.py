#!/usr/bin/python

import logging
import smtplib
import os
import sys
import logging.handlers
from Daemon.Conf import dnconf
from Data.Utils import pgdirectory as dirFunc


class JSMTPHandler(logging.handlers.SMTPHandler):

    def __init__(self, *args, **kwargs):
        super(JSMTPHandler, self).__init__(*args, **kwargs)

    def getSubject(self, record):
        return '{0}: {1} - {2}'.format(record.levelname, record.name, record.message[:20])


class PGDaemonLogging:

    __instance = None

    @staticmethod
    def get_instance():
        if PGDaemonLogging.__instance == None:
            PGDaemonLogging(dnconf.DNConf())
        else:
            return PGDaemonLogging.__instance

    def __init__(self, conf: dnconf.DNConf, logging_level=logging.INFO, subject='logger'):
        if PGDaemonLogging.__instance == None:
            logging.debug('Instantiating Logging')
            self._loggingLevel = logging_level
            self._loggingFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self._loggingVerboseFormatter = logging.Formatter(
                '-' * 80 + '\nSEVERITY: %(levelname)s\nhost-logger: %(name)s\ntime: %(asctime)s\nfile: %(pathname)s\nmodule: %(module)s\nfunction: %(funcName)s\nline: %(lineno)d\n\nMessage:\n%(message)s\n')
            self._logger_file = None
            self._logger_smtp_subject = subject
            self._conf = conf

            if "logger_file_filedir" not in dir(self._conf):
                if f"{self._conf.parameters['ENV_VAR_PREFIX']}DATA_HOME" in self._conf.parameters:
                    log_path = self._conf.parameters[f"{self._conf.parameters['ENV_VAR_PREFIX']}DATA_HOME"] + '/logs/daemon_log'
                else:
                    raise Exception(f"{self._conf.parameters['ENV_VAR_PREFIX']}DATA_HOME is not set in dnconf.yml")

                setattr(self._conf, 'logger_file_filedir', log_path)
                print(f"INFO: Since logger_file_filedir environment variable is not set, default is to {log_path}")

            if "logger_file_when" not in dir(self._conf):
                setattr(self._conf, 'logger_file_when', 'h')
                print(f"INFO: Since logger_file_when environment variable is not set, default is to {getattr(self.conf, 'logger_file_when')}")

            if "logger_file_interval" not in dir(self._conf):
                setattr(self._conf, 'logger_file_interval', 1)
                print(f"INFO: Since logger_file_interval environment variable is not set, default is to {getattr(self.conf, 'logger_file_interval')}")

            self._logger = None
            PGDaemonLogging.__instance = self

    @property
    def logger(self):
        return self._logger

    @property
    def loggingLevel(self):
        return self._loggingLevel

    def shutdown(self):
        logging.shutdown

    def sendEmail(self, recipient_emails, subj, msg):
        _mailserver = smtplib.SMTP(*self._conf.logger_smtp_mailhost)
        #        _mailserver.set_debuglevel(1)
        _formatted_msg = '''From: Daemon <{0}>
To: {1}
Subject: {2}
MIME-Version: 1.0
Content-type: text/html

{3}

'''.format(self._conf.logger_smtp_fromaddr,
           ', '.join(recipient_emails),
           subj,
           msg)

        _mailserver.sendmail(self._conf.logger_smtp_fromaddr, recipient_emails, _formatted_msg)
        _mailserver.quit()

    def getLogger(self, logger_name):

        logging.debug('''Logging.getLogger('{0}')'''.format(logger_name))
        try:
            logger = logging.getLogger(logger_name)
            logger.setLevel(self._loggingLevel)
            logger.propagate = True  # it is True by default
        except Exception as err:
            logging.critical('''Error in JLogging.getLogger('{0}'): {1}'''.format(logger_name, str(err)))
            sys.exit(999)

        if logger_name.count('.') == 0:

            # a root logger

            """
            try:
                smtp_handler = JSMTPHandler(self.conf.logger_smtp_mailhost, self.conf.logger_smtp_fromaddr,
                                                 self.conf.logger_smtp_toaddr, self._logger_smtp_subject)
                smtp_handler.setFormatter(self._loggingVerboseFormatter)
                smtp_handler.setLevel(logging.CRITICAL)
                logger.addHandler(smtp_handler)
            except Exception as err:
                logging.critical('JLogging.getLogger(): error creating smtp_handler: ' + str(err))
                sys.exit(999)

            """

            try:
                self._conf.logger_file_filedir = os.path.join(self._conf.logger_file_filedir, logger_name)
                # Create the new daemon log directory
                dirFunc.createdirectory(self._conf.logger_file_filedir)
                self._logger_file = os.path.join(self._conf.logger_file_filedir, logger_name + '.log')
                print(f"log file is {self._logger_file}\n")
                file_handler = logging.handlers.TimedRotatingFileHandler(self._logger_file,
                                                                         when=self._conf.logger_file_when,
                                                                         interval=self._conf.logger_file_interval,
                                                                         backupCount=0, encoding=None, delay=False,
                                                                         utc=True)
                file_handler.setFormatter(self._loggingFormatter)
                file_handler.setLevel(self._loggingLevel)
                logger.addHandler(file_handler)

            except Exception as err:
                logging.critical('DonkeyLogging.getLogger(): error creating file_handler: ' + str(err))
                sys.exit(999)

            try:
                self._logger_error_file = os.path.join(self._conf.logger_file_filedir, logger_name + '.err')
                error_file_handler = logging.handlers.TimedRotatingFileHandler(self._logger_error_file,
                                                                               when=self._conf.logger_file_when,
                                                                               interval=self._conf.logger_file_interval,
                                                                               backupCount=0, encoding=None,
                                                                               delay=False, utc=True)
                error_file_handler.setFormatter(self._loggingVerboseFormatter)
                error_file_handler.setLevel(logging.ERROR)
                logger.addHandler(error_file_handler)
                self._logger = logger

            except Exception as err:
                logging.critical('DonkeyLogging.getLogger(): error creating error_file_handler: ' + str(err))
                sys.exit(999)

        return logger

    def remove(self, logger):
        print(len(logger.handlers))

